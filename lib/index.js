'use strict';

const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const crypto = require('crypto');
const { EventEmitter } = require('events');

const SCHEMA = 'jsondb/1';
const CONFIG_FILE = 'jsondb.config.json';
const DEFAULT_CONFIG = { dir: './data', pretty: true };
const NAME_RE = /^[A-Za-z0-9_-]+$/;

const LOCK_STALE_MS = 10000;
const LOCK_TIMEOUT_MS = 5000;
const LOCK_RETRY_MS = 15;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const isPlain = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const clone = (v) => JSON.parse(JSON.stringify(v));

/* ------------------------------------------------------------------ */
/* File safety: atomic writes and cross-process locking               */
/* ------------------------------------------------------------------ */

// Write to a temp file in the same directory, flush to disk, then rename.
// rename() is atomic on POSIX, so readers never observe a partial file.
async function atomicWrite(file, text) {
  const tmp = `${file}.${process.pid}.${crypto.randomBytes(4).toString('hex')}.tmp`;
  const handle = await fsp.open(tmp, 'w');
  try {
    await handle.writeFile(text);
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await fsp.rename(tmp, file);
  } catch (err) {
    await fsp.unlink(tmp).catch(() => {});
    throw err;
  }
}

// Exclusive lock file ("wx" fails if it exists). Stale locks are reclaimed.
async function withLock(file, task) {
  const lock = `${file}.lock`;
  const started = Date.now();

  for (;;) {
    try {
      const handle = await fsp.open(lock, 'wx');
      await handle.close();
      break;
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      const stat = await fsp.stat(lock).catch(() => null);
      if (stat && Date.now() - stat.mtimeMs > LOCK_STALE_MS) {
        await fsp.unlink(lock).catch(() => {});
        continue;
      }
      if (Date.now() - started > LOCK_TIMEOUT_MS) {
        throw new Error(`Timed out waiting for lock: ${path.basename(lock)}`);
      }
      await sleep(LOCK_RETRY_MS);
    }
  }

  try {
    return await task();
  } finally {
    await fsp.unlink(lock).catch(() => {});
  }
}

/* ------------------------------------------------------------------ */
/* Document format                                                    */
/* ------------------------------------------------------------------ */

function newId() {
  return crypto.randomUUID();
}

function serialize(name, meta, records, pretty) {
  const doc = {
    $schema: SCHEMA,
    name,
    createdAt: meta.createdAt,
    updatedAt: new Date().toISOString(),
    records,
  };
  return JSON.stringify(doc, null, pretty ? 2 : 0) + '\n';
}

// Structural check shared by the collection reader, `status` and `--migrate`.
function analyze(raw) {
  const problems = [];
  let records;
  let hasHeader = false;

  if (Array.isArray(raw)) {
    records = raw;
  } else if (isPlain(raw) && Array.isArray(raw.records)) {
    records = raw.records;
    hasHeader = raw.$schema === SCHEMA;
  } else {
    return {
      records: null,
      hasHeader,
      missingIds: 0,
      problems: ['Root must be an array or an object with a "records" array'],
    };
  }

  const seen = new Set();
  let missingIds = 0;
  records.forEach((record, index) => {
    if (!isPlain(record)) {
      problems.push(`Record #${index} is not an object`);
      return;
    }
    if (record._id === undefined) {
      missingIds++;
      return;
    }
    if (seen.has(record._id)) problems.push(`Duplicate _id "${record._id}"`);
    seen.add(record._id);
  });

  return { records, hasHeader, missingIds, problems };
}

/* ------------------------------------------------------------------ */
/* Query helpers                                                      */
/* ------------------------------------------------------------------ */

function getPath(obj, dotted) {
  return dotted.split('.').reduce((acc, key) => (acc == null ? undefined : acc[key]), obj);
}

const OPERATORS = {
  $eq: (a, b) => a === b,
  $ne: (a, b) => a !== b,
  $gt: (a, b) => a > b,
  $gte: (a, b) => a >= b,
  $lt: (a, b) => a < b,
  $lte: (a, b) => a <= b,
  $in: (a, b) => Array.isArray(b) && b.includes(a),
  $nin: (a, b) => Array.isArray(b) && !b.includes(a),
};

function matches(doc, query) {
  if (typeof query === 'function') return Boolean(query(doc));
  if (!query) return true;

  return Object.entries(query).every(([key, cond]) => {
    const value = getPath(doc, key);
    const isOps = isPlain(cond) && Object.keys(cond).length > 0 && Object.keys(cond).every((k) => k.startsWith('$'));

    if (isOps) {
      return Object.entries(cond).every(([op, arg]) => {
        if (!OPERATORS[op]) throw new Error(`Unknown operator: ${op}`);
        return OPERATORS[op](value, arg);
      });
    }
    if (isPlain(cond) || Array.isArray(cond)) return JSON.stringify(value) === JSON.stringify(cond);
    return value === cond;
  });
}

/* ------------------------------------------------------------------ */
/* Collection                                                         */
/* ------------------------------------------------------------------ */

class Collection {
  constructor(db, name) {
    this.db = db;
    this.name = name;
    this.file = path.join(db.dir, `${name}.json`);
    this._queue = Promise.resolve();
  }

  async _read() {
    let text;
    try {
      text = await fsp.readFile(this.file, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') {
        return { meta: { createdAt: new Date().toISOString() }, records: [] };
      }
      throw err;
    }

    let raw;
    try {
      raw = JSON.parse(text);
    } catch (err) {
      throw new Error(`Collection "${this.name}" is not valid JSON: ${err.message}`);
    }

    const info = analyze(raw);
    if (!info.records) throw new Error(`Collection "${this.name}": ${info.problems[0]}`);

    const meta = isPlain(raw) && raw.createdAt ? { createdAt: raw.createdAt } : { createdAt: new Date().toISOString() };
    return { meta, records: info.records };
  }

  // Serialised read-modify-write: in-process queue first, then the file lock.
  // The task returns { changed, value }; the file is only rewritten if changed.
  _mutate(task) {
    const run = () =>
      withLock(this.file, async () => {
        const { meta, records } = await this._read();
        const result = await task(records);
        if (result.changed) {
          await atomicWrite(this.file, serialize(this.name, meta, records, this.db.pretty));
        }
        return result.value;
      });

    const next = this._queue.then(run, run);
    this._queue = next.catch(() => {});
    return next;
  }

  async insert(input) {
    const docs = Array.isArray(input) ? input : [input];
    if (!docs.every(isPlain)) throw new TypeError('insert() expects an object or an array of objects');

    const inserted = await this._mutate((records) => {
      const now = new Date().toISOString();
      const existing = new Set(records.map((r) => r._id));
      const created = docs.map((doc) => {
        const record = { ...clone(doc), _id: doc._id !== undefined ? doc._id : newId(), createdAt: now, updatedAt: now };
        if (existing.has(record._id)) throw new Error(`Duplicate _id "${record._id}"`);
        existing.add(record._id);
        return record;
      });
      records.push(...created);
      return { changed: true, value: created };
    });

    this.db.emit('change', { collection: this.name, op: 'insert', count: inserted.length });
    return Array.isArray(input) ? inserted : inserted[0];
  }

  // options: { sort: { field: 1 | -1 }, skip, limit }
  async find(query = {}, options = {}) {
    const { records } = await this._read();
    let result = records.filter((doc) => matches(doc, query));

    if (options.sort) {
      const [[field, dir]] = Object.entries(options.sort);
      result.sort((a, b) => {
        const x = getPath(a, field);
        const y = getPath(b, field);
        return (x > y ? 1 : x < y ? -1 : 0) * (dir < 0 ? -1 : 1);
      });
    }
    if (options.skip) result = result.slice(options.skip);
    if (options.limit != null) result = result.slice(0, options.limit);

    return clone(result);
  }

  async findOne(query = {}) {
    const [first] = await this.find(query, { limit: 1 });
    return first || null;
  }

  async count(query = {}) {
    const { records } = await this._read();
    return records.filter((doc) => matches(doc, query)).length;
  }

  // changes: object (shallow merge) or function(doc) returning an object.
  async update(query, changes, { multi = true } = {}) {
    const count = await this._mutate((records) => {
      let touched = 0;
      for (const doc of records) {
        if (!matches(doc, query)) continue;
        if (!multi && touched > 0) break;
        const patch = typeof changes === 'function' ? changes(clone(doc)) : changes;
        Object.assign(doc, clone(patch), { _id: doc._id, updatedAt: new Date().toISOString() });
        touched++;
      }
      return { changed: touched > 0, value: touched };
    });

    if (count) this.db.emit('change', { collection: this.name, op: 'update', count });
    return count;
  }

  async delete(query, { multi = true } = {}) {
    const count = await this._mutate((records) => {
      let removed = 0;
      for (let i = records.length - 1; i >= 0; i--) {
        if (!matches(records[i], query)) continue;
        records.splice(i, 1);
        removed++;
        if (!multi) break;
      }
      return { changed: removed > 0, value: removed };
    });

    if (count) this.db.emit('change', { collection: this.name, op: 'delete', count });
    return count;
  }
}

/* ------------------------------------------------------------------ */
/* JsonDB                                                             */
/* ------------------------------------------------------------------ */

class JsonDB extends EventEmitter {
  constructor() {
    super();
    this.dir = null;
    this.pretty = true;
    this._collections = new Map();
  }

  // Resolves options from arguments, then jsondb.config.json, then defaults.
  async connect(options = {}) {
    const config = { ...DEFAULT_CONFIG, ...JsonDB.loadConfig(process.cwd()), ...options };
    this.dir = path.resolve(config.dir);
    this.pretty = Boolean(config.pretty);
    await fsp.mkdir(this.dir, { recursive: true });
    this.emit('connect', this.dir);
    return this;
  }

  collection(name) {
    if (!this.dir) throw new Error('Call connect() before collection()');
    if (!NAME_RE.test(name)) throw new Error('Collection names may only contain letters, numbers, "_" and "-"');
    if (!this._collections.has(name)) this._collections.set(name, new Collection(this, name));
    return this._collections.get(name);
  }

  static loadConfig(cwd) {
    const file = path.join(cwd, CONFIG_FILE);
    if (!fs.existsSync(file)) return {};
    try {
      return JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch (err) {
      throw new Error(`Invalid ${CONFIG_FILE}: ${err.message}`);
    }
  }

  static listFiles(dir) {
    return fs
      .readdirSync(dir)
      .filter((f) => f.endsWith('.json') && f !== CONFIG_FILE)
      .sort();
  }

  // Read-only report for every collection file in `dir`.
  static inspect(dir) {
    return JsonDB.listFiles(dir).map((file) => {
      const full = path.join(dir, file);
      const report = { name: path.basename(file, '.json'), file: full, size: fs.statSync(full).size, records: 0, status: 'ok', problems: [] };

      let raw;
      try {
        raw = JSON.parse(fs.readFileSync(full, 'utf8'));
      } catch (err) {
        return { ...report, status: 'invalid', problems: [`Invalid JSON: ${err.message}`] };
      }

      const info = analyze(raw);
      report.records = info.records ? info.records.length : 0;
      report.problems = info.problems;
      if (info.problems.length) report.status = 'invalid';
      else if (!info.hasHeader || info.missingIds) report.status = 'legacy';
      return report;
    });
  }

  // Validates each file and applies the metadata header. Invalid files are
  // skipped; migrated files are backed up to "<name>.json.bak" first.
  static async migrate(dir) {
    const results = [];

    for (const report of JsonDB.inspect(dir)) {
      if (report.status === 'invalid') {
        results.push({ name: report.name, action: 'skipped', detail: report.problems[0] });
        continue;
      }
      if (report.status === 'ok') {
        results.push({ name: report.name, action: 'unchanged', detail: '' });
        continue;
      }

      await withLock(report.file, async () => {
        const raw = JSON.parse(await fsp.readFile(report.file, 'utf8'));
        const records = Array.isArray(raw) ? raw : raw.records;
        const now = new Date().toISOString();
        let assigned = 0;

        for (const record of records) {
          if (record._id === undefined) {
            record._id = newId();
            assigned++;
          }
        }

        await fsp.copyFile(report.file, `${report.file}.bak`);
        const meta = { createdAt: isPlain(raw) && raw.createdAt ? raw.createdAt : now };
        await atomicWrite(report.file, serialize(report.name, meta, records, true));
        results.push({ name: report.name, action: 'migrated', detail: `${assigned} id(s) assigned` });
      });
    }

    return results;
  }
}

JsonDB.SCHEMA = SCHEMA;
JsonDB.CONFIG_FILE = CONFIG_FILE;
JsonDB.DEFAULT_CONFIG = DEFAULT_CONFIG;

module.exports = JsonDB;
module.exports.JsonDB = JsonDB;
