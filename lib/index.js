'use strict';

const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const crypto = require('crypto');
const { EventEmitter } = require('events');

const SCHEMA = 'jsondb/1';
const CONFIG_FILE = 'jsondb.config.json';
const DEFAULT_CONFIG = {
  dir: './data',
  pretty: true,
  lock: { timeoutMs: 5000, staleMs: 10000, retryMs: 15 },
};
const NAME_RE = /^[A-Za-z0-9_-]+$/;

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
async function withLock(file, task, options = DEFAULT_CONFIG.lock) {
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
      if (stat && Date.now() - stat.mtimeMs > options.staleMs) {
        await fsp.unlink(lock).catch(() => {});
        continue;
      }
      if (Date.now() - started > options.timeoutMs) {
        throw new Error(`Timed out waiting for lock: ${path.basename(lock)}`);
      }
      await sleep(options.retryMs);
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
    if (key === '$and') return Array.isArray(cond) && cond.every((part) => matches(doc, part));
    if (key === '$or') return Array.isArray(cond) && cond.some((part) => matches(doc, part));
    if (key === '$nor') return Array.isArray(cond) && !cond.some((part) => matches(doc, part));

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

function valueOf(doc, expression) {
  if (typeof expression === 'string' && expression.startsWith('$')) return getPath(doc, expression.slice(1));
  return expression;
}

function compareBy(sort) {
  const fields = Object.entries(sort);
  return (a, b) => {
    for (const [field, direction] of fields) {
      const x = getPath(a, field);
      const y = getPath(b, field);
      const result = x > y ? 1 : x < y ? -1 : 0;
      if (result) return result * (direction < 0 ? -1 : 1);
    }
    return 0;
  };
}

function project(doc, specification) {
  const entries = Object.entries(specification);
  const include = entries.some(([, value]) => value === 1 || value === true || (typeof value === 'string' && value.startsWith('$')));
  if (!include) {
    const result = clone(doc);
    for (const [field, enabled] of entries) if (!enabled) {
      const parts = field.split('.');
      const leaf = parts.pop();
      const parent = parts.reduce((node, key) => (node == null ? undefined : node[key]), result);
      if (parent) delete parent[leaf];
    }
    return result;
  }

  const result = {};
  if (specification._id !== 0 && specification._id !== false && doc._id !== undefined) result._id = doc._id;
  for (const [field, enabled] of entries) {
    if (field === '_id' || enabled === 0 || enabled === false) continue;
    const value = typeof enabled === 'string' && enabled.startsWith('$') ? valueOf(doc, enabled) : getPath(doc, field);
    if (value !== undefined) result[field] = value;
  }
  return result;
}

function aggregate(records, pipeline) {
  if (!Array.isArray(pipeline)) throw new TypeError('aggregate() expects a pipeline array');
  let result = clone(records);
  for (const stage of pipeline) {
    if (!isPlain(stage) || Object.keys(stage).length !== 1) throw new TypeError('Each aggregation stage must be a one-key object');
    const [operator, specification] = Object.entries(stage)[0];
    switch (operator) {
      case '$match':
        result = result.filter((doc) => matches(doc, specification));
        break;
      case '$sort':
        result.sort(compareBy(specification));
        break;
      case '$skip':
        if (!Number.isInteger(specification) || specification < 0) throw new TypeError('$skip must be a non-negative integer');
        result = result.slice(specification);
        break;
      case '$limit':
        if (!Number.isInteger(specification) || specification < 0) throw new TypeError('$limit must be a non-negative integer');
        result = result.slice(0, specification);
        break;
      case '$project':
        if (!isPlain(specification)) throw new TypeError('$project expects an object');
        result = result.map((doc) => project(doc, specification));
        break;
      case '$unwind': {
        const field = typeof specification === 'string' ? specification.replace(/^\$/, '') : specification && specification.path && specification.path.replace(/^\$/, '');
        if (!field) throw new TypeError('$unwind expects a field path');
        const expanded = [];
        for (const doc of result) {
          const value = getPath(doc, field);
          if (Array.isArray(value)) {
            for (const item of value) {
              const copy = clone(doc);
              const parts = field.split('.');
              const leaf = parts.pop();
              const parent = parts.reduce((node, key) => node[key], copy);
              parent[leaf] = item;
              expanded.push(copy);
            }
          } else if (specification && specification.preserveNullAndEmptyArrays && value == null) expanded.push(doc);
        }
        result = expanded;
        break;
      }
      case '$group': {
        if (!isPlain(specification) || !Object.hasOwn(specification, '_id')) throw new TypeError('$group requires an _id expression');
        const groups = new Map();
        for (const doc of result) {
          const key = valueOf(doc, specification._id);
          const encoded = JSON.stringify(key);
          if (!groups.has(encoded)) {
            const group = { _id: key };
            for (const [field, accumulator] of Object.entries(specification)) {
              if (field !== '_id' && isPlain(accumulator)) {
                const op = Object.keys(accumulator)[0];
                if (op === '$push' || op === '$addToSet') group[field] = [];
                else if (op === '$sum' || op === '$avg') group[field] = 0;
                if (op === '$avg') group[`${field}__count`] = 0;
              }
            }
            groups.set(encoded, group);
          }
          const group = groups.get(encoded);
          for (const [field, accumulator] of Object.entries(specification)) {
            if (field === '_id') continue;
            if (!isPlain(accumulator) || Object.keys(accumulator).length !== 1) throw new TypeError(`Invalid accumulator for ${field}`);
            const [op, expression] = Object.entries(accumulator)[0];
            const value = valueOf(doc, expression);
            switch (op) {
              case '$sum': group[field] += Number(value) || 0; break;
              case '$avg': group[field] += Number(value) || 0; group[`${field}__count`]++; break;
              case '$min': if (group[field] === undefined || value < group[field]) group[field] = value; break;
              case '$max': if (group[field] === undefined || value > group[field]) group[field] = value; break;
              case '$first': if (!Object.hasOwn(group, field)) group[field] = value; break;
              case '$last': group[field] = value; break;
              case '$push': group[field].push(value); break;
              case '$addToSet': if (!group[field].some((item) => JSON.stringify(item) === JSON.stringify(value))) group[field].push(value); break;
              default: throw new Error(`Unknown aggregation accumulator: ${op}`);
            }
          }
        }
        result = [...groups.values()];
        for (const group of result) for (const field of Object.keys(specification)) {
          if (field.endsWith('__count')) continue;
          if (specification[field] && specification[field].$avg) {
            const count = group[`${field}__count`];
            group[field] = count ? group[field] / count : null;
            delete group[`${field}__count`];
          }
        }
        break;
      }
      default: throw new Error(`Unknown aggregation stage: ${operator}`);
    }
  }
  return result;
}

function withLocks(files, task, options) {
  const acquire = (index) => index === files.length
    ? task()
    : withLock(files[index], () => acquire(index + 1), options);
  return acquire(0);
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
    this._indexes = new Map();
    this._indexVersion = null;
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
          this._indexVersion = null;
        }
        return result.value;
      }, this.db.lock);

    const next = this._queue.then(run, run);
    this._queue = next.catch(() => {});
    return next;
  }

  async createIndex(field) {
    if (typeof field !== 'string' || !field || field.split('.').some((part) => !part)) {
      throw new TypeError('createIndex() expects a non-empty field path');
    }
    const { records } = await this._read();
    const index = new Map();
    for (const record of records) {
      const key = JSON.stringify(getPath(record, field));
      if (!index.has(key)) index.set(key, []);
      index.get(key).push(record);
    }
    this._indexes.set(field, index);
    const stat = await fsp.stat(this.file).catch(() => null);
    this._indexVersion = stat ? `${stat.mtimeMs}:${stat.size}` : 'missing';
    return { field, entries: index.size };
  }

  dropIndex(field) {
    return this._indexes.delete(field);
  }

  async _indexedRecords(records, query) {
    const stat = await fsp.stat(this.file).catch(() => null);
    const version = stat ? `${stat.mtimeMs}:${stat.size}` : 'missing';
    if (this._indexVersion !== version) {
      for (const field of this._indexes.keys()) {
        const index = new Map();
        for (const record of records) {
          const key = JSON.stringify(getPath(record, field));
          if (!index.has(key)) index.set(key, []);
          index.get(key).push(record);
        }
        this._indexes.set(field, index);
      }
      this._indexVersion = version;
    }
    for (const [field, index] of this._indexes) {
      if (!Object.hasOwn(query, field)) continue;
      const condition = query[field];
      const value = isPlain(condition) && Object.keys(condition).length === 1 && Object.hasOwn(condition, '$eq')
        ? condition.$eq
        : isPlain(condition) || Array.isArray(condition) ? undefined : condition;
      if (value !== undefined) return index.get(JSON.stringify(value)) || [];
    }
    return records;
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
    const candidates = isPlain(query) ? await this._indexedRecords(records, query) : records;
    let result = candidates.filter((doc) => matches(doc, query));

    if (options.sort) result.sort(compareBy(options.sort));
    if (options.skip) result = result.slice(options.skip);
    if (options.limit != null) result = result.slice(0, options.limit);

    return clone(result);
  }

  async aggregate(pipeline = []) {
    const { records } = await this._read();
    return aggregate(records, pipeline);
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
    this.lock = { ...DEFAULT_CONFIG.lock };
    this._collections = new Map();
  }

  // Resolves options from arguments, then jsondb.config.json, then defaults.
  async connect(options = {}) {
    if (!isPlain(options)) throw new TypeError('connect() options must be an object');
    const stored = JsonDB.loadConfig(process.cwd());
    const config = JsonDB.validateConfig({
      ...DEFAULT_CONFIG,
      ...stored,
      ...options,
      lock: { ...DEFAULT_CONFIG.lock, ...(stored.lock || {}), ...(options.lock || {}) },
    });
    this.dir = path.resolve(config.dir);
    this.pretty = config.pretty;
    this.lock = config.lock;
    await fsp.mkdir(this.dir, { recursive: true });
    await this._recoverTransaction();
    this.emit('connect', this.dir);
    return this;
  }

  collection(name) {
    if (!this.dir) throw new Error('Call connect() before collection()');
    if (!NAME_RE.test(name)) throw new Error('Collection names may only contain letters, numbers, "_" and "-"');
    if (!this._collections.has(name)) this._collections.set(name, new Collection(this, name));
    return this._collections.get(name);
  }

  async transaction(names, callback) {
    if (!Array.isArray(names) || !names.length || !names.every((name) => typeof name === 'string' && NAME_RE.test(name))) {
      throw new TypeError('transaction() expects a non-empty array of valid collection names');
    }
    if (typeof callback !== 'function') throw new TypeError('transaction() expects a callback');
    const uniqueNames = [...new Set(names)].sort();
    const collections = new Map(uniqueNames.map((name) => [name, this.collection(name)]));
    const files = uniqueNames.map((name) => collections.get(name).file);

    await this._recoverTransaction();
    const transactionLock = path.join(this.dir, '.jsondb-transaction');
    return withLock(transactionLock, () => withLocks(files, async () => {
      const state = new Map();
      for (const [name, collection] of collections) {
        const { meta, records } = await collection._read();
        state.set(name, { collection, meta, records: clone(records), changed: false, operations: [] });
      }

      const tx = {
        collection: (name) => {
          const entry = state.get(name);
          if (!entry) throw new Error(`Collection "${name}" was not declared in this transaction`);
          return {
            async insert(input) {
              const docs = Array.isArray(input) ? input : [input];
              if (!docs.every(isPlain)) throw new TypeError('insert() expects an object or an array of objects');
              const now = new Date().toISOString();
              const ids = new Set(entry.records.map((doc) => doc._id));
              const created = docs.map((doc) => {
                const record = { ...clone(doc), _id: doc._id !== undefined ? doc._id : newId(), createdAt: now, updatedAt: now };
                if (ids.has(record._id)) throw new Error(`Duplicate _id "${record._id}"`);
                ids.add(record._id);
                return record;
              });
              entry.records.push(...created);
              if (created.length) { entry.changed = true; entry.operations.push({ op: 'insert', count: created.length }); }
              return Array.isArray(input) ? created : created[0];
            },
            async find(query = {}, options = {}) {
              let found = entry.records.filter((doc) => matches(doc, query));
              if (options.sort) found.sort(compareBy(options.sort));
              if (options.skip) found = found.slice(options.skip);
              if (options.limit != null) found = found.slice(0, options.limit);
              return clone(found);
            },
            async findOne(query = {}) {
              return (await this.find(query, { limit: 1 }))[0] || null;
            },
            async count(query = {}) {
              return entry.records.filter((doc) => matches(doc, query)).length;
            },
            async update(query, changes, { multi = true } = {}) {
              let count = 0;
              for (const doc of entry.records) {
                if (!matches(doc, query)) continue;
                if (!multi && count) break;
                const patch = typeof changes === 'function' ? changes(clone(doc)) : changes;
                if (!isPlain(patch)) throw new TypeError('update() changes must be an object or a function returning an object');
                Object.assign(doc, clone(patch), { _id: doc._id, updatedAt: new Date().toISOString() });
                count++;
              }
              if (count) { entry.changed = true; entry.operations.push({ op: 'update', count }); }
              return count;
            },
            async delete(query, { multi = true } = {}) {
              let count = 0;
              for (let i = entry.records.length - 1; i >= 0; i--) {
                if (!matches(entry.records[i], query)) continue;
                entry.records.splice(i, 1);
                count++;
                if (!multi) break;
              }
              if (count) { entry.changed = true; entry.operations.push({ op: 'delete', count }); }
              return count;
            },
          };
        },
      };

      const result = await callback(tx);
      const changed = [...state.entries()].filter(([, entry]) => entry.changed);
      if (changed.length) {
        const journal = {
          version: 1,
          collections: changed.map(([name, entry]) => ({ name, meta: entry.meta, records: entry.records })),
        };
        await atomicWrite(path.join(this.dir, '.jsondb-transaction.json'), JSON.stringify(journal));
      }
      for (const entry of state.values()) {
        if (!entry.changed) continue;
        await atomicWrite(entry.collection.file, serialize(entry.collection.name, entry.meta, entry.records, this.pretty));
      }
      for (const [name, entry] of state) {
        if (!entry.changed) continue;
        entry.collection._indexVersion = null;
        this.emit('change', { collection: name, op: 'transaction', count: entry.operations.reduce((sum, op) => sum + op.count, 0) });
      }
      if (changed.length) await fsp.unlink(path.join(this.dir, '.jsondb-transaction.json')).catch(() => {});
      return result;
    }, this.lock), this.lock);
  }

  async _recoverTransaction() {
    const file = path.join(this.dir, '.jsondb-transaction.json');
    await withLock(path.join(this.dir, '.jsondb-transaction'), async () => {
      let journal;
      try {
        journal = JSON.parse(await fsp.readFile(file, 'utf8'));
      } catch (err) {
        if (err.code === 'ENOENT') return;
        throw new Error(`Cannot recover transaction journal: ${err.message}`);
      }
      if (!journal || journal.version !== 1 || !Array.isArray(journal.collections)) {
        throw new Error('Cannot recover transaction journal: invalid format');
      }
      for (const item of journal.collections) {
        if (!item || !NAME_RE.test(item.name) || !Array.isArray(item.records) || !isPlain(item.meta)) {
          throw new Error('Cannot recover transaction journal: invalid collection entry');
        }
        const collection = this.collection(item.name);
        await withLock(collection.file, async () => {
          await atomicWrite(collection.file, serialize(item.name, item.meta, item.records, this.pretty));
        }, this.lock);
      }
      await fsp.unlink(file);
    }, this.lock);
  }

  static loadConfig(cwd) {
    const file = path.join(cwd, CONFIG_FILE);
    if (!fs.existsSync(file)) return {};
    try {
      const config = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (!isPlain(config)) throw new Error('Configuration root must be a JSON object');
      return config;
    } catch (err) {
      throw new Error(`Invalid ${CONFIG_FILE}: ${err.message}`);
    }
  }

  static validateConfig(config) {
    if (!isPlain(config)) throw new TypeError('Configuration must be an object');
    if (typeof config.dir !== 'string' || !config.dir.trim()) throw new TypeError('Configuration "dir" must be a non-empty string');
    if (typeof config.pretty !== 'boolean') throw new TypeError('Configuration "pretty" must be a boolean');
    if (!isPlain(config.lock)) throw new TypeError('Configuration "lock" must be an object');
    for (const key of Object.keys(config.lock)) {
      if (!['timeoutMs', 'staleMs', 'retryMs'].includes(key)) throw new Error(`Unknown lock configuration option: ${key}`);
      if (!Number.isInteger(config.lock[key]) || config.lock[key] < 1) throw new TypeError(`Configuration "lock.${key}" must be a positive integer`);
    }
    return { ...config, lock: { ...config.lock } };
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
