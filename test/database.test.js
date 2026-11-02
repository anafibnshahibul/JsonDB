'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const JsonDB = require('../lib');

async function createDatabase(t) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'jsondb-test-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  return new JsonDB().connect({ dir, lock: { timeoutMs: 1000, staleMs: 2000, retryMs: 5 } });
}

test('supports logical queries and indexed lookups after mutations', async (t) => {
  const db = await createDatabase(t);
  const users = db.collection('users');
  await users.insert([{ name: 'Ada', age: 36 }, { name: 'Lin', age: 24 }]);
  assert.deepEqual(await users.find({ $or: [{ age: { $gte: 35 } }, { name: 'Lin' }] }, { sort: { age: 1 } }).then((docs) => docs.map((doc) => doc.name)), ['Lin', 'Ada']);

  assert.deepEqual(await users.createIndex('name'), { field: 'name', entries: 2 });
  assert.equal((await users.find({ name: 'Ada' })).length, 1);
  await users.update({ name: 'Ada' }, { name: 'A. Lovelace' });
  assert.equal((await users.find({ name: 'A. Lovelace' })).length, 1);
  assert.equal((await users.find({ name: 'Ada' })).length, 0);
  assert.equal(users.dropIndex('name'), true);
});

test('runs aggregation stages with grouping and projection', async (t) => {
  const db = await createDatabase(t);
  const sales = db.collection('sales');
  await sales.insert([
    { category: 'book', amount: 10 },
    { category: 'book', amount: 20 },
    { category: 'game', amount: 50 },
  ]);
  const result = await sales.aggregate([
    { $group: { _id: '$category', total: { $sum: '$amount' }, average: { $avg: '$amount' }, count: { $sum: 1 } } },
    { $sort: { total: -1 } },
    { $project: { _id: 1, total: 1, average: 1, count: 1 } },
  ]);
  assert.deepEqual(result, [
    { _id: 'game', total: 50, average: 50, count: 1 },
    { _id: 'book', total: 30, average: 15, count: 2 },
  ]);
});

test('commits declared collection changes and rolls back callback errors', async (t) => {
  const db = await createDatabase(t);
  const accounts = db.collection('accounts');
  const ledger = db.collection('ledger');

  await assert.rejects(db.transaction(['accounts', 'ledger'], async (tx) => {
    await tx.collection('accounts').insert({ owner: 'Ada', balance: 100 });
    throw new Error('abort');
  }), /abort/);
  assert.equal(await accounts.count(), 0);

  await db.transaction(['accounts', 'ledger'], async (tx) => {
    const account = await tx.collection('accounts').insert({ owner: 'Ada', balance: 100 });
    await tx.collection('accounts').update({ _id: account._id }, { balance: 75 });
    await tx.collection('ledger').insert({ owner: account._id, delta: -25 });
  });
  assert.equal(await accounts.findOne({ balance: 75 }).then(Boolean), true);
  assert.equal(await ledger.count(), 1);
});

test('validates advanced lock configuration', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'jsondb-config-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  await assert.rejects(new JsonDB().connect({ dir, lock: { timeoutMs: 0 } }), /positive integer/);
  assert.throws(() => JsonDB.validateConfig({ dir, pretty: 'yes', lock: JsonDB.DEFAULT_CONFIG.lock }), /must be a boolean/);
});

test('recovers a journaled commit when connecting', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'jsondb-recovery-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const now = new Date().toISOString();
  await fs.writeFile(path.join(dir, '.jsondb-transaction.json'), JSON.stringify({
    version: 1,
    collections: [{
      name: 'users',
      meta: { createdAt: now },
      records: [{ _id: 'recovered', name: 'Ada', createdAt: now, updatedAt: now }],
    }],
  }));

  const db = await new JsonDB().connect({ dir });
  assert.equal(await db.collection('users').findOne({ _id: 'recovered' }).then((doc) => doc.name), 'Ada');
  await assert.rejects(fs.access(path.join(dir, '.jsondb-transaction.json')));
});
