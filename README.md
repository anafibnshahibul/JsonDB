# JsonDB Lite

JsonDB Lite is a small, zero-dependency document database for Node.js. It stores each collection as a JSON file and provides atomic file replacement, cross-process write locks, query operators, indexes, aggregation, and journaled multi-collection transactions.

JsonDB is not a drop-in MongoDB replacement: it is intended for embedded applications and small datasets, not distributed deployments or high-concurrency workloads. Indexes are in-memory and rebuilt when needed. A transaction journal supports recovery after interrupted commits, but this file-based design does not provide MongoDB's full isolation or distributed transaction guarantees.

## Requirements

- Node.js 18 or newer

## Install from npm

Install JsonDB Lite as a dependency in your Node.js project:

```sh
npm install jsondb-lite-database
```

Use the database directly from JavaScript—**the CLI is optional**. You do not need to run an initialization command or install the package globally. `connect()` creates the configured storage folder automatically:

```js
const JsonDB = require('jsondb-lite-database');

async function main() {
  const db = await new JsonDB().connect({ dir: './data' });
  const users = db.collection('users');

  const created = await users.insert({ name: 'Ada', active: true });
  console.log(await users.find({ active: true }));

  await users.update({ _id: created._id }, { active: false });
  console.log(await users.count());
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
```

The storage path is relative to the process working directory unless an absolute path is supplied. Configuration can also be loaded from `jsondb.config.json` in the current working directory. Connect options override configuration-file values.

### Optional CLI

The package also provides a `jsondb` CLI for initialization, status checks, and legacy migrations. To use it globally, install globally; otherwise run it through `npx` or your local npm binary:

```sh
npm install --global jsondb-lite-database
jsondb --help
jsondb init
jsondb status
```

The CLI is not required for using the JavaScript API. `init` creates a configuration file and default storage folder. `status` reports collection sizes, record counts, and integrity. `jsondb --migrate` migrates legacy collection files after confirmation; `jsondb --migrate --yes` skips the prompt. Migration backs up changed files as `<name>.json.bak`.

## Configuration

`jsondb init` creates this configuration by default:

```json
{
  "dir": "./data",
  "pretty": true,
  "lock": {
    "timeoutMs": 5000,
    "staleMs": 10000,
    "retryMs": 15
  }
}
```

`dir` is the collection storage directory, relative to the current working directory (or an absolute path). `pretty` controls JSON formatting for writes. Lock settings are positive integer milliseconds: `timeoutMs` is how long a writer waits, `staleMs` determines when an abandoned lock can be reclaimed, and `retryMs` sets the retry interval. Unknown lock options and invalid values are rejected. The CLI and library read `jsondb.config.json` from the current working directory. Connect options override file settings.

Use `connect()` to create the storage directory if needed. You can also override configuration in code:

```js
await db.connect({ dir: './store', pretty: false });
```

Available collection methods are `insert(objectOrArray)`, `find(query, options)`, `findOne(query)`, `count(query)`, `update(query, changes, { multi })`, `delete(query, { multi })`, `createIndex(field)`, `dropIndex(field)`, and `aggregate(pipeline)`. Updates shallow-merge the supplied changes; `multi` defaults to `true`. Find options support multi-field `sort`, `skip`, and `limit`.

Queries support exact field matches, the operators `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, and `$nin`, and logical operators `$and`, `$or`, and `$nor`. Dotted field names can address nested fields, such as `{ 'profile.city': 'London' }`. Indexes are in-memory equality indexes; they accelerate matching exact indexed fields and are not persisted to disk.

Aggregation pipelines support `$match`, `$sort`, `$skip`, `$limit`, `$project`, `$unwind`, and `$group`. Group accumulators include `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, and `$addToSet`. Expressions in project/group stages can refer to fields with a `$fieldName` string.

### Transactions

Use `db.transaction(collectionNames, callback)` to stage changes across a declared set of collections. The callback receives a transaction object with `collection(name)` methods for `insert`, `find`, `findOne`, `count`, `update`, and `delete`. Throwing from the callback discards the staged changes. A commit journal allows the next connection to finish applying a commit if the process is interrupted. Keep transaction collection names explicit; accessing an undeclared collection throws.

```js
await db.transaction(['accounts', 'ledger'], async (tx) => {
  const accounts = tx.collection('accounts');
  const ledger = tx.collection('ledger');
  const account = await accounts.findOne({ _id: accountId });
  if (!account || account.balance < 10) throw new Error('Insufficient funds');
  await accounts.update({ _id: accountId }, { balance: account.balance - 10 });
  await ledger.insert({ accountId, amount: -10 });
});
```

Transactions stage the callback's operations and serialize commits, but ordinary reads do not participate in transaction isolation. For multi-process or high-throughput production databases, use a database engine designed for those guarantees.

Collection names may contain letters, numbers, underscores, and hyphens. JsonDB adds `_id`, `createdAt`, and `updatedAt` fields to inserted records.

## Tests

Run the built-in Node.js test suite with `npm test`.

## License

MIT
