#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const readline = require('readline');
const JsonDB = require('../lib/index.js');
const pkg = require('../package.json');

/* ------------------------------ styling ------------------------------ */

const useColor = !process.env.NO_COLOR && (process.stdout.isTTY || process.env.FORCE_COLOR);
const paint = (code) => (text) => (useColor ? `\x1b[${code}m${text}\x1b[0m` : text);
const c = {
  cyan: paint('36'),
  brightCyan: paint('1;96'),
  green: paint('32'),
  yellow: paint('33'),
  red: paint('31'),
  dim: paint('2'),
  italic: paint('3'),
  bold: paint('1'),
};

const LOGO = [
  String.raw`     _                 ____  ____  `,
  String.raw`    | |___  ___  _ __ |  _ \| __ ) `,
  String.raw` _  | / __|/ _ \| '_ \| | | |  _ \ `,
  String.raw`| |_| \__ \ (_) | | | | |_| | |_) |`,
  String.raw` \___/|___/\___/|_| |_|____/|____/ `,
];

const out = (line = '') => process.stdout.write(`${line}\n`);
const fail = (message) => {
  process.stderr.write(`${c.red('Error:')} ${message}\n`);
  process.exit(1);
};

/* -------------------------------- help ------------------------------- */

function rows(items) {
  const width = Math.max(...items.map(([label]) => label.length));
  return items.map(([label, text]) => `  ${c.green(label.padEnd(width))}  ${text}`);
}

function showHelp() {
  out();
  LOGO.forEach((line, i) => out(i % 2 ? c.cyan(line) : c.brightCyan(line)));
  out();
  out(`${c.green('JsonDB')} ${c.yellow(pkg.version)}`);
  out(c.italic('A tiny, zero-dependency JSON database for Node.js.'));
  out();
  out(c.yellow('Usage:'));
  out('  jsondb [options] [command]');
  out();
  out(c.yellow('Options:'));
  rows([
    ['-h, --help', 'Display help for the command'],
    ['-V, --version', 'Display the application version'],
    ['    --migrate', 'Validate collections and apply schema headers'],
    ['-y, --yes', 'Skip confirmation prompts'],
  ]).forEach((l) => out(l));
  out();
  out(c.yellow('Commands:'));
  rows([
    ['init', 'Create a jsondb.config.json in the current directory'],
    ['status', 'Show file sizes, record counts and integrity status'],
  ]).forEach((l) => out(l));
  out();
}

/* ------------------------------ helpers ------------------------------ */

function formatSize(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}

function resolveDir() {
  let config;
  try {
    config = { ...JsonDB.DEFAULT_CONFIG, ...JsonDB.loadConfig(process.cwd()) };
  } catch (err) {
    fail(err.message);
  }
  const dir = path.resolve(config.dir);
  if (!fs.existsSync(dir)) fail(`Storage directory not found: ${dir}\nRun "jsondb init" first.`);
  return dir;
}

function confirm(question) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    rl.question(`${question} ${c.dim('[y/N]')} `, (answer) => {
      rl.close();
      resolve(/^y(es)?$/i.test(answer.trim()));
    });
  });
}

const STATUS_LABEL = {
  ok: () => c.green('OK'),
  legacy: () => c.yellow('NEEDS MIGRATION'),
  invalid: () => c.red('INVALID'),
};

/* ----------------------------- commands ------------------------------ */

function init() {
  const file = path.join(process.cwd(), JsonDB.CONFIG_FILE);
  if (fs.existsSync(file)) fail(`${JsonDB.CONFIG_FILE} already exists.`);

  fs.writeFileSync(file, JSON.stringify(JsonDB.DEFAULT_CONFIG, null, 2) + '\n');
  fs.mkdirSync(path.resolve(JsonDB.DEFAULT_CONFIG.dir), { recursive: true });
  out(`${c.green('Created')} ${JsonDB.CONFIG_FILE}`);
  out(`${c.green('Created')} ${JsonDB.DEFAULT_CONFIG.dir}/`);
}

function status() {
  const dir = resolveDir();
  const reports = JsonDB.inspect(dir);

  out();
  out(`${c.yellow('Storage:')} ${dir}`);
  out();

  if (!reports.length) {
    out('  No collections found.');
    out();
    return;
  }

  const nameWidth = Math.max(10, ...reports.map((r) => r.name.length));
  out(c.dim(`  ${'COLLECTION'.padEnd(nameWidth)}  ${'SIZE'.padStart(10)}  ${'RECORDS'.padStart(8)}  INTEGRITY`));
  for (const r of reports) {
    out(`  ${c.cyan(r.name.padEnd(nameWidth))}  ${formatSize(r.size).padStart(10)}  ${String(r.records).padStart(8)}  ${STATUS_LABEL[r.status]()}`);
    r.problems.slice(0, 3).forEach((p) => out(c.dim(`    - ${p}`)));
  }

  const total = reports.reduce((sum, r) => sum + r.size, 0);
  out();
  out(c.dim(`  ${reports.length} collection(s), ${formatSize(total)} total`));
  out();

  if (reports.some((r) => r.status === 'invalid')) process.exitCode = 1;
}

async function migrate(skipPrompt) {
  const dir = resolveDir();
  const pending = JsonDB.inspect(dir).filter((r) => r.status === 'legacy');

  if (!pending.length) {
    out(c.green('Nothing to migrate.'));
    JsonDB.inspect(dir)
      .filter((r) => r.status === 'invalid')
      .forEach((r) => out(`${c.red('Invalid:')} ${r.name} - ${r.problems[0]}`));
    return;
  }

  out(c.yellow('Collections to migrate:'));
  pending.forEach((r) => out(`  ${c.cyan(r.name)} (${r.records} records)`));
  out(c.dim('  Originals are kept as <name>.json.bak'));

  if (!skipPrompt) {
    if (!process.stdin.isTTY) fail('Not an interactive terminal. Re-run with --yes.');
    if (!(await confirm('Apply migration?'))) {
      out('Aborted.');
      return;
    }
  }

  const results = await JsonDB.migrate(dir);
  for (const r of results) {
    const tag = r.action === 'migrated' ? c.green('migrated') : r.action === 'skipped' ? c.red('skipped ') : c.dim('unchanged');
    out(`  ${tag}  ${r.name}${r.detail ? c.dim(`  ${r.detail}`) : ''}`);
  }
  if (results.some((r) => r.action === 'skipped')) process.exitCode = 1;
}

/* ------------------------------- main -------------------------------- */

async function main() {
  const args = process.argv.slice(2);
  const has = (...names) => names.some((n) => args.includes(n));
  const command = args.find((a) => !a.startsWith('-'));

  if (has('-h', '--help') || (!args.length)) return showHelp();
  if (has('-V', '--version')) return out(pkg.version);
  if (has('--migrate')) return migrate(has('-y', '--yes'));

  switch (command) {
    case 'init':
      return init();
    case 'status':
      return status();
    default: {
      process.stderr.write(`${c.red('Unknown input:')} ${args.join(' ')}\n`);
      showHelp();
      process.exitCode = 1;
    }
  }
}

main().catch((err) => fail(err.message));
