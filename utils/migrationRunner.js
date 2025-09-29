// utils/migrationRunner.js
'use strict';

const oraPkg = require('ora');
const ora = oraPkg.default || oraPkg; // compat ESM/CJS
const chalk = require('chalk');
const fs = require('fs-extra');
const path = require('path');

module.exports = async function runStep(name, fn, ctx = {}) {
  const scopeKey = ctx.isSingleTenant ? `tenant-${ctx.tenantId}` : 'all-tenants';
  const logDir = path.resolve(__dirname, '../logs', scopeKey);
  fs.ensureDirSync(logDir);

  const startAt = new Date();
  const timestamp = startAt.toISOString().replace(/[:.]/g, '-');
  const logFile = path.join(logDir, `migration-${name}.log`); // mantém append por etapa
  const logStream = fs.createWriteStream(logFile, { flags: 'a' });

  const spinner = ora({
    text: `Executando "${name}" (${scopeKey})...`,
    discardStdin: false
  }).start();

  // Header do log
  logStream.write(`\n===== [${startAt.toISOString()}] STEP: ${name} | SCOPE: ${scopeKey} =====\n`);

  // Redireciona console para o arquivo (sem perder saída no terminal)
  const originalConsoleLog = console.log;
  const originalConsoleError = console.error;

  console.log = (...args) => {
    const line = args.map(String).join(' ');
    originalConsoleLog(line);
    logStream.write(`[LOG ${new Date().toISOString()}] ${line}\n`);
  };

  console.error = (...args) => {
    const line = args.map(String).join(' ');
    originalConsoleError(line);
    logStream.write(`[ERRO ${new Date().toISOString()}] ${line}\n`);
  };

  try {
    await fn(ctx); // <<<<<<<<<< repassa o contexto para a migration
    const ms = Date.now() - startAt.getTime();
    spinner.succeed(`✅ ${name} executado com sucesso (${(ms / 1000).toFixed(2)}s).`);
    logStream.write(`----- SUCESSO em ${ms} ms -----\n`);
    return true;
  } catch (err) {
    const ms = Date.now() - startAt.getTime();
    spinner.fail(`❌ Falha em ${name} (${(ms / 1000).toFixed(2)}s): ${err.message}`);
    logStream.write(`[${new Date().toISOString()}] STACK:\n${err.stack || err}\n`);
    return false;
  } finally {
    // Restaura console
    console.log = originalConsoleLog;
    console.error = originalConsoleError;
    logStream.end();
  }
};
