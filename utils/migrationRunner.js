const ora = require('ora').default;
const chalk = require('chalk');
const fs = require('fs-extra');
const path = require('path');

const logDir = path.resolve(__dirname, '../logs');
fs.ensureDirSync(logDir);

module.exports = async function runStep(name, fn) {
  const spinner = ora(`Executando "${name}"...`).start();
  const logFile = path.join(logDir, `migration-${name}.log`);
  const logStream = fs.createWriteStream(logFile, { flags: 'a' });

  // Redirecionar console para o arquivo
  const originalConsoleLog = console.log;
  const originalConsoleError = console.error;
  console.log = (...args) => {
    originalConsoleLog(...args);
    logStream.write(args.join(' ') + '\n');
  };
  console.error = (...args) => {
    originalConsoleError(...args);
    logStream.write('[ERRO] ' + args.join(' ') + '\n');
  };

  try {
    await fn();
    spinner.succeed(`✅ ${name} executado com sucesso.`);
    return true;
  } catch (err) {
    spinner.fail(`❌ Falha em ${name}: ${err.message}`);
    logStream.write(`[${new Date().toISOString()}] ${err.stack}\n`);
    return false;
  } finally {
    // Restaurar console padrão
    console.log = originalConsoleLog;
    console.error = originalConsoleError;
    logStream.end();
  }
};
