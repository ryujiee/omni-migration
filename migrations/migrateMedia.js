require('dotenv').config();
const chalk = require('chalk');
const { NodeSSH } = require('node-ssh');

const ssh = new NodeSSH();

module.exports = async function migrateMedia() {
  console.log(chalk.blue('🔐 Verificando conexão com o servidor de destino...'));

  try {
    await ssh.connect({
      host: process.env.DST_SSH_HOST,
      username: process.env.DST_SSH_USER,
      password: process.env.DST_SSH_PASSWORD,
      port: process.env.DST_SSH_PORT || 22,
    });

    console.log(chalk.green('✅ Conexão com servidor de destino verificada com sucesso.'));
  } catch (err) {
    console.warn(chalk.yellow('⚠️ Não foi possível verificar conexão com o destino. Continuando...'));
  }

  ssh.dispose();

  console.log('\n📦 Esta etapa precisa ser executada manualmente. Siga as instruções abaixo:\n');

  console.log(chalk.yellow('1️⃣  Acesse o servidor de destino:'));
  console.log(chalk.cyan(`ssh ${process.env.DST_SSH_USER}@${process.env.DST_SSH_HOST}`));

  console.log(chalk.yellow('\n2️⃣  Execute o seguinte comando no servidor de destino:'));
  console.log(
    chalk.greenBright(
      `scp -o StrictHostKeyChecking=no -r ${process.env.SRC_SSH_USER}@${process.env.SRC_SSH_HOST}:${'/www/wwwroot/omniathostec/backend/public/'}* ${'/New-Omni/backend/public/media/'}`
    )
  );

  console.log(chalk.yellow('\n💡 Será solicitada a senha do servidor de origem (usuário "athostec").'));
  console.log(chalk.yellow('💾 Após a transferência, os arquivos estarão disponíveis no novo servidor.\n'));

  return true;
};
