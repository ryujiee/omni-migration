// main.js
'use strict';

require('dotenv').config();
const inquirer = require('inquirer');
const chalk = require('chalk');
const fs = require('fs-extra');
const path = require('path');
const runStep = require('./utils/migrationRunner');
const { buildContext } = require('./utils/context');

const steps = [
  { name: 'Tenants', func: require('./migrations/migrateTenants') },
  { name: 'Departments', func: require('./migrations/migrateDepartments') },
  { name: 'Users', func: require('./migrations/migrateUsers') },
  { name: 'Permissions', func: require('./migrations/migratePermissions') },
  { name: 'TaskTypes', func: require('./migrations/migrateTaskTypes') },
  { name: 'Tasks', func: require('./migrations/migrateTasks') },
  { name: 'Tags', func: require('./migrations/migrateTags') },
  { name: 'QuickMessages', func: require('./migrations/migrateQuickMessages') },
  { name: 'Flows', func: require('./migrations/migrateFlows') },
  { name: 'Channels', func: require('./migrations/migrateChannels') },
  { name: 'Campaigns', func: require('./migrations/migrateCampaigns') },
  { name: 'Contacts', func: require('./migrations/migrateContacts') },
  { name: 'CampaignContacts', func: require('./migrations/migrateCampaignContacts') },
  { name: 'Settings', func: require('./migrations/migrateSettings') },
  { name: 'Tickets', func: require('./migrations/migrateTickets') },
  { name: 'Messages', func: require('./migrations/migrateMessages') },
  { name: 'InternalMessages', func: require('./migrations/migrateInternalMessages') },
  { name: 'MediaFiles', func: require('./migrations/migrateMedia') }
];

async function run() {
  const ctx = buildContext();
  const scopeKey = ctx.isSingleTenant ? `tenant-${ctx.tenantId}` : 'all-tenants';
  const progressPath = path.resolve(__dirname, `progress-${scopeKey}.json`);

  console.log(
    chalk.cyan.bold(
      ctx.isSingleTenant
        ? `🏷️  Modo: SINGLE TENANT (TENANT_ID=${ctx.tenantId})`
        : '🌍 Modo: FULL (todos os tenants)'
    )
  );

  let lastCompletedIndex = -1;

  if (fs.existsSync(progressPath)) {
    try {
      const progress = await fs.readJson(progressPath);
      lastCompletedIndex = steps.findIndex(s => s.name === progress.lastCompleted);
    } catch (e) {
      console.log(chalk.yellow('⚠️  Não foi possível ler o progress anterior. Reiniciando do início.'));
    }
  }

  for (let i = lastCompletedIndex + 1; i < steps.length; i++) {
    const step = steps[i];
    const confirm = await inquirer.prompt([
      {
        name: 'start',
        type: 'confirm',
        message: `🚀 Deseja executar a etapa "${step.name}" agora?`,
        default: true
      }
    ]);

    if (!confirm.start) {
      console.log(chalk.yellow(`🔸 Etapa "${step.name}" pulada.`));
      continue;
    }

    const success = await runStep(step.name, step.func, ctx);
    if (!success) {
      console.log(chalk.red(`❌ Parando execução. Corrija o erro e reexecute.`));
      process.exit(1);
    }

    await fs.writeJson(progressPath, { lastCompleted: step.name });
  }

  console.log(chalk.green.bold('✅ MIGRAÇÃO COMPLETA COM SUCESSO!'));
  await fs.remove(progressPath);
}

run();
