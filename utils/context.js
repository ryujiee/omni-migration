// utils/context.js
'use strict';

require('dotenv').config();

function buildContext() {
  const tenantId = (process.env.TENANT_ID || '').toString().trim() || null;

  return {
    tenantId,
    isSingleTenant: !!tenantId,

    // Dados DB origem/destino (use onde precisar)
    srcDb: {
      host: process.env.SRC_HOST,
      port: Number(process.env.SRC_PORT || 5432),
      user: process.env.SRC_USER,
      password: process.env.SRC_PASS,
      database: process.env.SRC_DB
    },
    dstDb: {
      host: process.env.DST_HOST,
      port: Number(process.env.DST_PORT || 5432),
      user: process.env.DST_USER,
      password: process.env.DST_PASS,
      database: process.env.DST_DB
    },

    // Dados SSH (caso suas migrations usem cópias remotas/rsync/scp)
    srcSSH: {
      host: process.env.SRC_SSH_HOST,
      user: process.env.SRC_SSH_USER,
      port: Number(process.env.SRC_SSH_PORT || 22),
      password: process.env.SRC_SSH_PASSWORD
    },
    dstSSH: {
      host: process.env.DST_SSH_HOST,
      user: process.env.DST_SSH_USER,
      port: Number(process.env.DST_SSH_PORT || 22),
      password: process.env.DST_SSH_PASSWORD
    }
  };
}

module.exports = { buildContext };
