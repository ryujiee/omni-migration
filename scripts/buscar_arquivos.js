// scripts/dump-media-names.js
'use strict';

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Client } = require('pg');

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '5000', 10);

function arg(name, def = null) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? hit.split('=').slice(1).join('=') : def;
}

const TENANT_ID = arg('tenant', process.env.TENANT_ID && String(process.env.TENANT_ID).trim());
const OUTPUT = arg('output', 'media-names.txt');
const BASENAME = process.argv.includes('--basename');

function getDest() {
  return new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB
  });
}

function extractBaseName(s) {
  if (!s) return s;
  try {
    // se for URL, pega o último segmento do pathname
    const u = new URL(s);
    const base = u.pathname.split('/').filter(Boolean).pop() || '';
    return decodeURIComponent(base);
  } catch {
    // não é URL: tenta como caminho (posix/win)
    const bySlash = s.split('/').pop();
    const byBack = bySlash.split('\\').pop();
    return byBack || s;
  }
}

async function main() {
  const dest = getDest();
  await dest.connect();

  console.log(`🗂️  Dump de media_name do destino → ${OUTPUT}`);
  if (TENANT_ID) console.log(`   • filtro por company_id (tenant): ${TENANT_ID}`);
  if (BASENAME)  console.log(`   • gravando apenas o nome do arquivo (--basename)`);

  // contagem total (só pra log)
  const countSqlTenant = `
    SELECT COUNT(*)::bigint AS tot
    FROM messages m
    JOIN tickets  t ON t.id = m.ticket_id
    WHERE t.company_id = $1 AND m.media_name IS NOT NULL AND m.media_name <> ''
  `;
  const countSqlAll = `
    SELECT COUNT(*)::bigint AS tot
    FROM messages m
    WHERE m.media_name IS NOT NULL AND m.media_name <> ''
  `;
  const countRes = TENANT_ID
    ? await dest.query(countSqlTenant, [TENANT_ID])
    : await dest.query(countSqlAll);
  const total = Number(countRes.rows[0].tot || 0);
  console.log(`   • total de mensagens com media_name: ${total}`);

  const ws = fs.createWriteStream(OUTPUT, { flags: 'w', encoding: 'utf8' });

  // varre em lotes por id para evitar OFFSET pesado
  let lastId = 0;
  let written = 0;

  const fetchSqlTenant = `
    SELECT m.id, m.media_name
    FROM messages m
    JOIN tickets  t ON t.id = m.ticket_id
    WHERE t.company_id = $1
      AND m.media_name IS NOT NULL AND m.media_name <> ''
      AND m.id > $2
    ORDER BY m.id ASC
    LIMIT $3
  `;
  const fetchSqlAll = `
    SELECT m.id, m.media_name
    FROM messages m
    WHERE m.media_name IS NOT NULL AND m.media_name <> ''
      AND m.id > $1
    ORDER BY m.id ASC
    LIMIT $2
  `;

  try {
    while (true) {
      const res = TENANT_ID
        ? await dest.query(fetchSqlTenant, [TENANT_ID, lastId, BATCH_SIZE])
        : await dest.query(fetchSqlAll, [lastId, BATCH_SIZE]);

      if (res.rowCount === 0) break;

      for (const row of res.rows) {
        lastId = row.id;
        const raw = row.media_name || '';
        const out = BASENAME ? extractBaseName(raw) : raw;
        ws.write(out + '\n');
        written++;
      }

      if (written % (BATCH_SIZE * 2) === 0) {
        console.log(`   … progresso: ${written}/${total}`);
      }
    }
  } finally {
    await dest.end();
    ws.end();
  }

  console.log(`✅ Concluído: ${written} linhas gravadas em ${OUTPUT}`);
}

main().catch(err => {
  console.error('❌ Erro:', err.message);
  process.exit(1);
});
