// migrations/migrateTaskTypes.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateTaskTypes(ctx = {}) {
  console.log('🏷️ Migrando "TodoListTypes" → "task_types"...');

  // 5 params/linha → 5000 linhas ≈ 25k params (< 65535)
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 3000);

  // Usa TENANT_ID do ctx (preferencial) ou do .env
  const tenantId =
    ctx.tenantId != null && String(ctx.tenantId).trim() !== ''
      ? String(ctx.tenantId).trim()
      : (process.env.TENANT_ID ? String(process.env.TENANT_ID).trim() : null);

  const source = new Client({
    host: process.env.SRC_HOST,
    port: process.env.SRC_PORT,
    user: process.env.SRC_USER,
    password: process.env.SRC_PASS,
    database: process.env.SRC_DB,
    application_name: 'migrateTaskTypes:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateTaskTypes:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // 1) COUNT p/ barra/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."TodoListTypes"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
    `;
    const { rows: crows } = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(crows[0]?.total || 0);
    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhum tipo de tarefa encontrado para TENANT_ID=${tenantId}.`
          : '⚠️  Nenhum tipo de tarefa encontrado na origem.'
      );
      return;
    }

    // 2) Cursor server-side (ordem estável)
    const selectSql = `
      SELECT
        id,
        type        AS name,
        "tenantId"  AS company_id,
        "createdAt",
        "updatedAt"
      FROM "public"."TodoListTypes"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
      ORDER BY id
    `;
    const cursor = source.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    // 3) Barra de progresso
    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    // 4) UPSERT single (fallback)
    const upsertSqlSingle = `
      INSERT INTO task_types (
        id, name, company_id, created_at, updated_at
      ) VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (id) DO UPDATE SET
        name       = EXCLUDED.name,
        company_id = EXCLUDED.company_id,
        updated_at = EXCLUDED.updated_at
    `;

    const startedAt = Date.now();
    let processed = 0;
    let migrados = 0;
    let erros = 0;

    // 5) Loop por lote
    while (true) {
      const batch = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!batch || batch.length === 0) break;

      const placeholders = [];
      const values = [];
      const perRowParams = [];

      batch.forEach((row, i) => {
        const v = [
          row.id,                           // 1 id
          safeName(row.name, row.id),       // 2 name
          row.company_id,                   // 3 company_id
          row.createdAt,                    // 4 created_at
          row.updatedAt                     // 5 updated_at
        ];
        perRowParams.push(v);

        const base = i * 5;
        placeholders.push(`($${base+1}, $${base+2}, $${base+3}, $${base+4}, $${base+5})`);
        values.push(...v);
      });

      // Execução do lote (multi-VALUES)
      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(
          `
          INSERT INTO task_types (
            id, name, company_id, created_at, updated_at
          ) VALUES
            ${placeholders.join(',')}
          ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            company_id = EXCLUDED.company_id,
            updated_at = EXCLUDED.updated_at
          `,
          values
        );
        await dest.query('COMMIT');
        migrados += batch.length;
      } catch (batchErr) {
        await dest.query('ROLLBACK');
        // fallback: registro a registro
        for (const v of perRowParams) {
          try {
            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');
            await dest.query(upsertSqlSingle, v);
            await dest.query('COMMIT');
            migrados += 1;
          } catch (rowErr) {
            await dest.query('ROLLBACK');
            erros += 1;
            console.error(`❌ Erro ao migrar task_type id=${v[0]}: ${rowErr.message}`);
          }
        }
      }

      processed += batch.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = (processed / Math.max(1, elapsed)).toFixed(1);
      bar.update(processed, { rate });
    }

    bar.stop();
    await new Promise((resolve, reject) => cursor.close(err => (err ? reject(err) : resolve())));
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`✅ Migrados ${migrados}/${total} tipo(s) de tarefa em ${secs}s.${erros ? ` (${erros} com erro)` : ''}`);
  } finally {
    await source.end();
    await dest.end();
  }
};

// —— helpers
function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Tipo ${id}`;
}
