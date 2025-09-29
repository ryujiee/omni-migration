// migrations/migrateDepartments.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateDepartments(ctx = {}) {
  console.log('🏢 Migrando "Queues" → "departments"...');

  // 10 params/linha (transfer_type é literal) → 3000 linhas ≈ 30k params < 65535
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
    application_name: 'migrateDepartments:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateDepartments:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // 1) total p/ barra/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."Queues"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
    `;
    const { rows: countRows } = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(countRows[0]?.total || 0);

    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhum departamento (queue) encontrado para TENANT_ID=${tenantId}.`
          : '⚠️  Nenhum departamento (queue) encontrado na origem.'
      );
      return;
    }

    // 2) cursor server-side (ordem estável)
    const selectSql = `
      SELECT
        "id",
        "queue" AS name,
        COALESCE("isActive", true) AS status,
        "tenantId" AS company_id,
        COALESCE("inactivity_enabled", false) AS inactivity_active,
        COALESCE("inactivity_timeout", 0) AS inactivity_seconds,
        "inactivity_action" AS inactivity_action,
        "inactivity_target" AS inactivity_target_id,
        "createdAt",
        "updatedAt"
      FROM "public"."Queues"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
      ORDER BY "id"
    `;
    const cursor = source.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    // 3) barra de progresso
    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    const startedAt = Date.now();
    let processed = 0;
    let migrados = 0;
    let erros = 0;

    // 4) loop por lote
    while (true) {
      const batch = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!batch || batch.length === 0) break;

      const placeholders = [];
      const values = [];
      const perRowParams = []; // fallback se batch falhar

      batch.forEach((row, i) => {
        const v = [
          row.id,                                // 1  id
          safeName(row.name, row.id),            // 2  name
          toBool(row.status, true),              // 3  status (boolean)
          row.company_id,                        // 4  company_id
          toBool(row.inactivity_active, false),  // 5  inactivity_active
          toNonNegInt(row.inactivity_seconds),   // 6  inactivity_seconds
          row.inactivity_action || null,         // 7  inactivity_action
          row.inactivity_target_id || null,      // 8  inactivity_target_id
          row.createdAt,                         // 9  created_at
          row.updatedAt                          // 10 updated_at
        ];
        perRowParams.push(v);

        const base = i * 10;
        // transfer_type é literal 'queue' (coluna 5)
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, 'queue', $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10})`
        );
        values.push(...v);
      });

      const upsertSql = `
        INSERT INTO departments (
          id, name, status, company_id, transfer_type,
          inactivity_active, inactivity_seconds, inactivity_action,
          inactivity_target_id, created_at, updated_at
        ) VALUES
          ${placeholders.join(',')}
        ON CONFLICT (id) DO UPDATE SET
          name                = EXCLUDED.name,
          status              = EXCLUDED.status,
          company_id          = EXCLUDED.company_id,
          transfer_type       = EXCLUDED.transfer_type,
          inactivity_active   = EXCLUDED.inactivity_active,
          inactivity_seconds  = EXCLUDED.inactivity_seconds,
          inactivity_action   = EXCLUDED.inactivity_action,
          inactivity_target_id= EXCLUDED.inactivity_target_id,
          updated_at          = EXCLUDED.updated_at
      `;

      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(upsertSql, values);
        await dest.query('COMMIT');
        migrados += batch.length;
      } catch (batchErr) {
        await dest.query('ROLLBACK');
        // fallback: registro a registro
        for (const v of perRowParams) {
          try {
            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');
            await dest.query(
              `
              INSERT INTO departments (
                id, name, status, company_id, transfer_type,
                inactivity_active, inactivity_seconds, inactivity_action,
                inactivity_target_id, created_at, updated_at
              ) VALUES (
                $1, $2, $3, $4, 'queue',
                $5, $6, $7,
                $8, $9, $10
              )
              ON CONFLICT (id) DO UPDATE SET
                name                = EXCLUDED.name,
                status              = EXCLUDED.status,
                company_id          = EXCLUDED.company_id,
                transfer_type       = EXCLUDED.transfer_type,
                inactivity_active   = EXCLUDED.inactivity_active,
                inactivity_seconds  = EXCLUDED.inactivity_seconds,
                inactivity_action   = EXCLUDED.inactivity_action,
                inactivity_target_id= EXCLUDED.inactivity_target_id,
                updated_at          = EXCLUDED.updated_at
              `,
              v
            );
            await dest.query('COMMIT');
            migrados += 1;
          } catch (rowErr) {
            await dest.query('ROLLBACK');
            erros += 1;
            console.error(`❌ Erro ao migrar department id=${v[0]}: ${rowErr.message}`);
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
    console.log(`✅ Migrados ${migrados}/${total} departamento(s) em ${secs}s. (${erros} com erro)`);
  } finally {
    await source.end();
    await dest.end();
  }
};

// helpers
function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Departamento ${id}`;
}
function toBool(v, def = false) {
  return typeof v === 'boolean' ? v : (v == null ? def : String(v).toLowerCase() === 'true' || Number(v) === 1);
}
function toNonNegInt(v, def = 0) {
  const n = Number(v);
  if (!Number.isFinite(n)) return def;
  return Math.max(0, Math.trunc(n));
}
