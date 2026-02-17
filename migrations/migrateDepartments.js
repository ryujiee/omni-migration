// migrations/migrateDepartments.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateDepartments(ctx = {}) {
  console.log('🏢 Migrando "Queues" → "departments"...');

  // 24 params/linha (transfer_type é literal) → 2000 linhas ≈ 48k params < 65535
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);

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
          '',                                    // 5  color
          toBool(row.inactivity_active, false),  // 6  inactivity_active
          toNonNegInt(row.inactivity_seconds),   // 7  inactivity_seconds
          row.inactivity_action || '',           // 8  inactivity_action
          row.inactivity_target_id || null,      // 9  inactivity_target_id
          false,                                 // 10 open_inactivity_active
          0,                                     // 11 open_inactivity_seconds
          '',                                    // 12 open_inactivity_action
          null,                                  // 13 open_inactivity_target_id
          false,                                 // 14 email_on_close_enabled
          false,                                 // 15 rating_enabled
          null,                                  // 16 rating_flow_id
          '',                                    // 17 rating_timeout_message
          0,                                     // 18 rating_timeout_seconds
          '',                                    // 19 ai_context
          '',                                    // 20 ai_for_who
          '',                                    // 21 ai_how
          '[]',                                  // 22 ai_keywords
          row.createdAt,                         // 23 created_at
          row.updatedAt                          // 24 updated_at
        ];
        perRowParams.push(v);

        const base = i * 24;
        // transfer_type é literal 'queue' (coluna 5)
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, 'queue', $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12}, $${base + 13}, $${base + 14}, $${base + 15}, $${base + 16}, $${base + 17}, $${base + 18}, $${base + 19}, $${base + 20}, $${base + 21}, $${base + 22}::jsonb, $${base + 23}, $${base + 24})`
        );
        values.push(...v);
      });

      const upsertSql = `
        INSERT INTO departments (
          id, name, status, company_id, color, transfer_type,
          inactivity_active, inactivity_seconds, inactivity_action,
          inactivity_target_id,
          open_inactivity_active, open_inactivity_seconds, open_inactivity_action, open_inactivity_target_id,
          email_on_close_enabled,
          rating_enabled, rating_flow_id, rating_timeout_message, rating_timeout_seconds,
          ai_context, ai_for_who, ai_how, ai_keywords,
          created_at, updated_at
        ) VALUES
          ${placeholders.join(',')}
        ON CONFLICT (id) DO UPDATE SET
          name                = EXCLUDED.name,
          status              = EXCLUDED.status,
          company_id          = EXCLUDED.company_id,
          color               = EXCLUDED.color,
          transfer_type       = EXCLUDED.transfer_type,
          inactivity_active   = EXCLUDED.inactivity_active,
          inactivity_seconds  = EXCLUDED.inactivity_seconds,
          inactivity_action   = EXCLUDED.inactivity_action,
          inactivity_target_id= EXCLUDED.inactivity_target_id,
          open_inactivity_active = EXCLUDED.open_inactivity_active,
          open_inactivity_seconds = EXCLUDED.open_inactivity_seconds,
          open_inactivity_action = EXCLUDED.open_inactivity_action,
          open_inactivity_target_id = EXCLUDED.open_inactivity_target_id,
          email_on_close_enabled = EXCLUDED.email_on_close_enabled,
          rating_enabled      = EXCLUDED.rating_enabled,
          rating_flow_id      = EXCLUDED.rating_flow_id,
          rating_timeout_message = EXCLUDED.rating_timeout_message,
          rating_timeout_seconds = EXCLUDED.rating_timeout_seconds,
          ai_context          = EXCLUDED.ai_context,
          ai_for_who          = EXCLUDED.ai_for_who,
          ai_how              = EXCLUDED.ai_how,
          ai_keywords         = EXCLUDED.ai_keywords,
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
                id, name, status, company_id, color, transfer_type,
                inactivity_active, inactivity_seconds, inactivity_action,
                inactivity_target_id,
                open_inactivity_active, open_inactivity_seconds, open_inactivity_action, open_inactivity_target_id,
                email_on_close_enabled,
                rating_enabled, rating_flow_id, rating_timeout_message, rating_timeout_seconds,
                ai_context, ai_for_who, ai_how, ai_keywords,
                created_at, updated_at
              ) VALUES (
                $1, $2, $3, $4, $5, 'queue',
                $6, $7, $8,
                $9, $10, $11, $12, $13,
                $14, $15, $16, $17, $18,
                $19, $20, $21, $22::jsonb,
                $23, $24
              )
              ON CONFLICT (id) DO UPDATE SET
                name                = EXCLUDED.name,
                status              = EXCLUDED.status,
                company_id          = EXCLUDED.company_id,
                color               = EXCLUDED.color,
                transfer_type       = EXCLUDED.transfer_type,
                inactivity_active   = EXCLUDED.inactivity_active,
                inactivity_seconds  = EXCLUDED.inactivity_seconds,
                inactivity_action   = EXCLUDED.inactivity_action,
                inactivity_target_id= EXCLUDED.inactivity_target_id,
                open_inactivity_active = EXCLUDED.open_inactivity_active,
                open_inactivity_seconds = EXCLUDED.open_inactivity_seconds,
                open_inactivity_action = EXCLUDED.open_inactivity_action,
                open_inactivity_target_id = EXCLUDED.open_inactivity_target_id,
                email_on_close_enabled = EXCLUDED.email_on_close_enabled,
                rating_enabled      = EXCLUDED.rating_enabled,
                rating_flow_id      = EXCLUDED.rating_flow_id,
                rating_timeout_message = EXCLUDED.rating_timeout_message,
                rating_timeout_seconds = EXCLUDED.rating_timeout_seconds,
                ai_context          = EXCLUDED.ai_context,
                ai_for_who          = EXCLUDED.ai_for_who,
                ai_how              = EXCLUDED.ai_how,
                ai_keywords         = EXCLUDED.ai_keywords,
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
