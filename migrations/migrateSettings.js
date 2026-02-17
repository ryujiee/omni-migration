// migrations/migrateSettings.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateSettings(ctx = {}) {
  console.log('⚙️  Migrando "Settings" → "settings"...');

  // Distintos por tenant costumam ser poucos, mas mantemos a mesma infra de lote.
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
    application_name: 'migrateSettings:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateSettings:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // 1) COUNT (DISTINCT tenantId) para progresso/ETA
    const countSql = `
      SELECT COUNT(DISTINCT "tenantId")::bigint AS total
      FROM "public"."Settings"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
    `;
    const countRes = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(countRes.rows[0]?.total || 0);
    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhuma configuração encontrada para TENANT_ID=${tenantId}.`
          : '⚠️  Nenhuma configuração encontrada na origem.'
      );
      return;
    }

    // 2) Cursor server-side com DISTINCT + ORDER
    const selectSql = `
      SELECT DISTINCT "tenantId" AS company_id
      FROM "public"."Settings"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
      ORDER BY "tenantId"
    `;
    const cursor = source.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    // 3) Barra de progresso
    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    const startedAt = Date.now();
    let processed = 0;
    let afetadas = 0; // total de companies tratadas
    let erros = 0;

    while (true) {
      const batch = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!batch || batch.length === 0) break;

      // Filtra nulos/duplicados defensivamente
      const ids = Array.from(new Set(batch.map(r => r.company_id).filter(v => v != null)));

      if (ids.length === 0) {
        processed += batch.length;
        const elapsed = (Date.now() - startedAt) / 1000;
        bar.update(processed, { rate: (processed / Math.max(1, elapsed)).toFixed(1) });
        continue;
      }

      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');

        // 3.1) UPDATE em massa para quem já existe
        await dest.query(
          `
          UPDATE settings
             SET message_signature = TRUE,
                 view_chatbot      = TRUE,
                 allow_user_disable_message_signature = COALESCE(allow_user_disable_message_signature, TRUE),
                 smtp = COALESCE(smtp, '{}'::jsonb),
                 support_ticket_config = COALESCE(support_ticket_config, '{}'::jsonb),
                 updated_at        = NOW()
           WHERE company_id = ANY($1::int[])
          `,
          [ids]
        );

        // 3.2) INSERT apenas dos faltantes (anti-join via UNNEST)
        await dest.query(
          `
          INSERT INTO settings (
            company_id, message_signature, view_chatbot, allow_user_disable_message_signature,
            smtp, support_ticket_config, created_at, updated_at
          )
          SELECT s.company_id, TRUE, TRUE, TRUE, '{}'::jsonb, '{}'::jsonb, NOW(), NOW()
          FROM UNNEST($1::int[]) AS s(company_id)
          LEFT JOIN settings t ON t.company_id = s.company_id
          WHERE t.company_id IS NULL
          `,
          [ids]
        );

        await dest.query('COMMIT');
        afetadas += ids.length;
      } catch (e) {
        await dest.query('ROLLBACK');
        erros += 1;
        console.error(`❌ Falha no lote de settings (companies=${ids.length}): ${e.message}`);
      }

      processed += batch.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = (processed / Math.max(1, elapsed)).toFixed(1);
      bar.update(processed, { rate });
    }

    bar.stop();
    await new Promise((resolve, reject) => cursor.close(err => (err ? reject(err) : resolve())));
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`✅ Settings aplicadas para ${afetadas}/${total} empresa(s) em ${secs}s.${erros ? ` (${erros} lote(s) com erro)` : ''}`);
  } finally {
    await source.end();
    await dest.end();
  }
};
