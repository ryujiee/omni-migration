// migrations/migrateCampaigns.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateCampaigns(ctx = {}) {
  console.log('📢 Migrando "Campaigns" → "campaigns"...');

  // Tamanho do lote (ajuste via .env BATCH_SIZE). 1000 ≈ 12k params < 65535
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 1000);

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
    application_name: 'migrateCampaigns:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateCampaigns:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // 0) Pré-carrega info de canais no DEST para evitar N+1 queries
    //    - Set de canais válidos (por id)
    //    - Fallback por empresa: MIN(id) por company_id
    const validChannelIds = new Set(
      (await dest.query('SELECT id FROM channel_instances')).rows.map(r => String(r.id))
    );
    const fallbackByCompany = new Map(
      (await dest.query(
        'SELECT company_id, MIN(id) AS channel_id FROM channel_instances GROUP BY company_id'
      )).rows.map(r => [String(r.company_id), r.channel_id])
    );

    // 1) Conta total p/ progresso/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."Campaigns"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
    `;
    const countRes = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(countRes.rows[0]?.total || 0);

    if (total === 0) {
      console.log(
        tenantId
          ? `⚠️  Nenhuma campanha encontrada para TENANT_ID=${tenantId}.`
          : '⚠️  Nenhuma campanha encontrada na origem.'
      );
      return;
    }

    // 2) Cursor server-side para stream em lotes (ordem estável)
    const baseSelectOrdered = `
      SELECT
        id,
        name,
        start,
        status,
        "sessionId" AS channel_id,
        message1,
        message2,
        message3,
        "mediaUrl",
        delay,
        "tenantId" AS company_id,
        "createdAt",
        "updatedAt"
      FROM "public"."Campaigns"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
      ORDER BY id
    `;
    const cursor = source.query(new Cursor(baseSelectOrdered, tenantId ? [tenantId] : []));

    // 3) Barra de progresso com ETA e rate
    const bar = new cliProgress.SingleBar(
      {
        format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s',
        hideCursor: true
      },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    const startedAt = Date.now();
    let processed = 0;
    let migradas = 0;
    let ignoradas = 0;

    // 4) Loop de leitura por lote
    while (true) {
      const rows = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!rows || rows.length === 0) break;

      // Monta multi-VALUES
      const values = [];
      const placeholders = [];

      // Mantém também os dados "crus" do lote para fallback por registro se der erro no INSERT batelado
      const perRowParams = [];

      rows.forEach((row, i) => {
        const messages = [row.message1, row.message2, row.message3]
          .map(m => (typeof m === 'string' ? m.trim() : m))
          .filter(m => !!m && String(m).length > 0);

        const normStatus = normalizeStatus(row.status);

        // Canal: aceita o de origem se existir no DEST; senão, fallback por empresa; senão, null
        let channelId = row.channel_id ? String(row.channel_id) : null;
        if (channelId && !validChannelIds.has(channelId)) {
          channelId = null;
        }
        if (!channelId) {
          const fb = fallbackByCompany.get(String(row.company_id));
          channelId = fb || null;
        }

        const v = [
          row.id,                               // id
          safeName(row.name, row.id),           // name
          row.start || null,                    // start_at
          normStatus,                           // status
          toInt(row.delay, 0),                  // delay_seconds
          JSON.stringify(messages),             // messages (jsonb)
          row.mediaUrl || null,                 // media_path
          null,                                 // template_id (origem não tem)
          row.company_id,                       // company_id
          channelId,                            // channel_id
          row.createdAt,                        // created_at
          row.updatedAt                         // updated_at
        ];
        perRowParams.push(v);

        // 12 colunas por linha ⇒ placeholders
        const base = i * 12;
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}::jsonb, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12})`
        );
        values.push(...v);
      });

      const upsertSql = `
        INSERT INTO campaigns (
          id, name, start_at, status, delay_seconds,
          messages, media_path, template_id,
          company_id, channel_id, created_at, updated_at
        ) VALUES
          ${placeholders.join(',')}
        ON CONFLICT (id) DO UPDATE SET
          name          = EXCLUDED.name,
          start_at      = EXCLUDED.start_at,
          status        = EXCLUDED.status,
          delay_seconds = EXCLUDED.delay_seconds,
          messages      = EXCLUDED.messages,
          media_path    = EXCLUDED.media_path,
          company_id    = EXCLUDED.company_id,
          channel_id    = EXCLUDED.channel_id,
          updated_at    = EXCLUDED.updated_at
      `;

      // Transação por lote com commit rápido
      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(upsertSql, values);
        await dest.query('COMMIT');

        migradas += rows.length;
      } catch (batchErr) {
        // Fallback: tenta registro a registro pra não perder o lote inteiro
        await dest.query('ROLLBACK');
        for (const v of perRowParams) {
          try {
            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');
            await dest.query(
              `
              INSERT INTO campaigns (
                id, name, start_at, status, delay_seconds,
                messages, media_path, template_id,
                company_id, channel_id, created_at, updated_at
              ) VALUES (
                $1, $2, $3, $4, $5,
                $6::jsonb, $7, $8,
                $9, $10, $11, $12
              )
              ON CONFLICT (id) DO UPDATE SET
                name          = EXCLUDED.name,
                start_at      = EXCLUDED.start_at,
                status        = EXCLUDED.status,
                delay_seconds = EXCLUDED.delay_seconds,
                messages      = EXCLUDED.messages,
                media_path    = EXCLUDED.media_path,
                company_id    = EXCLUDED.company_id,
                channel_id    = EXCLUDED.channel_id,
                updated_at    = EXCLUDED.updated_at
              `,
              v
            );
            await dest.query('COMMIT');
            migradas += 1;
          } catch (rowErr) {
            await dest.query('ROLLBACK');
            ignoradas += 1;
            console.error(`❌ Erro ao migrar campanha id=${v[0]}: ${rowErr.message}`);
          }
        }
      }

      processed += rows.length;

      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = processed > 0 && elapsed > 0 ? (processed / elapsed).toFixed(1) : '0.0';
      bar.update(processed, { rate });
    }

    bar.stop();
    await new Promise((resolve, reject) => cursor.close(err => (err ? reject(err) : resolve())));
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`✅ Migradas ${migradas}/${total} campanhas em ${secs}s. (${ignoradas} ignoradas)`);
  } finally {
    await source.end();
    await dest.end();
  }
};

// ---------- helpers ----------
function normalizeStatus(s) {
  if (!s) return 'scheduled';
  const v = String(s).toLowerCase();
  switch (v) {
    case 'pending':
    case 'scheduled':
      return 'scheduled';
    case 'in_progress':
    case 'running':
      return 'running';
    case 'completed':
    case 'finished':
      return 'finished';
    case 'canceled':
    case 'cancelled':
      return 'canceled';
    default:
      return 'scheduled';
  }
}

function toInt(v, def = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : def;
}

function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Campanha ${id}`;
}
