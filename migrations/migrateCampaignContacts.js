// migrations/migrateCampaignContacts.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateCampaignContacts(ctx = {}) {
  console.log('👥 Migrando "CampaignContacts" → "campaign_contacts"...');

  // Lote configurável (1000 é um bom ponto de partida)
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 1000);

  // Usa TENANT_ID do ctx (preferencial) ou do .env)
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
    application_name: 'migrateCampaignContacts:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateCampaignContacts:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // 1) Conta total para progresso/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."CampaignContacts" cc
      JOIN "public"."Campaigns" c ON c.id = cc."campaignId"
      ${tenantId ? 'WHERE c."tenantId" = $1' : ''}
    `;
    const countRes = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(countRes.rows[0]?.total || 0);

    if (total === 0) {
      console.log(
        tenantId
          ? `⚠️  Nenhum CampaignContact encontrado para TENANT_ID=${tenantId}.`
          : '⚠️  Nenhum CampaignContact encontrado na origem.'
      );
      return;
    }

    // 2) Cursor server-side para stream em lotes
    //    (ordenar por id garante paginação estável)
    const baseSelectOrdered = `
      SELECT
        cc.id,
        cc."campaignId" AS campaign_id,
        cc."contactId"  AS contact_id,
        cc.ack,
        cc."timestamp",
        c.message1,
        c.message2,
        c.message3,
        c."mediaUrl" AS sent_media_path,
        cc."createdAt",
        cc."updatedAt"
      FROM "public"."CampaignContacts" cc
      JOIN "public"."Campaigns" c ON c.id = cc."campaignId"
      ${tenantId ? 'WHERE c."tenantId" = $1' : ''}
      ORDER BY cc.id
    `;
    const cursor = source.query(new Cursor(baseSelectOrdered, tenantId ? [tenantId] : []));

    // 3) Barra de progresso com ETA
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
    let batchNum = 0;

    // 4) Loop de leitura por lote
    while (true) {
      const rows = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!rows || rows.length === 0) break;

      batchNum++;

      // 4.1) Transforma lote e monta VALUES multi-rows
      const values = [];
      const placeholders = [];

      rows.forEach((row, i) => {
        // Map de ack → status
        // -1 = error; >=1 = sent; outros = pending
        let status = 'pending';
        if (row.ack === -1) status = 'error';
        else if (Number(row.ack) >= 1) status = 'sent';

        // Se tiver timestamp (resposta registrada), marca responded
        let responded = false;
        let respondedAt = null;
        if (row.timestamp && Number(row.timestamp) !== 0) {
          responded = true;
          respondedAt = new Date(Number(row.timestamp) * 1000); // origem em segundos
        }

        const sentPreview =
          [row.message1, row.message2, row.message3]
            .find(v => typeof v === 'string' && v.trim().length > 0) || null;
        const sentAt = status === 'pending' ? null : row.createdAt || null;
        const errorMsg = status === 'error' ? 'Falha no envio migrada da plataforma antiga (ack=-1).' : null;

        // 13 colunas por linha ↓
        const base = i * 13;
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12}, $${base + 13})`
        );

        values.push(
          row.id,              // $1  id
          row.campaign_id,     // $2  campaign_id
          row.contact_id,      // $3  contact_id
          status,              // $4  status
          errorMsg,            // $5  error_msg
          responded,           // $6  responded
          respondedAt,         // $7  responded_at
          null,                // $8  expired_at
          sentAt,              // $9  sent_at
          sentPreview,         // $10 sent_preview
          row.sent_media_path || null, // $11 sent_media_path
          row.createdAt,       // $12 created_at
          row.updatedAt        // $13 updated_at
        );
      });

      const upsertSql = `
        INSERT INTO campaign_contacts (
          id, campaign_id, contact_id, status, error_msg, responded, responded_at, expired_at,
          sent_at, sent_preview, sent_media_path, created_at, updated_at
        ) VALUES
          ${placeholders.join(',')}
        ON CONFLICT (id) DO UPDATE SET
          campaign_id  = EXCLUDED.campaign_id,
          contact_id   = EXCLUDED.contact_id,
          status       = EXCLUDED.status,
          error_msg    = EXCLUDED.error_msg,
          responded    = EXCLUDED.responded,
          responded_at = EXCLUDED.responded_at,
          expired_at   = EXCLUDED.expired_at,
          sent_at      = EXCLUDED.sent_at,
          sent_preview = EXCLUDED.sent_preview,
          sent_media_path = EXCLUDED.sent_media_path,
          updated_at   = EXCLUDED.updated_at
      `;

      // 4.2) Transação por lote + commit rápido
      await dest.query('BEGIN');
      await dest.query('SET LOCAL synchronous_commit TO OFF'); // acelera commits deste TX
      await dest.query(upsertSql, values);
      await dest.query('COMMIT');

      processed += rows.length;

      // 4.3) Atualiza barra (rate e ETA)
      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = processed > 0 && elapsed > 0 ? (processed / elapsed).toFixed(1) : '0.0';
      bar.update(processed, { rate });
    }

    bar.stop();
    await new Promise((resolve, reject) => cursor.close(err => (err ? reject(err) : resolve())));
    console.log(`✅ Migrados ${processed}/${total} contatos de campanha em ${((Date.now() - startedAt) / 1000).toFixed(1)}s.`);
  } finally {
    await source.end();
    await dest.end();
  }
};
