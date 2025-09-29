// migrations/migrateInternalMessages.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateInternalMessages(ctx = {}) {
  console.log('📨 Migrando "InternalMessage" → "internal_messages"...');

  // 12 params/linha (media_name='' e is_deleted=false são literais) → 3000 linhas ≈ 36k params
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
    application_name: 'migrateInternalMessages:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateInternalMessages:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // —— COUNT para barra/ETA
    let countSql, countParams, selectSql, selectParams;
    if (tenantId) {
      countSql = `
        SELECT COUNT(*)::bigint AS total
        FROM "public"."InternalMessage" im
        WHERE EXISTS (SELECT 1 FROM "public"."Users" u WHERE u.id = im."senderId" AND u."tenantId" = $1)
           OR EXISTS (SELECT 1 FROM "public"."Users" u2 WHERE u2.id = im."receiverId" AND u2."tenantId" = $1)
      `;
      selectSql = `
        SELECT *
        FROM "public"."InternalMessage" im
        WHERE EXISTS (SELECT 1 FROM "public"."Users" u WHERE u.id = im."senderId" AND u."tenantId" = $1)
           OR EXISTS (SELECT 1 FROM "public"."Users" u2 WHERE u2.id = im."receiverId" AND u2."tenantId" = $1)
        ORDER BY im.id
      `;
      countParams = [tenantId];
      selectParams = [tenantId];
    } else {
      countSql = `SELECT COUNT(*)::bigint AS total FROM "public"."InternalMessage"`;
      selectSql = `SELECT * FROM "public"."InternalMessage" ORDER BY id`;
      countParams = [];
      selectParams = [];
    }

    const countRes = await source.query(countSql, countParams);
    const total = Number(countRes.rows[0]?.total || 0);

    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhuma InternalMessage para TENANT_ID=${tenantId}.`
          : '⚠️  Nenhuma InternalMessage na origem.'
      );
      return;
    }

    // —— Cursor server-side
    const cursor = source.query(new Cursor(selectSql, selectParams));

    // —— Barra de progresso
    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    const startedAt = Date.now();
    let processed = 0;
    let migradas = 0;
    let erros = 0;

    // —— UPSERT (media_name='' e is_deleted=false)
    const upsertSqlSingle = `
      INSERT INTO internal_messages (
        id, body, media_type, media_name, media_url, data_json, ack,
        is_deleted, sender_id, recipient_id, group_id, is_group_message,
        created_at, updated_at
      ) VALUES (
        $1, $2, $3, '', $4, $5::jsonb, $6,
        false, $7, $8, $9, $10,
        $11, $12
      )
      ON CONFLICT (id) DO UPDATE SET
        body             = EXCLUDED.body,
        media_type       = EXCLUDED.media_type,
        media_url        = EXCLUDED.media_url,
        data_json        = EXCLUDED.data_json,
        ack              = EXCLUDED.ack,
        is_deleted       = EXCLUDED.is_deleted,
        sender_id        = EXCLUDED.sender_id,
        recipient_id     = EXCLUDED.recipient_id,
        group_id         = EXCLUDED.group_id,
        is_group_message = EXCLUDED.is_group_message,
        updated_at       = EXCLUDED.updated_at
    `;

    // —— Loop por lote
    while (true) {
      const batch = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!batch || batch.length === 0) break;

      const placeholders = [];
      const values = [];
      const perRowParams = []; // fallback se batch falhar

      batch.forEach((row, i) => {
        const id = get(row, 'id');
        const body = get(row, 'text') || '';
        const mediaType = normalizeMedia(get(row, 'mediaType'));
        const mediaUrl = get(row, 'mediaUrl') || '';
        const dataJson = JSON.stringify(parseJSON(get(row, 'dataJson')));
        const ack = normalizeAck(get(row, 'read'));
        const senderId = get(row, 'senderId') || null;
        const recipientId = get(row, 'receiverId') || null;
        const groupId = get(row, 'groupId') || null;
        const isGroup = !!groupId;
        const createdAt = get(row, 'createdAt');
        const updatedAt = get(row, 'updatedAt');

        const v = [
          id, body, mediaType, mediaUrl, dataJson, ack,
          senderId, recipientId, groupId, isGroup, createdAt, updatedAt
        ];
        perRowParams.push(v);

        const base = i * 12;
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, '', $${base + 4}, $${base + 5}::jsonb, $${base + 6}, false, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12})`
        );
        values.push(...v);
      });

      if (placeholders.length === 0) {
        processed += batch.length;
        const elapsed = (Date.now() - startedAt) / 1000;
        bar.update(processed, { rate: (processed / Math.max(1, elapsed)).toFixed(1) });
        continue;
      }

      // —— Execução do lote
      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(
          `
          INSERT INTO internal_messages (
            id, body, media_type, media_name, media_url, data_json, ack,
            is_deleted, sender_id, recipient_id, group_id, is_group_message,
            created_at, updated_at
          ) VALUES
            ${placeholders.join(',')}
          ON CONFLICT (id) DO UPDATE SET
            body             = EXCLUDED.body,
            media_type       = EXCLUDED.media_type,
            media_url        = EXCLUDED.media_url,
            data_json        = EXCLUDED.data_json,
            ack              = EXCLUDED.ack,
            is_deleted       = EXCLUDED.is_deleted,
            sender_id        = EXCLUDED.sender_id,
            recipient_id     = EXCLUDED.recipient_id,
            group_id         = EXCLUDED.group_id,
            is_group_message = EXCLUDED.is_group_message,
            updated_at       = EXCLUDED.updated_at
          `,
          values
        );
        await dest.query('COMMIT');
        migradas += placeholders.length;
      } catch (batchErr) {
        await dest.query('ROLLBACK');
        // —— fallback: registro a registro
        for (const v of perRowParams) {
          try {
            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');
            await dest.query(upsertSqlSingle, v);
            await dest.query('COMMIT');
            migradas += 1;
          } catch (rowErr) {
            await dest.query('ROLLBACK');
            erros += 1;
            console.error(`❌ Erro ao migrar InternalMessage id=${v[0]}: ${rowErr.message}`);
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
    console.log(`✅ Migradas ${migradas}/${total} InternalMessage em ${secs}s. (${erros} com erro)`);
  } finally {
    await source.end();
    await dest.end();
  }
};

// —— helpers
function get(row, key) {
  if (key in row) return row[key];
  const low = key.toLowerCase();
  for (const k of Object.keys(row)) if (k.toLowerCase() === low) return row[k];
  return null;
}
function normalizeMedia(mt) {
  const v = String(mt || 'text');
  return ['conversation', 'extendedTextMessage', 'chat'].includes(v) ? 'text' : v;
}
function parseJSON(v) {
  if (v == null) return {};
  if (typeof v === 'string') {
    try { return JSON.parse(v); } catch { return {}; }
  }
  return typeof v === 'object' ? v : {};
}
function normalizeAck(readVal) {
  if (readVal === true || readVal === 1 || String(readVal).toLowerCase() === 'true') return 'read';
  return 'sent';
}
