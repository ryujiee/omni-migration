'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migratePrivateMessages(ctx = {}) {
  console.log('📨 Migrando "PrivateMessage" → "internal_messages"... (batched + progress + schema-aware)');

  // cuidado com limite de parâmetros: ~ 65535
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);
  const PROGRESS_EVERY = Number(process.env.INTERNAL_PROGRESS_EVERY || 200);

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
    application_name: 'migratePrivateMessages:source',
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migratePrivateMessages:dest',
  });

  await source.connect();
  await dest.connect();

  let cursor;
  const bar = new cliProgress.SingleBar(
    {
      format:
        'Internal |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s | {status}',
      hideCursor: true,
      clearOnComplete: false,
    },
    cliProgress.Presets.shades_classic,
  );

  try {
    // 0) Detectar colunas do DEST
    const destCols = await loadDestColumns(dest, 'internal_messages');
    const has = (c) => destCols.has(String(c).toLowerCase());

    // 1) Pré-carregar users válidos (FK)
    const whereCompany = tenantId ? 'WHERE company_id=$1' : '';
    const pCompany = tenantId ? [tenantId] : [];

    // users sempre é importante para sender_id
    const usersRes = await dest.query(`SELECT id FROM users ${whereCompany}`, pCompany).catch(() => ({ rows: [] }));
    const validUsers = new Set(usersRes.rows.map((r) => Number(r.id)));

    // 2) COUNT + SELECT (origem)
    let countSql, countParams, selectSql, selectParams;

    if (tenantId) {
      countSql = `
        SELECT COUNT(*)::bigint AS total
        FROM "public"."PrivateMessage" im
        WHERE EXISTS (SELECT 1 FROM "public"."Users" u  WHERE u.id  = im."senderId"   AND u."tenantId" = $1)
           OR EXISTS (SELECT 1 FROM "public"."Users" u2 WHERE u2.id = im."receiverId" AND u2."tenantId" = $1)
      `;
      selectSql = `
        SELECT im.*
        FROM "public"."PrivateMessage" im
        WHERE EXISTS (SELECT 1 FROM "public"."Users" u  WHERE u.id  = im."senderId"   AND u."tenantId" = $1)
           OR EXISTS (SELECT 1 FROM "public"."Users" u2 WHERE u2.id = im."receiverId" AND u2."tenantId" = $1)
        ORDER BY im.id
      `;
      countParams = [tenantId];
      selectParams = [tenantId];
    } else {
      countSql = `SELECT COUNT(*)::bigint AS total FROM "public"."PrivateMessage"`;
      selectSql = `SELECT * FROM "public"."PrivateMessage" ORDER BY id`;
      countParams = [];
      selectParams = [];
    }

    const countRes = await source.query(countSql, countParams);
    const total = Number(countRes.rows[0]?.total || 0);

    if (!total) {
      console.log(tenantId ? `⚠️ Nenhuma PrivateMessage para TENANT_ID=${tenantId}.` : '⚠️ Nenhuma PrivateMessage na origem.');
      return;
    }
    console.log(`📦 Total na origem${tenantId ? ` (tenant ${tenantId})` : ''}: ${total}`);

    cursor = source.query(new Cursor(selectSql, selectParams));

    // 3) Colunas alvo (dinâmicas)
    const targetCols = [
      'id',
      has('body') ? 'body' : null,
      has('media_type') ? 'media_type' : null,
      has('media_name') ? 'media_name' : null,
      has('media_url') ? 'media_url' : null,
      has('data_json') ? 'data_json' : null,
      has('ack') ? 'ack' : null,

      // novos campos no destino (se existirem)
      has('quoted_message_id') ? 'quoted_message_id' : null,
      has('edited_at') ? 'edited_at' : null,
      has('edited_message') ? 'edited_message' : null,
      has('is_deleted') ? 'is_deleted' : null,
      has('deleted_at') ? 'deleted_at' : null,

      has('sender_id') ? 'sender_id' : null,
      has('recipient_id') ? 'recipient_id' : null,
      has('group_id') ? 'group_id' : null,
      has('is_group_message') ? 'is_group_message' : null,

      has('reactions') ? 'reactions' : null,
      has('forwarded_from_id') ? 'forwarded_from_id' : null,

      has('created_at') ? 'created_at' : null,
      has('updated_at') ? 'updated_at' : null,
    ].filter(Boolean);

    const insertColsSql = targetCols.join(', ');
    const updateColsSql = targetCols
      .filter((c) => c !== 'id' && c !== 'created_at')
      .map((c) => `${c}=EXCLUDED.${c}`)
      .join(', ');

    const startedAt = Date.now();
    let processed = 0;
    let migrated = 0;
    let skipped = 0;
    let errors = 0;
    let skippedMissingSender = 0;

    bar.start(total, 0, { rate: '0.0', status: 'Iniciando...' });

    while (true) {
      const batch = await readCursor(cursor, BATCH_SIZE);
      if (!batch || batch.length === 0) break;

      const values = [];
      const placeholders = [];
      const perRowParams = [];

      let rowIndex = 0;

      for (const row of batch) {
        const id = toInt(get(row, 'id'), null);
        const senderId = toInt(get(row, 'senderId'), null);

        // sender_id é obrigatório na model nova (uint, não pointer)
        // se não existir no destino -> não tem como migrar sem criar usuário
        if (!Number.isInteger(id) || !Number.isInteger(senderId) || !validUsers.has(senderId)) {
          skipped++;
          if (!Number.isInteger(senderId) || !validUsers.has(senderId)) skippedMissingSender++;
          continue;
        }

        const body = sanitizeUtf16(get(row, 'text') || '');
        const mediaType = sanitizeUtf16(normalizeMedia(get(row, 'mediaType')));
        const mediaUrl = sanitizeUtf16(get(row, 'mediaUrl') || '');

        const dataObj = parseJSON(get(row, 'dataJson'));
        const dataJson = safeJsonb(dataObj);

        const ack = normalizeAck(get(row, 'read'));

        const recipientIdRaw = toInt(get(row, 'receiverId'), null);
        const recipientId = Number.isInteger(recipientIdRaw) && validUsers.has(recipientIdRaw) ? recipientIdRaw : null;

        const groupIdRaw = toInt(get(row, 'groupId'), null);
        const groupId = Number.isInteger(groupIdRaw) ? groupIdRaw : null;

        const isGroup = !!groupId;

        const createdAt = get(row, 'createdAt');
        const updatedAt = get(row, 'updatedAt');

        const payloadObj = {
          id,
          body,
          media_type: mediaType || 'text',
          media_name: has('media_name') ? '' : undefined,
          media_url: mediaUrl,
          data_json: dataJson,
          ack,

          quoted_message_id: null,
          edited_at: null,
          edited_message: null,

          is_deleted: false,
          deleted_at: null,

          sender_id: senderId,
          recipient_id: recipientId,
          group_id: groupId,
          is_group_message: isGroup,

          reactions: has('reactions') ? '[]' : undefined,
          forwarded_from_id: null,

          created_at: createdAt,
          updated_at: updatedAt,
        };

        const rowVals = objToRow(targetCols, payloadObj);

        perRowParams.push(rowVals);

        const base = rowIndex * targetCols.length;
        placeholders.push(`(${targetCols.map((_, j) => `$${base + j + 1}`).join(', ')})`);
        values.push(...rowVals);
        rowIndex++;
      }

      // nada válido no batch
      if (!placeholders.length) {
        processed += batch.length;
        tickBar(bar, processed, total, startedAt, PROGRESS_EVERY, `ok=${migrated} skip=${skipped} err=${errors} noSender=${skippedMissingSender}`);
        continue;
      }

      const upsertBatchSql = `
        INSERT INTO internal_messages (${insertColsSql})
        VALUES ${placeholders.join(',')}
        ON CONFLICT (id) DO UPDATE SET
          ${updateColsSql}
      `;

      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(upsertBatchSql, values);
        await dest.query('COMMIT');
        migrated += rowIndex;
      } catch (batchErr) {
        try { await dest.query('ROLLBACK'); } catch {}

        // fallback row-by-row pra isolar o erro sem travar tudo
        for (const v of perRowParams) {
          try {
            const singleSql = makeSingleUpsert(targetCols);
            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');
            await dest.query(singleSql, v);
            await dest.query('COMMIT');
            migrated += 1;
          } catch (rowErr) {
            try { await dest.query('ROLLBACK'); } catch {}
            errors += 1;
            // não spammar — deixa só 1 linha por erro (já é suficiente)
            console.error(`❌ PrivateMessage id=${v[targetCols.indexOf('id')]}: ${rowErr.message}`);
          }
        }
      }

      processed += batch.length;
      tickBar(bar, processed, total, startedAt, PROGRESS_EVERY, `ok=${migrated} skip=${skipped} err=${errors} noSender=${skippedMissingSender}`);
    }

    bar.stop();

    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`✅ PrivateMessages: migrated=${migrated}, skipped=${skipped} (noSender=${skippedMissingSender}), errors=${errors} em ${secs}s`);
  } finally {
    try {
      if (cursor) await new Promise((resolve) => cursor.close(() => resolve()));
    } catch {}
    try { bar.stop(); } catch {}
    await source.end();
    await dest.end();
  }
};

/* ================= helpers ================= */

async function loadDestColumns(client, tableName) {
  const res = await client.query(
    `
    SELECT lower(column_name) AS column_name
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name=$1
    `,
    [tableName],
  );
  return new Set(res.rows.map((r) => r.column_name));
}

async function readCursor(cursor, size) {
  return await new Promise((resolve, reject) => {
    cursor.read(size, (err, rows) => (err ? reject(err) : resolve(rows || [])));
  });
}

function tickBar(bar, done, total, startedAtMs, every, status) {
  if (done % every !== 0 && done !== total) return;
  const elapsed = (Date.now() - startedAtMs) / 1000;
  const rate = (done / Math.max(1, elapsed)).toFixed(1);
  bar.update(done, { rate, status });
}

function objToRow(cols, obj) {
  return cols.map((c) => (c in obj ? obj[c] : null));
}

function toInt(v, def = null) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : def;
}

function get(row, key) {
  if (key in row) return row[key];
  const low = String(key).toLowerCase();
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

// remove NUL e surrogates inválidos
function sanitizeUtf16(str) {
  if (str == null) return str;
  let s = String(str).replace(/\u0000/g, '');
  s = s.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])/g, '\uFFFD');
  s = s.replace(/(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, '\uFFFD');
  return s;
}

function deepSanitize(o) {
  if (o == null) return o;
  if (typeof o === 'string') return sanitizeUtf16(o);
  if (Array.isArray(o)) return o.map(deepSanitize);
  if (typeof o === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(o)) out[sanitizeUtf16(k)] = deepSanitize(v);
    return out;
  }
  return o;
}

function safeJsonb(value, fallback = '{}') {
  try {
    const sani = deepSanitize(value);
    return JSON.stringify(sani);
  } catch {
    return fallback;
  }
}

function makeSingleUpsert(cols) {
  const insertCols = cols.join(', ');
  const ph = cols.map((_, i) => `$${i + 1}`).join(', ');
  const updates = cols
    .filter((c) => c !== 'id' && c !== 'created_at')
    .map((c) => `${c}=EXCLUDED.${c}`)
    .join(', ');

  return `
    INSERT INTO internal_messages (${insertCols})
    VALUES (${ph})
    ON CONFLICT (id) DO UPDATE SET
      ${updates}
  `;
}
