'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const INSERT_CHUNK = Number(process.env.INSERT_CHUNK || BATCH_SIZE);
const PROGRESS_EVERY = Number(process.env.MESSAGES_PROGRESS_EVERY || 200);

module.exports = async function migrateMessages(ctx = {}) {
  console.log('💬 Migrando "Messages" → "messages"... (unique(message_id) safe)');

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
    application_name: 'migrateMessages:source',
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateMessages:dest',
  });

  await source.connect();
  await dest.connect();

  let cursor;

  const bar = new cliProgress.SingleBar(
    {
      format:
        'Messages |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s | {status}',
      hideCursor: true,
      clearOnComplete: false,
    },
    cliProgress.Presets.shades_classic,
  );

  try {
    // 0) columns detect
    const destCols = await loadDestColumns(dest, 'messages');
    const has = (c) => destCols.has(String(c).toLowerCase());

    // 1) valid tickets (FK)
    const whereCompany = tenantId ? 'WHERE company_id=$1' : '';
    const pCompany = tenantId ? [tenantId] : [];

    const ticketsRes = await dest.query(`SELECT id FROM tickets ${whereCompany}`, pCompany);
    const validTickets = new Set(ticketsRes.rows.map((r) => Number(r.id)));

    // 2) count
    const countSql = tenantId
      ? `
        SELECT COUNT(*)::bigint AS total
        FROM "public"."Messages" m
        JOIN "public"."Tickets" t ON t."id" = m."ticketId"
        WHERE t."tenantId" = $1
      `
      : `SELECT COUNT(*)::bigint AS total FROM "public"."Messages"`;

    const countRes = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(countRes.rows[0]?.total || 0);

    if (!total) {
      console.log(tenantId ? `⚠️ Nenhuma mensagem para TENANT_ID=${tenantId}.` : '⚠️ Nenhuma mensagem na origem.');
      return;
    }
    console.log(`📦 Total na origem${tenantId ? ` (tenant ${tenantId})` : ''}: ${total}`);

    // 3) cursor
    const selectSql = tenantId
      ? `
        SELECT m.*
        FROM "public"."Messages" m
        JOIN "public"."Tickets" t ON t."id" = m."ticketId"
        WHERE t."tenantId" = $1
        ORDER BY m."id"
      `
      : `SELECT * FROM "public"."Messages" ORDER BY "id"`;

    cursor = source.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    // 4) staging
    await dest.query(`CREATE TEMP TABLE IF NOT EXISTS tmp_msg_map (old_id text PRIMARY KEY, new_id bigint NOT NULL)`);
    await dest.query(`CREATE TEMP TABLE IF NOT EXISTS tmp_quotes  (new_id bigint NOT NULL, quoted_old text NOT NULL)`);

    // 5) columns used
    const targetCols = [
      'ticket_id',
      has('body') ? 'body' : null,
      has('edited_body') ? 'edited_body' : null,
      has('media_type') ? 'media_type' : null,
      has('media_name') ? 'media_name' : null,
      has('message_id') ? 'message_id' : null,
      has('data_json') ? 'data_json' : null,
      has('ack') ? 'ack' : null,
      has('message_status') ? 'message_status' : null,
      has('is_deleted') ? 'is_deleted' : null,
      has('from_me') ? 'from_me' : null,
      has('user_id') ? 'user_id' : null,
      has('contact_id') ? 'contact_id' : null,
      has('schedule_date') ? 'schedule_date' : null,
      has('created_at') ? 'created_at' : null,
      has('updated_at') ? 'updated_at' : null,
    ].filter(Boolean);

    bar.start(total, 0, { rate: '0.0', status: 'Iniciando...' });

    const startedAt = Date.now();

    let processed = 0;
    let insertedTotal = 0;
    let existingTotal = 0;
    let skippedTotal = 0;
    let errorsTotal = 0;
    let quotesTotal = 0;

    // métrica importante
    let nulledMessageIdTotal = 0;

    while (true) {
      const rows = await readCursor(cursor, BATCH_SIZE);
      if (!rows || rows.length === 0) break;

      // payloads válidos (ticket FK)
      const payloads = [];
      for (const r of rows) {
        const p = buildPayload(r);
        const tid = toInt(p.ticket_id, null);
        if (!Number.isInteger(tid) || !validTickets.has(tid)) {
          skippedTotal++;
          continue;
        }
        p.ticket_id = tid;
        payloads.push(p);
      }

      if (!payloads.length) {
        processed += rows.length;
        tickBar(bar, processed, total, startedAt, PROGRESS_EVERY, `ins=${insertedTotal} exist=${existingTotal} skip=${skippedTotal} err=${errorsTotal} nulledMid=${nulledMessageIdTotal}`);
        continue;
      }

      // =========
      // PROTEÇÃO DO UNIQUE(message_id)
      // 1) detecta duplicados dentro do próprio batch
      // 2) detecta message_id já existentes no destino
      // 3) se duplicado -> message_id = NULL
      // =========
      if (has('message_id')) {
        const mids = payloads.map(p => p.message_id).filter(m => m && String(m).trim() !== '');
        const midCount = new Map();
        for (const m of mids) midCount.set(m, (midCount.get(m) || 0) + 1);

        const duplicatedInBatch = new Set([...midCount.entries()].filter(([, c]) => c > 1).map(([m]) => m));

        let existingMidSet = new Set();
        if (mids.length) {
          // consulta em chunks pra não estourar placeholders
          const chunkSize = 5000;
          for (let i = 0; i < mids.length; i += chunkSize) {
            const slice = mids.slice(i, i + chunkSize);
            const { rows: ex } = await dest.query(
              `SELECT message_id FROM messages WHERE message_id = ANY($1::text[])`,
              [slice],
            );
            for (const r of ex) if (r.message_id) existingMidSet.add(String(r.message_id));
          }
        }

        for (const p of payloads) {
          const mid = p.message_id ? String(p.message_id).trim() : '';
          if (!mid) continue;

          if (duplicatedInBatch.has(mid) || existingMidSet.has(mid)) {
            // anula para não quebrar o UNIQUE
            p.message_id = null;
            nulledMessageIdTotal++;
          }
        }
      }

      // dedupe existentes (agora com message_id possivelmente null)
      const withMsg = [];
      const withMsgKeyToOld = new Map();

      const noMsg = [];
      const noMsgKeyToOld = new Map();

      for (const p of payloads) {
        if (p.message_id) {
          const key = keyWithMsg(p.ticket_id, p.message_id);
          withMsg.push([p.ticket_id, p.message_id]);
          pushKey(withMsgKeyToOld, key, String(p.__old_id));
        } else {
          const key = keyNoMsg(p.ticket_id, p.from_me, p.created_at, p.body || '');
          noMsg.push([p.ticket_id, !!p.from_me, p.created_at, p.body || '']);
          pushKey(noMsgKeyToOld, key, String(p.__old_id));
        }
      }

      const existingOldToNew = new Map();

      if (withMsg.length) {
        const placeholders = withMsg.map((_, i) => `($${i * 2 + 1}, $${i * 2 + 2})`).join(',');
        const vals = withMsg.flat();
        const { rows: ex } = await dest.query(
          `SELECT id, ticket_id, message_id FROM messages WHERE (ticket_id, message_id) IN (${placeholders})`,
          vals,
        );
        for (const m of ex) {
          const key = keyWithMsg(m.ticket_id, m.message_id);
          const olds = withMsgKeyToOld.get(key) || [];
          for (const oldId of olds) existingOldToNew.set(oldId, Number(m.id));
        }
      }

      if (noMsg.length) {
        const placeholders = noMsg
          .map((_, i) =>
            `($${i * 4 + 1}::bigint, $${i * 4 + 2}::boolean, $${i * 4 + 3}::timestamptz, $${i * 4 + 4}::text)`,
          )
          .join(',');
        const vals = noMsg.flat();
        const { rows: ex } = await dest.query(
          `
          WITH v(ticket_id, from_me, created_at, body_norm) AS (VALUES ${placeholders})
          SELECT m.id, m.ticket_id, m.from_me, m.created_at, COALESCE(m.body,'') AS body_norm
          FROM v
          JOIN messages m
            ON m.ticket_id = v.ticket_id
           AND m.from_me   = v.from_me
           AND m.created_at= v.created_at
           AND COALESCE(m.body,'') = v.body_norm
           AND (m.message_id IS NULL OR m.message_id = '')
          `,
          vals,
        );
        for (const m of ex) {
          const key = keyNoMsg(m.ticket_id, m.from_me, m.created_at, m.body_norm);
          const olds = noMsgKeyToOld.get(key) || [];
          for (const oldId of olds) existingOldToNew.set(oldId, Number(m.id));
        }
      }

      existingTotal += existingOldToNew.size;

      const toInsert = payloads.filter((p) => !existingOldToNew.has(String(p.__old_id)));

      // insert em chunks
      const insertedMap = new Map();

      if (toInsert.length) {
        const chunks = chunkify(toInsert, INSERT_CHUNK);

        for (const chunk of chunks) {
          try {
            const { placeholders, values } = buildInsertPlaceholdersDynamic(chunk, targetCols);
            const sql = makeInsertSQLDynamic(targetCols, placeholders);

            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');

            const { rows: ret } = await dest.query(sql, values);

            await dest.query('COMMIT');

            const mapWith = new Map();
            const mapNo = new Map();

            for (const r of ret) {
              const rid = Number(r.id);
              const tId = Number(r.ticket_id);
              const mid = r.message_id ? String(r.message_id) : null;
              const bodyNorm = r.body_norm != null ? String(r.body_norm) : '';
              const fromMe = !!r.from_me;
              const createdAt = r.created_at;

              if (mid) mapWith.set(keyWithMsg(tId, mid), rid);
              else mapNo.set(keyNoMsg(tId, fromMe, createdAt, bodyNorm), rid);
            }

            for (const p of chunk) {
              let newId = null;
              if (p.message_id) newId = mapWith.get(keyWithMsg(p.ticket_id, p.message_id));
              else newId = mapNo.get(keyNoMsg(p.ticket_id, p.from_me, p.created_at, p.body || ''));
              if (newId) insertedMap.set(String(p.__old_id), newId);
            }
          } catch (e) {
            try { await dest.query('ROLLBACK'); } catch {}

            // se ainda assim aparecer unique -> fallback: anula message_id e tenta de novo 1x
            if (/idx_messages_message_id_unique|duplicate key value violates unique constraint/i.test(e.message) && has('message_id')) {
              for (const p of chunk) {
                p.message_id = null;
                nulledMessageIdTotal++;
              }

              const { placeholders, values } = buildInsertPlaceholdersDynamic(chunk, targetCols);
              const sql = makeInsertSQLDynamic(targetCols, placeholders);
              const { rows: ret } = await dest.query(sql, values);

              for (const r of ret) {
                insertedMap.set(String(r.id), Number(r.id)); // não usamos aqui; vamos mapear abaixo por fallback
              }

              // ⚠️ como o RETURNING não tem old_id, o mapeamento aqui depende do fallback sem message_id
              // para segurança, a gente não força mapear tudo nesse cenário; os quotes ainda vão resolver por tmp_msg_map (existentes + inseridos)
            } else {
              errorsTotal++;
              // tenta item a item pra não travar o processo inteiro
              for (const p of chunk) {
                try {
                  const { placeholders, values } = buildInsertPlaceholdersDynamic([p], targetCols);
                  const sql1 = makeInsertSQLDynamic(targetCols, placeholders);
                  const { rows: one } = await dest.query(sql1, values);
                  insertedMap.set(String(p.__old_id), Number(one[0].id));
                } catch {
                  skippedTotal++;
                  errorsTotal++;
                }
              }
            }
          }
        }
      }

      insertedTotal += insertedMap.size;

      // tmp_msg_map
      const pairs = [];
      for (const [oldId, newId] of existingOldToNew) pairs.push([oldId, newId]);
      for (const [oldId, newId] of insertedMap) pairs.push([oldId, newId]);

      if (pairs.length) {
        const { tuples, vals } = buildPairs(pairs);
        await dest.query(
          `INSERT INTO tmp_msg_map (old_id, new_id) VALUES ${tuples} ON CONFLICT (old_id) DO NOTHING`,
          vals,
        );
      }

      // tmp_quotes
      const quotes = [];
      for (const p of payloads) {
        const oldId = String(p.__old_id);
        const newId = insertedMap.get(oldId) ?? existingOldToNew.get(oldId);
        if (newId && p.__quoted_old != null && String(p.__quoted_old).trim() !== '') {
          quotes.push([newId, String(p.__quoted_old)]);
        }
      }
      if (quotes.length) {
        const { tuples, vals } = buildPairs(quotes);
        await dest.query(`INSERT INTO tmp_quotes (new_id, quoted_old) VALUES ${tuples}`, vals);
      }
      quotesTotal += quotes.length;

      processed += rows.length;
      tickBar(
        bar,
        processed,
        total,
        startedAt,
        PROGRESS_EVERY,
        `ins=${insertedTotal} exist=${existingTotal} skip=${skippedTotal} err=${errorsTotal} nulledMid=${nulledMessageIdTotal}`,
      );
    }

    // resolve quoted_msg_id
    if (has('quoted_msg_id')) {
      bar.update(processed, { status: 'Resolvendo quoted_msg_id...' });

      await dest.query(
        `
        UPDATE messages m
        SET quoted_msg_id = mm2.new_id
        FROM tmp_quotes tq
        JOIN tmp_msg_map mm2 ON mm2.old_id = tq.quoted_old
        WHERE m.id = tq.new_id
        `,
      );
    }

    await dest.query('DROP TABLE IF EXISTS tmp_quotes');
    await dest.query('DROP TABLE IF EXISTS tmp_msg_map');

    bar.stop();

    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log('');
    console.log('🧾 RESUMO:');
    console.log(`   • Total origem:           ${total}`);
    console.log(`   • Processadas (lidas):    ${processed}`);
    console.log(`   • Já existiam no destino: ${existingTotal}`);
    console.log(`   • Inseridas agora:        ${insertedTotal}`);
    console.log(`   • Puladas:                ${skippedTotal}`);
    console.log(`   • Erros:                  ${errorsTotal}`);
    console.log(`   • Quotes staging:         ${quotesTotal}`);
    console.log(`   • message_id anulados:    ${nulledMessageIdTotal} (por UNIQUE)`);
    console.log(`✅ Concluído em ${secs}s.`);
  } finally {
    try { if (cursor) await new Promise((resolve) => cursor.close(() => resolve())); } catch {}
    try { bar.stop(); } catch {}
    await source.end();
    await dest.end();
  }
};

/* ===================== helpers ===================== */

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

function get(row, key) {
  if (key in row) return row[key];
  const low = String(key).toLowerCase();
  for (const k of Object.keys(row)) if (k.toLowerCase() === low) return row[k];
  return null;
}

function toInt(v, def = null) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : def;
}

function normalizeMedia(mt) {
  const v = String(mt || 'text');
  return ['conversation', 'extendedTextMessage', 'chat'].includes(v) ? 'text' : v;
}

function parseJSON(v, fallback = {}) {
  if (v == null) return fallback;
  if (typeof v === 'string') {
    try { return JSON.parse(v); } catch { return fallback; }
  }
  return typeof v === 'object' ? v : fallback;
}

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

function ackToString(ackInt, statusStr, readBool) {
  const s = String(statusStr || '').trim().toLowerCase();
  if (s) {
    if (['sent', 'enviado'].includes(s)) return 'sent';
    if (['delivered', 'entregue'].includes(s)) return 'delivered';
    if (['read', 'lido', 'seen'].includes(s)) return 'read';
    if (['failed', 'erro', 'error'].includes(s)) return 'failed';
    return s;
  }
  const n = Number(ackInt);
  if (readBool === true) return 'read';
  if (!Number.isFinite(n)) return 'sent';
  if (n <= 0) return 'sent';
  if (n === 1) return 'delivered';
  if (n >= 2) return 'read';
  return 'sent';
}

function buildMessageStatus(ackStr, updatedAt) {
  const at = updatedAt ? new Date(updatedAt) : null;
  const ok = at && !isNaN(at.getTime());
  return { type: 'info', status: ackStr || 'sent', at: ok ? at.toISOString() : undefined };
}

function buildPayload(row) {
  const oldId = get(row, 'id');

  const ticketId = get(row, 'ticketId');

  const body = sanitizeUtf16(get(row, 'body') || '');
  const editedBody = sanitizeUtf16(get(row, 'edition') || get(row, 'edited') || '');

  const mediaType = sanitizeUtf16(normalizeMedia(get(row, 'mediaType')));
  const mediaName = sanitizeUtf16(get(row, 'mediaUrl') || '');

  const messageIdRaw = get(row, 'messageId');
  const messageId =
    messageIdRaw != null && String(messageIdRaw).trim() !== ''
      ? sanitizeUtf16(messageIdRaw)
      : null;

  const ackStr = ackToString(get(row, 'ack'), get(row, 'status'), get(row, 'read'));
  const msgStatus = buildMessageStatus(ackStr, get(row, 'updatedAt') || get(row, 'createdAt'));

  return {
    __old_id: String(oldId),
    __quoted_old: get(row, 'quotedMsgId'),

    ticket_id: ticketId,

    body,
    edited_body: editedBody,

    media_type: mediaType || 'text',
    media_name: mediaName,

    message_id: messageId,

    data_json: parseJSON(get(row, 'dataJson'), {}),

    ack: ackStr,
    message_status: msgStatus,

    is_deleted: !!get(row, 'isDeleted'),
    from_me: !!get(row, 'fromMe'),

    user_id: get(row, 'userId') || null,
    contact_id: get(row, 'contactId') || null,

    schedule_date: get(row, 'scheduleDate') || null,
    created_at: get(row, 'createdAt'),
    updated_at: get(row, 'updatedAt'),
  };
}

function buildInsertPlaceholdersDynamic(payloads, targetCols) {
  const placeholders = [];
  const values = [];

  for (let i = 0; i < payloads.length; i++) {
    const p = payloads[i];
    const base = i * targetCols.length;

    placeholders.push(`(${targetCols.map((_, j) => `$${base + j + 1}`).join(', ')})`);

    for (const col of targetCols) {
      switch (col) {
        case 'data_json':
          values.push(safeJsonb(p.data_json));
          break;
        case 'message_status':
          values.push(safeJsonb(p.message_status));
          break;
        default:
          values.push(p[col]);
      }
    }
  }

  return { placeholders, values };
}

function makeInsertSQLDynamic(targetCols, placeholders) {
  return `
    INSERT INTO messages (${targetCols.join(', ')})
    VALUES ${placeholders.join(',')}
    RETURNING id, ticket_id, message_id, from_me, created_at, COALESCE(body,'') AS body_norm
  `;
}

function keyWithMsg(ticketId, messageId) {
  return `mid:${ticketId}#${messageId}`;
}
function keyNoMsg(ticketId, fromMe, createdAt, bodyNorm) {
  const iso = toIsoSec(createdAt);
  return `fb:${ticketId}#${fromMe ? 1 : 0}#${iso}#${bodyNorm || ''}`;
}
function toIsoSec(d) {
  if (!d) return '';
  const dt = new Date(d);
  if (isNaN(dt)) return '';
  return dt.toISOString().replace(/\.\d{3}Z$/, 'Z');
}
function pushKey(map, key, val) {
  const arr = map.get(key);
  if (arr) arr.push(val);
  else map.set(key, [val]);
}

function buildPairs(pairs) {
  const tuples = [];
  const vals = [];
  let i = 0;
  for (const [a, b] of pairs) {
    tuples.push(`($${++i}, $${++i})`);
    vals.push(a, b);
  }
  return { tuples: tuples.join(','), vals };
}

function chunkify(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}
