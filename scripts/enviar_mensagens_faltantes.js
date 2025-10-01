// scripts/enviar_mensagens_faltantes.js
'use strict';

/**
 * Resume migrando APENAS mensagens que faltam.
 * Paginação dinâmica:
 *  - Se existir "idFront" numérico → pagina por idFront (rápido e simples)
 *  - Senão, detecta o tipo de "id":
 *      * bigint/int → m."id" > $2::bigint
 *      * uuid       → (m."createdAt", m."id") > ($2::timestamptz, $3::uuid)
 *      * text/varchar → (m."createdAt", m."id") > ($2::timestamptz, $3::text)
 *
 * Por que tuple (createdAt,id)? Porque quando id é UUID/text não dá pra usar > bigint,
 * e usar apenas createdAt pode dar empates; o par (ts,id) é totalmente ordenável.
 *
 * ENV:
 *  SRC_HOST, SRC_PORT, SRC_USER, SRC_PASS, SRC_DB
 *  DST_HOST, DST_PORT, DST_USER, DST_PASS, DST_DB
 *  TENANT_ID=2
 *  BATCH_SIZE=1000
 *  INSERT_CHUNK=500
 *  LOG_EVERY=1
 *
 * CLI:
 *  --tenant=2
 *  --after-id=<valor>          (para idFront ou id numérico/uuid/text, conforme modo)
 *  --after-ts=2020-01-01T00:00:00Z  (somente nos modos tuple)
 */

require('dotenv').config();
const { Client } = require('pg');

const BATCH_SIZE   = parseInt(process.env.BATCH_SIZE   || '1000', 10);
const INSERT_CHUNK = parseInt(process.env.INSERT_CHUNK || String(Math.min(BATCH_SIZE, 500)), 10);
const LOG_EVERY    = parseInt(process.env.LOG_EVERY    || '1', 10);

function arg(name, def = null) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? hit.split('=').slice(1).join('=') : def;
}
function readTenantId() {
  const fromEnv = process.env.TENANT_ID && String(process.env.TENANT_ID).trim();
  const fromArg = arg('tenant');
  return (fromArg && String(fromArg).trim()) || fromEnv || null;
}
const AFTER_ID = (arg('after-id', process.env.AFTER_ID || '0') || '0');
const AFTER_TS = (arg('after-ts', process.env.AFTER_TS || '1970-01-01T00:00:00Z'));

const getSource = () => new Client({
  host: process.env.SRC_HOST, port: process.env.SRC_PORT,
  user: process.env.SRC_USER, password: process.env.SRC_PASS,
  database: process.env.SRC_DB, application_name: 'resume:src'
});
const getDest = () => new Client({
  host: process.env.DST_HOST, port: process.env.DST_PORT,
  user: process.env.DST_USER, password: process.env.DST_PASS,
  database: process.env.DST_DB, application_name: 'resume:dst'
});

/* ===== helpers ===== */
const get = (row, key) => (key in row ? row[key] : (row[String(key).toLowerCase()] ?? null));
const normalizeMedia = (mt) => {
  const v = String(mt || 'text');
  return ['conversation','extendedTextMessage','chat'].includes(v) ? 'text' : v;
};
const parseJSON = (v, fallback) => {
  if (v == null) return fallback;
  if (typeof v === 'string') { try { return JSON.parse(v); } catch { return fallback; } }
  return typeof v === 'object' ? v : fallback;
};
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
  try { return JSON.stringify(deepSanitize(value)); } catch { return fallback; }
}
function toIsoSec(d) {
  if (!d) return '';
  const dt = new Date(d);
  if (isNaN(dt)) return '';
  return dt.toISOString().replace(/\.\d{3}Z$/, 'Z');
}
function keyWithMsg(ticketId, messageId) {
  return `mid:${ticketId}#${messageId}`;
}
function keyNoMsg(ticketId, fromMe, createdAt, bodyNorm) {
  const iso = toIsoSec(createdAt);
  return `fb:${ticketId}#${fromMe ? 1 : 0}#${iso}#${bodyNorm || ''}`;
}
function chunkify(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function getColInfo(db, schema, table, column) {
  const sql = `
    SELECT data_type, udt_name
      FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
  `;
  const r = await db.query(sql, [schema, table, column]);
  return r.rows[0] || null;
}

async function main() {
  const tenantId = readTenantId();
  if (!tenantId) {
    console.error('❌ Informe TENANT_ID (env ou --tenant=).'); process.exit(1);
  }

  const source = getSource();
  const dest   = getDest();
  await source.connect();
  await dest.connect();

  // Detectar paginação
  const idFrontInfo = await getColInfo(source, 'public', 'Messages', 'idFront');
  const idInfo      = await getColInfo(source, 'public', 'Messages', 'id');
  let mode = 'id_bigint';
  if (idFrontInfo && ['int8','int4','numeric'].includes(idFrontInfo.udt_name)) mode = 'idfront';
  else if (idInfo && ['int8','int4','numeric'].includes(idInfo.udt_name)) mode = 'id_bigint';
  else if (idInfo && idInfo.udt_name === 'uuid') mode = 'tuple_uuid';
  else mode = 'tuple_text'; // varchar/text/etc.

  // contagem + max "id" (só para log)
  const countRes = await source.query(`
    SELECT COUNT(*)::bigint AS total,
           MIN(m."id")::text AS min_id,
           MAX(m."id")::text AS max_id
    FROM "public"."Messages" m
    JOIN "public"."Tickets"  t ON t."id" = m."ticketId"
    WHERE t."tenantId" = $1
  `, [tenantId]);
  const total = Number(countRes.rows[0]?.total || 0);
  const maxIdTxt = countRes.rows[0]?.max_id || 'n/a';
  console.log(`[resume] TENANT_ID=${tenantId} | total=${total} | maxId=${maxIdTxt} | mode=${mode}`);
  if (!total) { await source.end(); await dest.end(); return; }

  let lastId  = AFTER_ID; // string sempre
  let lastTs  = AFTER_TS; // usado nos modos tuple
  let processed = 0, inserted = 0, skipped = 0, errors = 0, batches = 0;

  while (true) {
    let fetchSql, params;
    if (mode === 'idfront') {
      fetchSql = `
        SELECT m.*
          FROM "public"."Messages" m
          JOIN "public"."Tickets" t ON t."id" = m."ticketId"
         WHERE t."tenantId" = $1 AND m."idFront" > $2::bigint
         ORDER BY m."idFront" ASC
         LIMIT $3
      `;
      params = [tenantId, lastId, BATCH_SIZE];
    } else if (mode === 'id_bigint') {
      fetchSql = `
        SELECT m.*
          FROM "public"."Messages" m
          JOIN "public"."Tickets" t ON t."id" = m."ticketId"
         WHERE t."tenantId" = $1 AND m."id" > $2::bigint
         ORDER BY m."id" ASC
         LIMIT $3
      `;
      params = [tenantId, lastId, BATCH_SIZE];
    } else if (mode === 'tuple_uuid') {
      fetchSql = `
        SELECT m.*
          FROM "public"."Messages" m
          JOIN "public"."Tickets" t ON t."id" = m."ticketId"
         WHERE t."tenantId" = $1
           AND (m."createdAt", m."id") > ($2::timestamptz, $3::uuid)
         ORDER BY m."createdAt" ASC, m."id" ASC
         LIMIT $4
      `;
      params = [tenantId, lastTs, lastId, BATCH_SIZE];
    } else { // tuple_text
      fetchSql = `
        SELECT m.*
          FROM "public"."Messages" m
          JOIN "public"."Tickets" t ON t."id" = m."ticketId"
         WHERE t."tenantId" = $1
           AND (m."createdAt", m."id") > ($2::timestamptz, $3::text)
         ORDER BY m."createdAt" ASC, m."id" ASC
         LIMIT $4
      `;
      params = [tenantId, lastTs, lastId, BATCH_SIZE];
    }

    const { rows } = await source.query(fetchSql, params);
    if (!rows.length) break;
    batches++;

    // montar payloads
    const payloads = rows.map(row => ({
      __old_id: String(get(row, 'id')),
      __quoted_old: get(row, 'quotedMsgId'),
      ticket_id: get(row, 'ticketId'),
      body: sanitizeUtf16(get(row, 'body') || null),
      edited_body: sanitizeUtf16(get(row, 'edited') || null),
      media_type: sanitizeUtf16(normalizeMedia(get(row, 'mediaType'))),
      media_name: sanitizeUtf16(get(row, 'mediaUrl') || ''),
      message_id: sanitizeUtf16(get(row, 'messageId') || null),
      data_json: parseJSON(get(row, 'dataJson'), {}),
      ack: sanitizeUtf16(String(get(row, 'status') || 'sent')),
      is_deleted: !!get(row, 'isDeleted'),
      from_me: !!get(row, 'fromMe'),
      user_id: get(row, 'userId') || null,
      contact_id: get(row, 'contactId') || null,
      schedule_date: get(row, 'scheduleDate') || null,
      created_at: get(row, 'createdAt'),
      updated_at: get(row, 'updatedAt')
    }));

    const withMsg = [], withMsgKeyToOld = new Map();
    const noMsg = [],  noMsgKeyToOld   = new Map();

    for (const p of payloads) {
      if (p.message_id) {
        withMsg.push([p.ticket_id, p.message_id]);
        pushKey(withMsgKeyToOld, keyWithMsg(p.ticket_id, p.message_id), p.__old_id);
      } else {
        noMsg.push([p.ticket_id, !!p.from_me, p.created_at, p.body || '']);
        pushKey(noMsgKeyToOld, keyNoMsg(p.ticket_id, p.from_me, p.created_at, p.body || ''), p.__old_id);
      }
    }

    // dedupe em lote
    const existingOldToNew = new Map();
    if (withMsg.length) {
      const placeholders = withMsg.map((_, i) => `($${i*2+1}, $${i*2+2})`).join(',');
      const vals = withMsg.flat();
      const ex = await dest.query(
        `SELECT id, ticket_id, message_id FROM messages WHERE (ticket_id, message_id) IN (${placeholders})`, vals
      );
      for (const m of ex.rows) {
        const olds = withMsgKeyToOld.get(keyWithMsg(m.ticket_id, m.message_id)) || [];
        for (const oldId of olds) existingOldToNew.set(oldId, m.id);
      }
    }
    if (noMsg.length) {
      const placeholders = noMsg.map((_, i) =>
        `($${i*4+1}::bigint,$${i*4+2}::boolean,$${i*4+3}::timestamptz,$${i*4+4}::text)`
      ).join(',');
      const vals = noMsg.flat();
      const ex = await dest.query(
        `WITH v(ticket_id, from_me, created_at, body_norm) AS (VALUES ${placeholders})
         SELECT m.id, m.ticket_id, m.from_me, m.created_at, COALESCE(m.body,'') AS body_norm
           FROM v JOIN messages m
             ON m.ticket_id=v.ticket_id AND m.from_me=v.from_me
            AND m.created_at=v.created_at AND COALESCE(m.body,'')=v.body_norm
            AND m.message_id IS NULL`,
        vals
      );
      for (const m of ex.rows) {
        const olds = noMsgKeyToOld.get(keyNoMsg(m.ticket_id, m.from_me, m.created_at, m.body_norm)) || [];
        for (const oldId of olds) existingOldToNew.set(oldId, m.id);
      }
    }

    const toInsert = payloads.filter(p => !existingOldToNew.has(p.__old_id));

    // inserir faltantes
    const insertedMap = new Map();
    if (toInsert.length) {
      const chunks = chunkify(toInsert, INSERT_CHUNK);
      for (const ch of chunks) {
        try {
          const { placeholders, values } = buildInsertPlaceholders(ch);
          const sql = makeInsertSQL(placeholders);
          await dest.query('BEGIN'); await dest.query('SET LOCAL synchronous_commit TO OFF');
          const ret = await dest.query(sql, values);
          await dest.query('COMMIT');

          const mapWith = new Map(), mapNo = new Map();
          for (const r of ret.rows) {
            if (r.message_id) mapWith.set(keyWithMsg(r.ticket_id, r.message_id), r.id);
            else mapNo.set(keyNoMsg(r.ticket_id, r.from_me, r.created_at, r.body_norm), r.id);
          }
          for (const p of ch) {
            const k1 = p.message_id ? keyWithMsg(p.ticket_id, p.message_id)
                                    : keyNoMsg(p.ticket_id, p.from_me, p.created_at, p.body || '');
            const newId = p.message_id ? mapWith.get(k1) : mapNo.get(k1);
            if (newId) insertedMap.set(p.__old_id, newId);
          }
          inserted += ret.rowCount || ch.length;
        } catch (e) {
          await dest.query('ROLLBACK');
          if (/unsupported Unicode escape sequence|invalid input syntax for type json/i.test(e.message)) {
            console.warn('⚠️  Lote com JSON/texto inválido; tentando item a item...');
            for (const p of ch) {
              try {
                const { placeholders, values } = buildInsertPlaceholders([p]);
                const sql1 = makeInsertSQL(placeholders);
                const ret1 = await dest.query(sql1, values);
                insertedMap.set(p.__old_id, ret1.rows[0].id);
                inserted += ret1.rowCount || 1;
              } catch (e1) {
                try {
                  const p2 = { ...p, data_json: {} };
                  const { placeholders, values } = buildInsertPlaceholders([p2]);
                  const sql2 = makeInsertSQL(placeholders);
                  const ret2 = await dest.query(sql2, values);
                  insertedMap.set(p.__old_id, ret2.rows[0].id);
                  inserted += ret2.rowCount || 1;
                  console.error(`⚠️  Inserido com data_json={} (oldId=${p.__old_id})`);
                } catch (e2) {
                  errors++; console.error(`❌ Falha ao inserir oldId=${p.__old_id}: ${e2.message}`);
                }
              }
            }
          } else {
            errors += ch.length; console.error('💥 Falha no INSERT em lote:', e.message);
          }
        }
      }
    }

    skipped += rows.length - toInsert.length;

    // quoted_msg_id
    const needQuotes = rows.filter(r => insertedMap.has(String(get(r, 'id'))) && get(r, 'quotedMsgId'));
    if (needQuotes.length) {
      const quotedOldIds = [...new Set(needQuotes.map(r => String(get(r, 'quotedMsgId'))))];
      const ph = quotedOldIds.map((_, i) => `$${i+1}`).join(',');
      const qRows = (await source.query(
        `SELECT id, "ticketId" AS ticket_id, COALESCE("messageId",'') AS message_id,
                "fromMe" AS from_me, "createdAt" AS created_at, COALESCE("body",'') AS body_norm
           FROM "public"."Messages" WHERE id IN (${ph})`, quotedOldIds
      )).rows;

      const withM = qRows.filter(r => r.message_id);
      const noM   = qRows.filter(r => !r.message_id);
      const qOld2New = new Map();

      if (withM.length) {
        const pairs = withM.map(r => [r.ticket_id, r.message_id]);
        const ph2 = pairs.map((_, i) => `($${i*2+1}, $${i*2+2})`).join(',');
        const vals = pairs.flat();
        const ex = await dest.query(
          `SELECT id, ticket_id, message_id FROM messages WHERE (ticket_id, message_id) IN (${ph2})`, vals
        );
        const idx = new Map(ex.rows.map(x => [keyWithMsg(x.ticket_id, x.message_id), x.id]));
        for (const r of withM) {
          const k = keyWithMsg(r.ticket_id, r.message_id);
          if (idx.has(k)) qOld2New.set(String(r.id), idx.get(k));
        }
      }
      if (noM.length) {
        const vals = noM.flatMap(r => [r.ticket_id, !!r.from_me, r.created_at, r.body_norm]);
        const ph3 = noM.map((_, i) => `($${i*4+1}::bigint,$${i*4+2}::boolean,$${i*4+3}::timestamptz,$${i*4+4}::text)`).join(',');
        const ex = await dest.query(
          `WITH v(ticket_id, from_me, created_at, body_norm) AS (VALUES ${ph3})
           SELECT m.id, m.ticket_id, m.from_me, m.created_at, COALESCE(m.body,'') AS body_norm
             FROM v JOIN messages m
               ON m.ticket_id=v.ticket_id AND m.from_me=v.from_me
              AND m.created_at=v.created_at AND COALESCE(m.body,'')=v.body_norm
              AND m.message_id IS NULL`,
          vals
        );
        const idx = new Map(ex.rows.map(x => [keyNoMsg(x.ticket_id, x.from_me, x.created_at, x.body_norm), x.id]));
        for (const r of noM) {
          const k = keyNoMsg(r.ticket_id, !!r.from_me, r.created_at, r.body_norm);
          if (idx.has(k)) qOld2New.set(String(r.id), idx.get(k));
        }
      }

      const upd = [];
      for (const row of needQuotes) {
        const newId = insertedMap.get(String(get(row, 'id')));
        const qOld  = String(get(row, 'quotedMsgId'));
        const qNew  = qOld2New.get(qOld);
        if (newId && qNew) upd.push([qNew, newId]);
      }
      if (upd.length) {
        const phU = upd.map((_, i) => `($${i*2+1}, $${i*2+2})`).join(',');
        const valsU = upd.flat();
        await dest.query(
          `UPDATE messages m SET quoted_msg_id = v.qid
             FROM (VALUES ${phU}) AS v(qid, id)
            WHERE m.id = v.id`, valsU
        );
      }
    }

    // avançar ponteiro
    const last = rows[rows.length - 1];
    if (mode === 'idfront')       lastId = String(get(last, 'idFront'));
    else if (mode === 'id_bigint') lastId = String(get(last, 'id'));
    else { lastTs = get(last, 'createdAt'); lastId = String(get(last, 'id')); }

    processed += rows.length;
    if (batches % LOG_EVERY === 0) {
      const extra = (mode === 'idfront' || mode === 'id_bigint') ? `lastId=${lastId}` : `lastTs=${lastTs} lastId=${lastId}`;
      console.log(`batch #${batches} → ${extra} • proc=${processed} • ins=${inserted} • skip=${skipped} • err=${errors}`);
    }
  }

  console.log('✅ Concluído.');
  console.log(`Resumo → processadas: ${processed} | inseridas: ${inserted} | já existiam: ${skipped} | erros: ${errors}`);

  await source.end(); await dest.end();
}

function pushKey(map, key, val) { const arr = map.get(key); if (arr) arr.push(val); else map.set(key, [val]); }

function buildInsertPlaceholders(payloads) {
  const placeholders = [], values = [];
  for (let i = 0; i < payloads.length; i++) {
    const p = payloads[i]; const base = i * 15;
    placeholders.push(
      `($${base+1},$${base+2},$${base+3},$${base+4},$${base+5},$${base+6},` +
      `$${base+7}::jsonb,$${base+8},$${base+9},$${base+10},$${base+11},$${base+12},` +
      `$${base+13},$${base+14},$${base+15})`
    );
    values.push(
      p.ticket_id, p.body, p.edited_body, p.media_type, p.media_name, p.message_id,
      safeJsonb(p.data_json), p.ack, p.is_deleted, p.from_me, p.user_id, p.contact_id,
      p.schedule_date, p.created_at, p.updated_at
    );
  }
  return { placeholders, values };
}

function makeInsertSQL(placeholders) {
  return `
    INSERT INTO messages (
      ticket_id, body, edited_body, media_type, media_name, message_id,
      data_json, ack, is_deleted, from_me, user_id, contact_id,
      schedule_date, created_at, updated_at
    ) VALUES
      ${placeholders.join(',')}
    RETURNING id, ticket_id, message_id, from_me, created_at, COALESCE(body,'') AS body_norm
  `;
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
