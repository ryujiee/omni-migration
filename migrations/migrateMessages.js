// migrations/migrateMessages.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

/**
 * ENV (opcionais):
 *   BATCH_SIZE=500
 *   LOG_EVERY=1
 *   INSERT_CHUNK=500           # tamanho máximo por INSERT multi-row (padrão = BATCH_SIZE)
 */
const BATCH_SIZE  = Number(process.env.BATCH_SIZE  || 500);
const LOG_EVERY   = Number(process.env.LOG_EVERY   || 1);
const INSERT_CHUNK = Number(process.env.INSERT_CHUNK || BATCH_SIZE);

module.exports = async function migrateMessages(ctx = {}) {
  console.log('💬 Migrando "Messages" → "messages"...');

  const tenantId =
    ctx.tenantId != null && String(ctx.tenantId).trim() !== ''
      ? String(ctx.tenantId).trim()
      : (process.env.TENANT_ID ? String(process.env.TENANT_ID).trim() : null);

  const source = new Client({
    host: process.env.SRC_HOST,  port: process.env.SRC_PORT,
    user: process.env.SRC_USER,  password: process.env.SRC_PASS,
    database: process.env.SRC_DB, application_name: 'migrateMessages:source'
  });
  const dest = new Client({
    host: process.env.DST_HOST,  port: process.env.DST_PORT,
    user: process.env.DST_USER,  password: process.env.DST_PASS,
    database: process.env.DST_DB, application_name: 'migrateMessages:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // —— Count p/ ETA
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
      console.log(tenantId
        ? `⚠️  Nenhuma mensagem para TENANT_ID=${tenantId}.`
        : '⚠️  Nenhuma mensagem na origem.');
      return;
    }
    console.log(`📦 Total na origem${tenantId ? ` (tenant ${tenantId})` : ''}: ${total}`);

    // —— Cursor server-side
    const selectSql = tenantId
      ? `
        SELECT m.*
        FROM "public"."Messages" m
        JOIN "public"."Tickets" t ON t."id" = m."ticketId"
        WHERE t."tenantId" = $1
        ORDER BY m."id"
      `
      : `SELECT * FROM "public"."Messages" ORDER BY "id"`;
    const cursor = source.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    // —— Staging temporário (sem ON COMMIT DROP)
    await dest.query(`CREATE TEMP TABLE tmp_msg_map (old_id text PRIMARY KEY, new_id bigint NOT NULL)`);
    await dest.query(`CREATE TEMP TABLE tmp_quotes  (new_id bigint NOT NULL, quoted_old text NOT NULL)`);

    // —— Barra de progresso
    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    const startedAt = Date.now();
    let processed = 0;            // lidos da origem
    let migratedMapCount = 0;     // mapeados (existentes + inseridos) para tmp_msg_map
    let reactionsTotal = 0;       // reactions inseridas
    let quotesTotal = 0;          // linhas em tmp_quotes
    let batchNo = 0;

    // —— contadores detalhados
    let existingTotal = 0;        // já existiam no destino
    let toInsertTotal = 0;        // candidatos a insert (faltavam)
    let insertedTotal = 0;        // efetivamente inseridos
    let skippedTotal = 0;         // pulados por erro de JSON/texto
    let failedEvenEmptyJSON = 0;  // falharam até com data_json {}

    while (true) {
      const rows = await readCursor(cursor, BATCH_SIZE);
      if (!rows || rows.length === 0) break;
      batchNo++;

      // 1) Monta payloads + chaves p/ dedupe
      const payloads = rows.map(buildPayload);

      const withMsg = [];                // [[ticket_id, message_id]]
      const withMsgKeyToOld = new Map(); // "mid:ticket#msgid" -> [oldId...]
      const noMsg = [];                  // [[ticket_id, from_me, created_at, body_norm]]
      const noMsgKeyToOld = new Map();   // "fb:ticket#from#created#body" -> [oldId...]

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

      // 2) Busca existentes em lote
      const existingOldToNew = new Map();

      // 2a) com message_id
      if (withMsg.length) {
        const placeholders = withMsg.map((_, i) => `($${i * 2 + 1}, $${i * 2 + 2})`).join(',');
        const vals = withMsg.flat();
        const { rows: ex } = await dest.query(
          `SELECT id, ticket_id, message_id FROM messages WHERE (ticket_id, message_id) IN (${placeholders})`,
          vals
        );
        for (const m of ex) {
          const key = keyWithMsg(m.ticket_id, m.message_id);
          const olds = withMsgKeyToOld.get(key) || [];
          for (const oldId of olds) existingOldToNew.set(oldId, m.id);
        }
      }

      // 2b) sem message_id (fallback)
      if (noMsg.length) {
        const placeholders = noMsg.map((_, i) =>
          `($${i * 4 + 1}::bigint, $${i * 4 + 2}::boolean, $${i * 4 + 3}::timestamptz, $${i * 4 + 4}::text)`
        ).join(',');
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
           AND m.message_id IS NULL
          `,
          vals
        );
        for (const m of ex) {
          const key = keyNoMsg(m.ticket_id, m.from_me, m.created_at, m.body_norm);
          const olds = noMsgKeyToOld.get(key) || [];
          for (const oldId of olds) existingOldToNew.set(oldId, m.id);
        }
      }

      // 3) Separa os que faltam inserir
      const toInsert = payloads.filter(p => !existingOldToNew.has(String(p.__old_id)));
      const alreadyExist = existingOldToNew.size;
      const toInsertCount = toInsert.length;

      // 4) INSERT multi-row (RETURNING p/ mapear) — com fallback item-a-item em caso de JSON ruim
      let insertedMap = new Map(); // old -> new
      let skippedThisBatch = 0;
      let failedEvenEmptyThisBatch = 0;

      if (toInsert.length) {
        // fatiar o batch de inserts em chunks
        const chunks = chunkify(toInsert, INSERT_CHUNK);
        for (const chunk of chunks) {
          try {
            const { placeholders, values } = buildInsertPlaceholders(chunk);
            const sql = makeInsertSQL(placeholders);
            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');
            const { rows: ret } = await dest.query(sql, values);
            await dest.query('COMMIT');

            // monta índice por chave p/ achar oldIds
            const mapWith = new Map(); // keyWith -> newId
            const mapNo = new Map();   // keyNo   -> newId
            for (const r of ret) {
              if (r.message_id) {
                mapWith.set(keyWithMsg(r.ticket_id, r.message_id), r.id);
              } else {
                mapNo.set(keyNoMsg(r.ticket_id, r.from_me, r.created_at, r.body_norm), r.id);
              }
            }
            for (const p of chunk) {
              if (p.message_id) {
                const newId = mapWith.get(keyWithMsg(p.ticket_id, p.message_id));
                if (newId) insertedMap.set(String(p.__old_id), newId);
              } else {
                const newId = mapNo.get(keyNoMsg(p.ticket_id, p.from_me, p.created_at, p.body || ''));
                if (newId) insertedMap.set(String(p.__old_id), newId);
              }
            }
          } catch (e) {
            await dest.query('ROLLBACK');
            if (/unsupported Unicode escape sequence|invalid input syntax for type json/i.test(e.message)) {
              console.warn('⚠️  Lote com JSON/texto inválido; tentando item a item para isolar…');
              for (const p of chunk) {
                try {
                  const { placeholders, values } = buildInsertPlaceholders([p]);
                  const sql1 = makeInsertSQL(placeholders);
                  const { rows: one } = await dest.query(sql1, values);
                  const r = one[0];
                  const newId = r.id;
                  insertedMap.set(String(p.__old_id), newId);
                } catch (e1) {
                  console.error(`❌ Pulando old_id=${p.__old_id} por JSON/texto inválido:`, e1.message);
                  skippedThisBatch++;
                  // último recurso: tenta com data_json vazio
                  try {
                    const p2 = { ...p, data_json: {} };
                    const { placeholders, values } = buildInsertPlaceholders([p2]);
                    const sql2 = makeInsertSQL(placeholders);
                    const { rows: one2 } = await dest.query(sql2, values);
                    insertedMap.set(String(p.__old_id), one2[0].id);
                    skippedThisBatch--; // conseguiu salvar com data_json {}
                  } catch (e2) {
                    console.error(`❌ Falhou até com data_json vazio old_id=${p.__old_id}:`, e2.message);
                    failedEvenEmptyThisBatch++;
                  }
                }
              }
            } else {
              throw e;
            }
          }
        }
      }

      const insertedCount = insertedMap.size;
      const skippedCount = Math.max(0, toInsertCount - insertedCount); // estimado (inclui os realmente pulados)
      // refina com contadores de erro:
      const trulySkipped = skippedThisBatch + failedEvenEmptyThisBatch;

      // 5) Grava mapeamentos do lote
      const pairs = [];
      for (const [oldId, newId] of existingOldToNew) pairs.push([oldId, newId]);
      for (const [oldId, newId] of insertedMap)      pairs.push([oldId, newId]);

      if (pairs.length) {
        const { tuples, vals } = buildPairs(pairs);
        await dest.query(
          `INSERT INTO tmp_msg_map (old_id, new_id) VALUES ${tuples} ON CONFLICT (old_id) DO NOTHING`,
          vals
        );
      }

      // 6) Staging de quotes do lote
      const quotes = [];
      for (const p of payloads) {
        const newId =
          insertedMap.get(String(p.__old_id)) ??
          existingOldToNew.get(String(p.__old_id));
        if (newId && p.__quoted_old != null) {
          quotes.push([newId, String(p.__quoted_old)]);
        }
      }
      if (quotes.length) {
        const { tuples, vals } = buildPairs(quotes);
        await dest.query(`INSERT INTO tmp_quotes (new_id, quoted_old) VALUES ${tuples}`, vals);
      }
      quotesTotal += quotes.length;

      // 7) Reações do lote
      const reactionRows = [];
      for (const row of rows) {
        const oldId = String(get(row, 'id'));
        const newId = insertedMap.get(oldId) ?? existingOldToNew.get(oldId);
        if (!newId) continue;
        const rawReaction = get(row, 'reaction');
        const createdAt = get(row, 'updatedAt') || get(row, 'createdAt');
        const parsed = parseReactions(rawReaction, row, createdAt);
        for (const r of parsed) {
          if (!r.emoji) continue;
          reactionRows.push([newId, r.user_id, r.contact_id, sanitizeUtf16(r.emoji), r.created_at]);
        }
      }
      let reactionsInsertedThisBatch = 0;
      if (reactionRows.length) {
        const chunk = 5000;
        for (let i = 0; i < reactionRows.length; i += chunk) {
          const slice = reactionRows.slice(i, i + chunk);
          const ph = slice.map((_, j) =>
            `($${j * 5 + 1}, $${j * 5 + 2}, $${j * 5 + 3}, $${j * 5 + 4}, $${j * 5 + 5})`
          ).join(',');
          const vals = slice.flat();
          await dest.query(
            `INSERT INTO message_reactions (message_id, user_id, contact_id, emoji, created_at)
             VALUES ${ph}
             ON CONFLICT DO NOTHING`,
            vals
          );
          reactionsInsertedThisBatch += slice.length;
        }
        reactionsTotal += reactionsInsertedThisBatch;
      }

      migratedMapCount += pairs.length;
      processed += rows.length;

      existingTotal  += alreadyExist;
      toInsertTotal  += toInsertCount;
      insertedTotal  += insertedCount;
      skippedTotal   += trulySkipped;

      if (batchNo % LOG_EVERY === 0) {
        const elapsed = (Date.now() - startedAt) / 1000;
        const rate = (processed / Math.max(1, elapsed)).toFixed(1);
        bar.update(processed, { rate });

        const remainingEst = Math.max(0, total - processed);

        console.log(
          [
            '',
            `📊 Lote #${batchNo} | lidos: ${rows.length} | janela proc: ${processed}/${total} | ~restantes: ${remainingEst}`,
            `   • Já existiam no destino: ${alreadyExist}`,
            `   • Faltavam inserir:       ${toInsertCount}`,
            `   • Inseridos agora:        ${insertedCount}`,
            `   • Pulados por erro:       ${trulySkipped}${failedEvenEmptyThisBatch ? ` (falharam até com data_json {}: ${failedEvenEmptyThisBatch})` : ''}`,
            `   • Quotes enfileiradas:    ${quotes.length}`,
            `   • Reactions inseridas:    ${reactionsInsertedThisBatch}`,
            `   • Taxa média:             ${rate} rows/s`,
          ].join('\n')
        );
      }
    }

    // 8) Atualiza quoted_msg_id em massa
    console.log('🔗 Resolvendo quoted_msg_id a partir dos mapeamentos…');
    await dest.query(
      `
      UPDATE messages m
      SET quoted_msg_id = mm2.new_id
      FROM tmp_quotes tq
      JOIN tmp_msg_map mm2 ON mm2.old_id = tq.quoted_old
      WHERE m.id = tq.new_id
      `
    );

    // Limpeza (seriam dropadas ao fechar a conexão)
    await dest.query('DROP TABLE IF EXISTS tmp_quotes');
    await dest.query('DROP TABLE IF EXISTS tmp_msg_map');

    bar.stop();
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);

    const remainingEstFinal = Math.max(0, total - processed);
    console.log('');
    console.log('🧾 RESUMO:');
    console.log(`   • Total origem:               ${total}`);
    console.log(`   • Lidos da origem:            ${processed}`);
    console.log(`   • Já existiam no destino:     ${existingTotal}`);
    console.log(`   • Candidatos a insert:        ${toInsertTotal}`);
    console.log(`   • Inseridos com sucesso:      ${insertedTotal}`);
    console.log(`   • Pulados por erro:           ${skippedTotal}`);
    console.log(`   • Quotes enfileiradas total:  ${quotesTotal}`);
    console.log(`   • Reactions inseridas total:  ${reactionsTotal}`);
    console.log(`   • Estimativa restantes:       ${remainingEstFinal}`);
    console.log(`✅ Mensagens mapeadas (inseridas + existentes): ${migratedMapCount}/${total} em ${secs}s.`);
  } finally {
    await source.end();
    await dest.end();
  }
};

/* ===================== helpers ===================== */

async function readCursor(cursor, size) {
  return await new Promise((resolve, reject) => {
    cursor.read(size, (err, rows) => (err ? reject(err) : resolve(rows || [])));
  });
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

function parseJSON(v, fallback = {}) {
  if (v == null) return fallback;
  if (typeof v === 'string') {
    try { return JSON.parse(v); } catch { return fallback; }
  }
  return typeof v === 'object' ? v : fallback;
}

// Remove \u0000 e corrige surrogates desemparelhados (vira U+FFFD)
function sanitizeUtf16(str) {
  if (str == null) return str;
  let s = String(str).replace(/\u0000/g, ''); // Postgres não aceita NUL
  // high surrogate sem low:
  s = s.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])/g, '\uFFFD');
  // low surrogate sem high:
  s = s.replace(/(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, '\uFFFD');
  return s;
}

// Sanitiza profundamente objetos/arrays para JSONB
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

// Gera JSON “seguro” para jsonb
function safeJsonb(value, fallback = '{}') {
  try {
    const sani = deepSanitize(value);
    return JSON.stringify(sani);
  } catch {
    return fallback;
  }
}

function buildPayload(row) {
  const oldId = get(row, 'id');
  const ack = normalizeAck(get(row, 'status'));
  return {
    __old_id: String(oldId),
    __quoted_old: get(row, 'quotedMsgId'),

    ticket_id: get(row, 'ticketId'),
    body: sanitizeUtf16(get(row, 'body') || null),
    edited_body: sanitizeUtf16(get(row, 'edited') || null),
    media_type: sanitizeUtf16(normalizeMedia(get(row, 'mediaType'))),
    media_name: sanitizeUtf16(get(row, 'mediaUrl') || ''),
    message_id: sanitizeUtf16(get(row, 'messageId') || null),  // null se vazio
    data_json: parseJSON(get(row, 'dataJson'), {}),
    ack: sanitizeUtf16(ack),
    message_status: buildMessageStatus(ack, get(row, 'updatedAt') || get(row, 'createdAt')),
    is_deleted: !!get(row, 'isDeleted'),
    from_me: !!get(row, 'fromMe'),
    user_id: get(row, 'userId') || null,
    contact_id: get(row, 'contactId') || null,
    schedule_date: get(row, 'scheduleDate') || null,
    created_at: get(row, 'createdAt'),
    updated_at: get(row, 'updatedAt')
  };
}

function buildInsertPlaceholders(payloads) {
  const placeholders = [];
  const values = [];
  for (let i = 0; i < payloads.length; i++) {
    const p = payloads[i];
    const base = i * 16;
    placeholders.push(
      `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, ` +
      `$${base + 7}::jsonb, $${base + 8}, $${base + 9}::jsonb, $${base + 10}, $${base + 11}, $${base + 12}, $${base + 13}, ` +
      `$${base + 14}, $${base + 15}, $${base + 16})`
    );
    values.push(
      p.ticket_id, p.body, p.edited_body, p.media_type, p.media_name, p.message_id,
      safeJsonb(p.data_json),              // JSON seguro
      p.ack, safeJsonb(p.message_status, 'null'), p.is_deleted, p.from_me, p.user_id, p.contact_id,
      p.schedule_date, p.created_at, p.updated_at
    );
  }
  return { placeholders, values };
}

function makeInsertSQL(placeholders) {
  return `
    INSERT INTO messages (
      ticket_id, body, edited_body, media_type, media_name, message_id,
      data_json, ack, message_status, is_deleted, from_me, user_id, contact_id,
      schedule_date, created_at, updated_at
    ) VALUES
      ${placeholders.join(',')}
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
  if (arr) arr.push(val); else map.set(key, [val]);
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

// fatiador simples
function chunkify(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/**
 * Parser de reações do legado.
 * Suporta:
 * - array de objetos: [{emoji:'👍', userId:1, contactId:2, createdAt:'...'}, ...]
 * - array de strings: ['👍','❤️']
 * - objeto { userId: '👍', ... } ou {emoji:'👍', ...}
 * - string única '👍'
 */
function parseReactions(raw, row, fallbackDate) {
  const out = [];
  if (!raw) return out;

  const add = (emoji, userId = null, contactId = null, createdAt = fallbackDate) => {
    const e = sanitizeUtf16((emoji || '').toString().trim());
    if (!e) return;
    out.push({ emoji: e, user_id: userId ?? null, contact_id: contactId ?? null, created_at: createdAt || fallbackDate });
  };

  const v = typeof raw === 'string' ? safeParseJSON(raw, raw) : raw;

  if (Array.isArray(v)) {
    for (const it of v) {
      if (typeof it === 'string') add(it);
      else if (it && typeof it === 'object') {
        add(it.emoji || it.emotion || it.reaction, it.userId ?? null, it.contactId ?? null, it.createdAt || fallbackDate);
      }
    }
    return out;
  }

  if (v && typeof v === 'object') {
    if ('emoji' in v || 'reaction' in v || 'emotion' in v) {
      add(v.emoji || v.reaction || v.emotion, v.userId ?? null, v.contactId ?? null, v.createdAt || fallbackDate);
      return out;
    }
    for (const [k, val] of Object.entries(v)) {
      if (typeof val === 'string') add(val, k, null);
      else if (val && typeof val === 'object') add(val.emoji || val.reaction || val.emotion, val.userId ?? k ?? null, val.contactId ?? null, val.createdAt || fallbackDate);
    }
    return out;
  }

  if (typeof v === 'string') add(v);
  return out;
}
function safeParseJSON(s, fallback) {
  try { return JSON.parse(s); } catch { return fallback; }
}
function normalizeAck(v) {
  if (v == null) return 'sent';
  const raw = String(v).trim().toLowerCase();
  const n = Number(raw);
  if (Number.isFinite(n)) {
    if (n < 0) return 'failed';
    if (n <= 1) return 'sent';
    if (n === 2) return 'delivered';
    return 'read';
  }
  if (['failed', 'error', 'erro'].includes(raw)) return 'failed';
  if (['delivered', 'received'].includes(raw)) return 'delivered';
  if (['read', 'seen', 'lida'].includes(raw)) return 'read';
  return 'sent';
}
function buildMessageStatus(ack, at) {
  if (!ack) return null;
  const payload = {
    type: ack === 'failed' ? 'error' : 'info',
    status: ack
  };
  if (at) payload.at = at;
  return payload;
}
