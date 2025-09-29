// scripts/resume-messages.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '5000', 10);

function readTenantId() {
  const fromEnv = process.env.TENANT_ID && String(process.env.TENANT_ID).trim();
  const fromArg = process.argv.find(a => a.startsWith('--tenant='))?.split('=')[1];
  return (fromArg && String(fromArg).trim()) || fromEnv || null;
}

const getSource = () => new Client({
  host: process.env.SRC_HOST, port: process.env.SRC_PORT,
  user: process.env.SRC_USER, password: process.env.SRC_PASS,
  database: process.env.SRC_DB
});
const getDest = () => new Client({
  host: process.env.DST_HOST, port: process.env.DST_PORT,
  user: process.env.DST_USER, password: process.env.DST_PASS,
  database: process.env.DST_DB
});

const get = (row, key) => row[key] ?? row[key?.toLowerCase?.()] ?? null;
const normalizeMedia = (mt) => {
  const v = String(mt || 'text');
  return ['conversation','extendedTextMessage','chat'].includes(v) ? 'text' : v;
};
const parseJSON = (v, fallback) => {
  if (v == null) return fallback;
  if (typeof v === 'string') { try { return JSON.parse(v); } catch { return fallback; } }
  return v;
};

async function main() {
  const tenantId = readTenantId();
  if (!tenantId) {
    console.error('❌ Informe o TENANT_ID (env TENANT_ID=... ou --tenant=...).');
    process.exit(1);
  }

  const source = getSource();
  const dest   = getDest();
  await source.connect();
  await dest.connect();

  // total de mensagens na ORIGEM para esse tenant
  const countSql = `
    SELECT COUNT(*)::bigint AS tot
    FROM "public"."Messages" m
    JOIN "public"."Tickets"  t ON t."id" = m."ticketId"
    WHERE t."tenantId" = $1
  `;
  const countRes = await source.query(countSql, [tenantId]);
  const total = Number(countRes.rows[0].tot || 0);
  console.log(`[resume] TENANT_ID=${tenantId} | total na origem: ${total}`);

  if (total === 0) {
    console.log('⚠️  Nenhuma mensagem para esse tenant na origem.');
    await source.end(); await dest.end(); return;
  }

  let offset = 0;
  let processed = 0, inserted = 0, skipped = 0, errors = 0;

  // mapeamento apenas do que inserirmos NESTA execução (para quoted_msg_id dentro do lote/execução)
  const newIdByOldId = new Map();

  while (offset < total) {
    const fetchSql = `
      SELECT m.*
      FROM "public"."Messages" m
      JOIN "public"."Tickets"  t ON t."id" = m."ticketId"
      WHERE t."tenantId" = $1
      ORDER BY m."id" ASC
      LIMIT $2 OFFSET $3
    `;
    const { rows } = await source.query(fetchSql, [tenantId, BATCH_SIZE, offset]);
    if (rows.length === 0) break;

    await dest.query('BEGIN');
    try {
      for (const row of rows) {
        processed++;

        const ticketId = get(row, 'ticketId');                 // usa o mesmo id do legado
        const mediaType = normalizeMedia(get(row, 'mediaType'));
        const dataJson  = parseJSON(get(row, 'dataJson'), {});
        const fromMe    = !!get(row, 'fromMe');

        const payload = {
          ticket_id: ticketId,
          body: get(row, 'body'),
          edited_body: get(row, 'edited'),
          media_type: mediaType,
          media_name: get(row, 'mediaUrl') || '',
          message_id: get(row, 'messageId') || '',
          data_json: JSON.stringify(dataJson),
          ack: String(get(row, 'status') || 'sent'),
          is_deleted: !!get(row, 'isDeleted'),
          from_me: fromMe,
          user_id: get(row, 'userId'),
          contact_id: get(row, 'contactId'),
          schedule_date: get(row, 'scheduleDate'),
          created_at: get(row, 'createdAt'),
          updated_at: get(row, 'updatedAt')
        };

        // idempotência (já existe no destino?)
        let existsRes;
        if (payload.message_id) {
          existsRes = await dest.query(
            `SELECT id FROM messages WHERE ticket_id = $1 AND message_id = $2 LIMIT 1`,
            [payload.ticket_id, payload.message_id]
          );
        } else {
          existsRes = await dest.query(
            `SELECT id FROM messages
             WHERE ticket_id = $1 AND from_me = $2 AND created_at = $3 AND COALESCE(body,'') = COALESCE($4,'')
             LIMIT 1`,
            [payload.ticket_id, payload.from_me, payload.created_at, payload.body || null]
          );
        }

        if (existsRes.rowCount > 0) {
          newIdByOldId.set(row.id, existsRes.rows[0].id);
          skipped++;
          continue;
        }

        try {
          const ins = await dest.query(
            `
            INSERT INTO messages (
              ticket_id, body, edited_body, media_type, media_name, message_id,
              data_json, ack, is_deleted, from_me, user_id, contact_id,
              schedule_date, created_at, updated_at
            ) VALUES (
              $1, $2, $3, $4, $5, $6,
              $7::jsonb, $8, $9, $10, $11, $12,
              $13, $14, $15
            )
            RETURNING id
            `,
            [
              payload.ticket_id,
              payload.body,
              payload.edited_body,
              payload.media_type,
              payload.media_name,
              payload.message_id,
              payload.data_json,
              payload.ack,
              payload.is_deleted,
              payload.from_me,
              payload.user_id,
              payload.contact_id,
              payload.schedule_date,
              payload.created_at,
              payload.updated_at
            ]
          );
          newIdByOldId.set(row.id, ins.rows[0].id);
          inserted++;
        } catch (e) {
          errors++;
          console.error(`❌ Falha ao inserir msg oldId=${row.id} ticket=${ticketId}: ${e.message}`);
        }
      }

      // quoted_msg_id (apenas quando a citada também entrou nesta execução)
      for (const row of rows) {
        const newId = newIdByOldId.get(row.id);
        if (!newId) continue;
        const quotedOld = get(row, 'quotedMsgId');
        if (!quotedOld) continue;

        const quotedNew = newIdByOldId.get(quotedOld);
        if (!quotedNew) continue;

        try {
          await dest.query(
            `UPDATE messages SET quoted_msg_id = $1 WHERE id = $2`,
            [quotedNew, newId]
          );
        } catch (e) {
          console.error(`⚠️ Falha ao setar quoted_msg_id (msg ${newId} ← quoted ${quotedNew}): ${e.message}`);
        }
      }

      await dest.query('COMMIT');
    } catch (e) {
      await dest.query('ROLLBACK');
      console.error('💥 Erro no lote, transação revertida:', e.message);
    }

    offset += rows.length;

    // progresso
    console.log(`... progresso: ${processed}/${total} | inseridas=${inserted} | já existiam=${skipped} | erros=${errors}`);
  }

  console.log('✅ Concluído.');
  console.log(`Resumo → processadas: ${processed} | inseridas: ${inserted} | já existiam: ${skipped} | erros: ${errors}`);

  await source.end();
  await dest.end();
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
