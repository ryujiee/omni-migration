// migrations/fixTickets.fillMissing.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);

(async function run() {
  console.log('🛠  Corrigindo tickets já migrados (preenchendo campos faltantes)...');

  const tenantId =
    process.env.TENANT_ID && String(process.env.TENANT_ID).trim() !== ''
      ? String(process.env.TENANT_ID).trim()
      : null;

  const source = new Client({
    host: process.env.SRC_HOST,
    port: process.env.SRC_PORT,
    user: process.env.SRC_USER,
    password: process.env.SRC_PASS,
    database: process.env.SRC_DB,
    application_name: 'fixTickets:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'fixTickets:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // Carrega flows por empresa para validar flow_id
    const flowsSql = tenantId
      ? `SELECT id, company_id FROM flows WHERE company_id = $1`
      : `SELECT id, company_id FROM flows`;
    const flowsParams = tenantId ? [tenantId] : [];
    const flowsRes = await dest.query(flowsSql, flowsParams);

    const flowsByCompany = new Map(); // company_id -> Set(flow_ids)
    for (const f of flowsRes.rows) {
      const cid = String(f.company_id);
      if (!flowsByCompany.has(cid)) flowsByCompany.set(cid, new Set());
      flowsByCompany.get(cid).add(f.id);
    }

    // Cursor no legado
    const selectSql = `
      SELECT
        "id",
        "status",
        "lastMessage",
        "whatsappId"      AS channel_id,
        "contactId",
        "userId",
        "queueId"         AS department_id,
        COALESCE("chatFlowId", NULL) AS flow_id,
        "lastMessageAt",
        "closedAt",
        COALESCE("isGroup", false)   AS is_group,
        "participants",
        "silenced",
        "tenantId"        AS company_id,
        "createdAt",
        "updatedAt"
      FROM "public"."Tickets"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
      ORDER BY "id"
    `;
    const cursor = source.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    let processed = 0, updatedOK = 0, rowErrors = 0;

    while (true) {
      const rows = await readCursor(cursor, BATCH_SIZE);
      if (!rows.length) break;

      // monta VALUES (com casts por placeholder)
      const tuples = [];
      const vals = [];
      let p = 0;

      for (const r of rows) {
        const lastMessageAt = parseTimestamp(r.lastMessageAt);
        const closedAt = parseTimestamp(r.closedAt);
        const participants = JSON.stringify(normalizeJsonArray(r.participants));
        const silenced = JSON.stringify(normalizeJsonArray(r.silenced));

        // valida flow_id
        let flowId = r.flow_id || null;
        if (flowId != null) {
          const set = flowsByCompany.get(String(r.company_id));
          if (!set || !set.has(flowId)) flowId = null;
        }

        // Observação: valores 0 do legado viram NULL (mantendo a semântica antiga)
        const contactId = r.contactId || null;
        const userId = r.userId || null;
        const channelId = r.channel_id || null;
        const departmentId = r.department_id || null;

        tuples.push(
          `($${++p}::bigint,  $${++p}::text,    $${++p}::bigint, $${++p}::bigint, $${++p}::bigint, $${++p}::bigint, ` +
          ` $${++p}::bigint,  $${++p}::timestamptz, $${++p}::timestamptz, $${++p}::boolean, ` +
          ` $${++p}::jsonb,   $${++p}::jsonb,  $${++p}::timestamptz, $${++p}::bigint)`
        );
        vals.push(
          r.id, r.lastMessage || '',
          channelId, contactId, userId, departmentId,
          flowId, lastMessageAt, closedAt, !!r.is_group,
          participants, silenced, r.updatedAt, r.company_id
        );
      }

      const sql = `
        WITH v(id, last_message, channel_id, contact_id, user_id, department_id,
               flow_id, last_message_at, closed_at, is_group, participants, silenced, updated_at, company_id)
        AS (VALUES ${tuples.join(',')})
        UPDATE tickets t
        SET
          last_message    = v.last_message,
          channel_id      = ch.id,                               -- garante FK
          contact_id      = c.id,
          user_id         = u.id,
          department_id   = dpt.id,
          flow_id         = f_ok.id,                              -- somente se flow pertencer à company
          last_message_at = v.last_message_at,
          closed_at       = v.closed_at,
          is_group        = v.is_group,
          participants    = v.participants,
          silenced        = v.silenced,
          updated_at      = COALESCE(v.updated_at, t.updated_at)
        FROM v
        LEFT JOIN channel_instances ch ON ch.id = v.channel_id
        LEFT JOIN contacts          c  ON c.id  = v.contact_id
        LEFT JOIN users             u  ON u.id  = v.user_id
        LEFT JOIN departments       dpt ON dpt.id = v.department_id
        LEFT JOIN flows             f_ok ON f_ok.id = v.flow_id AND f_ok.company_id = v.company_id
        WHERE t.id = v.id
      `;

      try {
        const res = await dest.query(sql, vals);
        // res.rowCount aqui conta linhas afetadas (de v que encontraram t)
        updatedOK += res.rowCount || 0;
      } catch (e) {
        // fallback (raro): atualiza 1 a 1 para não perder o lote
        console.warn('⚠️ UPDATE em lote falhou; tentando linha a linha:', e.message);
        for (const r of rows) {
          try {
            const lastMessageAt = parseTimestamp(r.lastMessageAt);
            const closedAt = parseTimestamp(r.closedAt);
            const participants = JSON.stringify(normalizeJsonArray(r.participants));
            const silenced = JSON.stringify(normalizeJsonArray(r.silenced));

            let flowId = r.flow_id || null;
            if (flowId != null) {
              const set = flowsByCompany.get(String(r.company_id));
              if (!set || !set.has(flowId)) flowId = null;
            }

            const sql1 = `
              UPDATE tickets t
              SET
                last_message    = $2,
                channel_id      = ch.id,
                contact_id      = c.id,
                user_id         = u.id,
                department_id   = dpt.id,
                flow_id         = f_ok.id,
                last_message_at = $9,
                closed_at       = $10,
                is_group        = $11,
                participants    = $12::jsonb,
                silenced        = $13::jsonb,
                updated_at      = COALESCE($14, t.updated_at)
              FROM
                (SELECT $3::bigint AS channel_id) vch
                LEFT JOIN channel_instances ch ON ch.id = vch.channel_id
                ,(SELECT $4::bigint AS contact_id) vc
                LEFT JOIN contacts c ON c.id = vc.contact_id
                ,(SELECT $5::bigint AS user_id) vu
                LEFT JOIN users u ON u.id = vu.user_id
                ,(SELECT $6::bigint AS department_id) vd
                LEFT JOIN departments dpt ON dpt.id = vd.department_id
                ,(SELECT $7::bigint AS flow_id, $8::bigint AS company_id) vf
                LEFT JOIN flows f_ok ON f_ok.id = vf.flow_id AND f_ok.company_id = vf.company_id
              WHERE t.id = $1
            `;
            const params1 = [
              r.id,
              r.lastMessage || '',
              r.channel_id || null,
              r.contactId || null,
              r.userId || null,
              r.department_id || null,
              flowId,
              r.company_id,
              lastMessageAt,
              closedAt,
              !!r.is_group,
              participants,
              silenced,
              r.updatedAt
            ];
            const res1 = await dest.query(sql1, params1);
            updatedOK += res1.rowCount || 0;
          } catch (e1) {
            rowErrors++;
            console.error(`❌ Falha ao corrigir ticket id=${r.id}: ${e1.message}`);
          }
        }
      }

      processed += rows.length;
      console.log(`... batch OK. Processados=${processed}, Atualizados=${updatedOK}, Erros=${rowErrors}`);
    }

    console.log(`✅ Correção concluída. Processados=${processed}, Atualizados=${updatedOK}, Erros=${rowErrors}`);
  } finally {
    await source.end();
    await dest.end();
  }
})();

// ------------- helpers -------------
async function readCursor(cursor, size) {
  return await new Promise((resolve, reject) => {
    cursor.read(size, (err, rows) => (err ? reject(err) : resolve(rows || [])));
  });
}

function parseTimestamp(v) {
  if (v == null) return null;
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v;
  if (typeof v === 'number') {
    const ms = v > 1e12 ? v : v * 1000;
    const d = new Date(ms);
    return isNaN(d.getTime()) ? null : d;
  }
  const num = Number(v);
  if (Number.isFinite(num)) {
    const ms = num > 1e12 ? num : num * 1000;
    const d = new Date(ms);
    if (!isNaN(d.getTime())) return d;
  }
  const d = new Date(String(v));
  return isNaN(d.getTime()) ? null : d;
}

function normalizeJsonArray(v) {
  if (v == null) return [];
  if (Array.isArray(v)) return v;
  if (typeof v === 'string') {
    try {
      const parsed = JSON.parse(v);
      return Array.isArray(parsed) ? parsed : [];
    } catch { return []; }
  }
  if (typeof v === 'object') return Array.isArray(v) ? v : [];
  return [];
}
