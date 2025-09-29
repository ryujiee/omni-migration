// migrations/migrateTickets.fixed.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateTickets(ctx = {}) {
  console.log('🎫 Migrando "Tickets" → "tickets"...');

  const tenantId =
    ctx.tenantId != null && String(ctx.tenantId).trim() !== ''
      ? String(ctx.tenantId).trim()
      : (process.env.TENANT_ID ? String(process.env.TENANT_ID).trim() : null);

  const source = new Client({
    host: process.env.SRC_HOST,
    port: process.env.SRC_PORT,
    user: process.env.SRC_USER,
    password: process.env.SRC_PASS,
    database: process.env.SRC_DB
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB
  });

  await source.connect();
  await dest.connect();

  try {
    // Carrega flows por empresa no DESTINO para validar FK do flow_id
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

    const baseSelect = `
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
    `;
    const whereClause = tenantId ? `WHERE "tenantId" = $1` : '';
    const params = tenantId ? [tenantId] : [];
    const result = await source.query(`${baseSelect} ${whereClause}`, params);

    console.log('Total buscado:', result.rowCount);
    if (result.rowCount === 0) {
      console.log(
        tenantId
          ? `⚠️  Nenhum ticket encontrado para TENANT_ID=${tenantId}.`
          : '⚠️  Nenhum ticket encontrado na origem.'
      );
      return;
    }

    const upsertSql = `
      INSERT INTO tickets (
        id, status, last_message, channel_id, contact_id, user_id,
        department_id, flow_id,
        last_message_at, closed_at, is_group, participants, silenced,
        company_id, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6,
        $7, $8,
        $9, $10, $11, $12::jsonb, $13::jsonb,
        $14, $15, $16
      )
      ON CONFLICT (id) DO UPDATE SET
        status          = EXCLUDED.status,
        last_message    = EXCLUDED.last_message,
        channel_id      = EXCLUDED.channel_id,
        contact_id      = EXCLUDED.contact_id,
        user_id         = EXCLUDED.user_id,
        department_id   = EXCLUDED.department_id,
        flow_id         = EXCLUDED.flow_id,
        last_message_at = EXCLUDED.last_message_at,
        closed_at       = EXCLUDED.closed_at,
        is_group        = EXCLUDED.is_group,
        participants    = EXCLUDED.participants,
        silenced        = EXCLUDED.silenced,
        company_id      = EXCLUDED.company_id,
        updated_at      = EXCLUDED.updated_at
    `;

    let migrated = 0;

    for (const row of result.rows) {
      const lastMessageAt = parseTimestamp(row.lastMessageAt);
      const closedAt = parseTimestamp(row.closedAt);
      const participants = normalizeJsonArray(row.participants);
      const silenced = normalizeJsonArray(row.silenced);

      // valida flow_id (se não existir no destino, vira NULL)
      let flowId = row.flow_id || null;
      if (flowId != null) {
        const set = flowsByCompany.get(String(row.company_id));
        if (!set || !set.has(flowId)) flowId = null;
      }

      try {
        await dest.query(upsertSql, [
          row.id,                                   // $1
          row.status || 'pending',                  // $2
          row.lastMessage || '',                    // $3
          row.channel_id || null,                   // $4
          row.contactId || null,                    // $5
          row.userId || null,                       // $6
          row.department_id || null,                // $7
          flowId,                                   // $8
          lastMessageAt,                            // $9
          closedAt,                                 // $10
          !!row.is_group,                           // $11
          JSON.stringify(participants),             // $12
          JSON.stringify(silenced),                 // $13
          row.company_id,                           // $14
          row.createdAt,                            // $15
          row.updatedAt                             // $16
        ]);
        migrated++;
      } catch (err) {
        console.error(`❌ Erro ao migrar ticket ID ${row.id}: ${err.message}`);
        console.error('📦 Dados (resumo):', {
          id: row.id,
          company_id: row.company_id,
          contact_id: row.contactId,
          user_id: row.userId,
          channel_id: row.channel_id,
          department_id: row.department_id,
          flow_id: flowId
        });
      }
    }

    console.log(`✅ Total migrado: ${migrated} tickets.`);
  } finally {
    await source.end();
    await dest.end();
  }
};

// -------- helpers --------
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
