"use strict";

require("dotenv").config();
const { Client } = require("pg");
const cliProgress = require("cli-progress");
const fs = require("fs");
const path = require("path");

module.exports = async function migrateTickets(ctx = {}) {
  console.log('🎫 Migrando "Tickets" → "tickets"... (diagnóstico de skipped)');

  const PROGRESS_EVERY = Number(process.env.TICKETS_PROGRESS_EVERY || 200);
  const SKIP_PRINT_LIMIT = Number(process.env.TICKETS_SKIP_PRINT_LIMIT || 30); // quantos IDs mostrar no terminal
  const SKIP_CSV_PATH = process.env.TICKETS_SKIP_CSV_PATH || path.resolve(process.cwd(), "skipped_tickets.csv");

  const tenantId =
    ctx.tenantId != null && String(ctx.tenantId).trim() !== ""
      ? String(ctx.tenantId).trim()
      : process.env.TENANT_ID
        ? String(process.env.TENANT_ID).trim()
        : null;

  const source = new Client({
    host: process.env.SRC_HOST,
    port: process.env.SRC_PORT,
    user: process.env.SRC_USER,
    password: process.env.SRC_PASS,
    database: process.env.SRC_DB,
    application_name: "migrateTickets:source",
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: "migrateTickets:dest",
  });

  await source.connect();
  await dest.connect();

  const bar = new cliProgress.SingleBar(
    {
      format:
        "Tickets |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s | {status}",
      hideCursor: true,
      clearOnComplete: false,
    },
    cliProgress.Presets.shades_classic,
  );

  // CSV writer (append)
  ensureCsvHeader(SKIP_CSV_PATH);

  try {
    const destCols = await loadDestColumns(dest, "tickets");
    const has = (c) => destCols.has(String(c).toLowerCase());

    // Pré-carregar FKs válidas do DEST
    const whereCompany = tenantId ? "WHERE company_id=$1" : "";
    const pCompany = tenantId ? [tenantId] : [];

    const [channelsRes, contactsRes, usersRes, deptsRes, flowsRes] = await Promise.all([
      dest.query(`SELECT id FROM channel_instances ${whereCompany}`, pCompany),
      dest.query(`SELECT id FROM contacts ${whereCompany}`, pCompany),
      dest.query(`SELECT id FROM users ${whereCompany}`, pCompany).catch(() => ({ rows: [] })),
      dest.query(`SELECT id FROM departments ${whereCompany}`, pCompany).catch(() => ({ rows: [] })),
      dest.query(`SELECT id FROM flows ${whereCompany}`, pCompany).catch(() => ({ rows: [] })),
    ]);

    const validChannels = new Set(channelsRes.rows.map((r) => Number(r.id)));
    const validContacts = new Set(contactsRes.rows.map((r) => Number(r.id)));
    const validUsers = new Set(usersRes.rows.map((r) => Number(r.id)));
    const validDepts = new Set(deptsRes.rows.map((r) => Number(r.id)));
    const validFlows = new Set(flowsRes.rows.map((r) => Number(r.id)));

    const baseSelect = `
      SELECT
        id,
        status,
        "lastMessage" AS last_message,
        "whatsappId" AS channel_id,
        "contactId" AS contact_id,
        "userId" AS user_id,
        "queueId" AS department_id,
        "chatFlowId" AS flow_id,
        "stepChatFlow" AS flow_step_raw,
        "botRetries" AS bot_retries,
        "lastMessageAt",
        "lastMessageReceived",
        "startedAttendanceAt",
        "closedAt",
        COALESCE("isGroup", false) AS is_group,
        "unreadMessages" AS unread_messages,
        "tenantId" AS company_id,
        "createdAt" AS created_at,
        "updatedAt" AS updated_at
      FROM "public"."Tickets"
    `;
    const whereClause = tenantId ? `WHERE "tenantId" = $1` : "";
    const params = tenantId ? [tenantId] : [];

    const result = await source.query(`${baseSelect} ${whereClause} ORDER BY id`, params);

    const total = result.rowCount || 0;
    console.log("Total buscado:", total);
    if (!total) return;

    bar.start(total, 0, { rate: "0.0", status: "Preparando..." });

    const targetCols = [
      "id",
      has("status") ? "status" : null,
      has("last_message") ? "last_message" : null,
      has("channel_id") ? "channel_id" : null,
      has("contact_id") ? "contact_id" : null,
      has("company_id") ? "company_id" : null,

      has("user_id") ? "user_id" : null,
      has("department_id") ? "department_id" : null,
      has("flow_id") ? "flow_id" : null,

      has("flow_step_id") ? "flow_step_id" : null,
      has("flow_attempts") ? "flow_attempts" : null,

      has("last_message_at") ? "last_message_at" : null,
      has("closed_at") ? "closed_at" : null,
      has("last_interaction_at") ? "last_interaction_at" : null,

      has("is_group") ? "is_group" : null,
      has("tags") ? "tags" : null,

      has("chat_id") ? "chat_id" : null,
      has("is_rating") ? "is_rating" : null,

      has("created_at") ? "created_at" : null,
      has("updated_at") ? "updated_at" : null,
    ].filter(Boolean);

    const insertColsSql = targetCols.join(", ");
    const placeholders = targetCols.map((_, i) => `$${i + 1}`).join(", ");
    const updateCols = targetCols
      .filter((c) => c !== "id" && c !== "created_at")
      .map((c) => `${c}=EXCLUDED.${c}`)
      .join(", ");

    const upsertSql = `
      INSERT INTO tickets (${insertColsSql})
      VALUES (${placeholders})
      ON CONFLICT (id) DO UPDATE SET
        ${updateCols}
    `;

    let migrated = 0;
    let errors = 0;

    // diagnóstico
    const skippedStats = {
      missing_required: 0,
      missing_channel_fk: 0,
      missing_contact_fk: 0,
      missing_both_fk: 0,
    };
    const printed = { count: 0 };

    const startedAt = Date.now();

    for (let idx = 0; idx < result.rows.length; idx++) {
      const row = result.rows[idx];

      const id = toInt(row.id, null);
      const companyId = toInt(row.company_id, null);
      const channelId = toInt(row.channel_id, null);
      const contactId = toInt(row.contact_id, null);

      // 1) required inválidos
      if (!isInt(id) || !isInt(companyId) || !isInt(channelId) || !isInt(contactId)) {
        skippedStats.missing_required++;
        writeSkip(SKIP_CSV_PATH, { id, companyId, channelId, contactId, reason: "missing_required" });
        maybePrintSkip(printed, SKIP_PRINT_LIMIT, { id, companyId, channelId, contactId, reason: "missing_required" });
        tickBar(bar, idx + 1, total, startedAt, PROGRESS_EVERY, migrated, skippedStats, errors);
        continue;
      }

      // 2) FKs obrigatórias (channel + contact)
      const hasChannel = validChannels.has(channelId);
      const hasContact = validContacts.has(contactId);

      if (!hasChannel && !hasContact) {
        skippedStats.missing_both_fk++;
        writeSkip(SKIP_CSV_PATH, { id, companyId, channelId, contactId, reason: "missing_both_fk" });
        maybePrintSkip(printed, SKIP_PRINT_LIMIT, { id, companyId, channelId, contactId, reason: "missing_both_fk" });
        tickBar(bar, idx + 1, total, startedAt, PROGRESS_EVERY, migrated, skippedStats, errors);
        continue;
      }
      if (!hasChannel) {
        skippedStats.missing_channel_fk++;
        writeSkip(SKIP_CSV_PATH, { id, companyId, channelId, contactId, reason: "missing_channel_fk" });
        maybePrintSkip(printed, SKIP_PRINT_LIMIT, { id, companyId, channelId, contactId, reason: "missing_channel_fk" });
        tickBar(bar, idx + 1, total, startedAt, PROGRESS_EVERY, migrated, skippedStats, errors);
        continue;
      }
      if (!hasContact) {
        skippedStats.missing_contact_fk++;
        writeSkip(SKIP_CSV_PATH, { id, companyId, channelId, contactId, reason: "missing_contact_fk" });
        maybePrintSkip(printed, SKIP_PRINT_LIMIT, { id, companyId, channelId, contactId, reason: "missing_contact_fk" });
        tickBar(bar, idx + 1, total, startedAt, PROGRESS_EVERY, migrated, skippedStats, errors);
        continue;
      }

      // opcionais
      let userId = toInt(row.user_id, null);
      if (!isInt(userId) || !validUsers.has(userId)) userId = null;

      let deptId = toInt(row.department_id, null);
      if (!isInt(deptId) || !validDepts.has(deptId)) deptId = null;

      let flowId = toInt(row.flow_id, null);
      if (!isInt(flowId) || !validFlows.has(flowId)) flowId = null;

      const lastMessageAt = parseTimestamp(row.lastMessageAt);
      const closedAt = parseTimestamp(row.closedAt);
      const lastInteractionAt =
        parseTimestamp(row.lastMessageReceived) ||
        parseTimestamp(row.startedAttendanceAt) ||
        lastMessageAt ||
        null;

      const flowAttempts = has("flow_attempts") ? toInt(row.bot_retries, null) : null;
      const flowStepId = has("flow_step_id") ? parseStepToInt(row.flow_step_raw) : null;

      const payload = objToRow(targetCols, {
        id,
        status: normalizeStatus(row.status),
        last_message: row.last_message != null ? String(row.last_message) : "",

        channel_id: channelId,
        contact_id: contactId,
        company_id: companyId,

        user_id: userId,
        department_id: deptId,
        flow_id: flowId,

        flow_step_id: flowStepId,
        flow_attempts: flowAttempts,

        last_message_at: lastMessageAt,
        closed_at: closedAt,
        last_interaction_at: lastInteractionAt,

        is_group: !!row.is_group,

        tags: has("tags") ? JSON.stringify([]) : undefined,

        chat_id: has("chat_id") ? Number(id) : undefined,
        is_rating: has("is_rating") ? false : undefined,

        created_at: row.created_at,
        updated_at: row.updated_at,
      });

      try {
        await dest.query(upsertSql, payload);
        migrated++;
      } catch (err) {
        errors++;
        // registra no CSV como erro pra você investigar também
        writeSkip(SKIP_CSV_PATH, { id, companyId, channelId, contactId, reason: `insert_error:${sanitize(err.message)}` });
      }

      tickBar(bar, idx + 1, total, startedAt, PROGRESS_EVERY, migrated, skippedStats, errors);
    }

    bar.stop();

    const skippedTotal =
      skippedStats.missing_required +
      skippedStats.missing_channel_fk +
      skippedStats.missing_contact_fk +
      skippedStats.missing_both_fk;

    console.log(`✅ Tickets: migrated=${migrated}, skipped=${skippedTotal}, errors=${errors}`);
    console.log("📊 Skipped por motivo:", skippedStats);
    console.log(`📄 Arquivo com TODOS os skipped: ${SKIP_CSV_PATH}`);
    if (printed.count >= SKIP_PRINT_LIMIT) {
      console.log(`ℹ️  Mostrei só ${SKIP_PRINT_LIMIT} IDs no terminal pra não virar spam. O resto está no CSV.`);
    }
  } finally {
    try { bar.stop(); } catch {}
    await source.end();
    await dest.end();
  }
};

// ---------------- helpers ----------------

function tickBar(bar, done, total, startedAtMs, every, migrated, skippedStats, errors) {
  if (done % every !== 0 && done !== total) return;
  const skippedTotal =
    skippedStats.missing_required +
    skippedStats.missing_channel_fk +
    skippedStats.missing_contact_fk +
    skippedStats.missing_both_fk;

  bar.update(done, {
    rate: calcRate(done, startedAtMs),
    status: `mig=${migrated} skip=${skippedTotal} err=${errors}`,
  });
}

function maybePrintSkip(state, limit, obj) {
  if (state.count >= limit) return;
  state.count++;
  console.log(
    `⚠️ SKIP ticket=${obj.id} company=${obj.companyId} channel=${obj.channelId} contact=${obj.contactId} reason=${obj.reason}`,
  );
}

function ensureCsvHeader(filePath) {
  if (fs.existsSync(filePath)) return;
  fs.writeFileSync(filePath, "ticket_id,company_id,channel_id,contact_id,reason\n", "utf8");
}

function writeSkip(filePath, { id, companyId, channelId, contactId, reason }) {
  const line = `${safeCsv(id)},${safeCsv(companyId)},${safeCsv(channelId)},${safeCsv(contactId)},${safeCsv(reason)}\n`;
  fs.appendFileSync(filePath, line, "utf8");
}

function safeCsv(v) {
  if (v == null) return "";
  const s = String(v).replace(/"/g, '""');
  // quote sempre pra evitar surpresa
  return `"${s}"`;
}

function sanitize(msg) {
  return String(msg || "").replace(/\s+/g, " ").slice(0, 140);
}

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

function objToRow(cols, obj) {
  return cols.map((c) => (c in obj ? obj[c] : null));
}

function toInt(v, def = null) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : def;
}

function isInt(v) {
  return Number.isInteger(v);
}

function normalizeStatus(s) {
  const v = String(s || "").trim().toLowerCase();
  const allowed = new Set(["pending", "open", "closed", "resolved", "waiting", "in_progress"]);
  return allowed.has(v) ? v : (v || "pending");
}

function parseStepToInt(v) {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s) return null;
  const m = s.match(/-?\d+/);
  if (!m) return null;
  const n = Number(m[0]);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function parseTimestamp(v) {
  if (v == null) return null;
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v;
  const num = Number(v);
  if (Number.isFinite(num)) {
    const ms = num > 1e12 ? num : num * 1000;
    const d = new Date(ms);
    return isNaN(d.getTime()) ? null : d;
  }
  const d = new Date(String(v));
  return isNaN(d.getTime()) ? null : d;
}

function calcRate(done, startedAtMs) {
  const elapsed = (Date.now() - startedAtMs) / 1000;
  return (done / Math.max(1, elapsed)).toFixed(1);
}
