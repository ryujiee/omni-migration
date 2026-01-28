// migrations/migrateDepartments.batched.js
"use strict";

require("dotenv").config();
const { Client } = require("pg");
const Cursor = require("pg-cursor");
const cliProgress = require("cli-progress");

module.exports = async function migrateDepartments(ctx = {}) {
  console.log('🏢 Migrando "Queues" → "departments"...');

  // agora são 22 params/linha → 2000 linhas ≈ 44k params (< 65535)
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);

  // Usa TENANT_ID do ctx (preferencial) ou do .env
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
    application_name: "migrateDepartments:source",
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: "migrateDepartments:dest",
  });

  await source.connect();
  await dest.connect();

  try {
    // 1) total p/ barra/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."Queues"
      ${tenantId ? 'WHERE "tenantId" = $1' : ""}
    `;
    const { rows: countRows } = await source.query(
      countSql,
      tenantId ? [tenantId] : [],
    );
    const total = Number(countRows[0]?.total || 0);

    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhum departamento (queue) encontrado para TENANT_ID=${tenantId}.`
          : "⚠️  Nenhum departamento (queue) encontrado na origem.",
      );
      return;
    }

    // 2) cursor server-side (ordem estável)
    // ✅ apenas colunas que existem no schema antigo atual
    const selectSql = `
      SELECT
        "id",
        "queue" AS name,
        COALESCE("isActive", true) AS status,
        "tenantId" AS company_id,
        "createdAt",
        "updatedAt"
      FROM "public"."Queues"
      ${tenantId ? 'WHERE "tenantId" = $1' : ""}
      ORDER BY "id"
    `;
    const cursor = source.query(
      new Cursor(selectSql, tenantId ? [tenantId] : []),
    );

    // 3) barra de progresso
    const bar = new cliProgress.SingleBar(
      {
        format:
          "Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s",
        hideCursor: true,
      },
      cliProgress.Presets.shades_classic,
    );
    bar.start(total, 0, { rate: "0.0" });

    const startedAt = Date.now();
    let processed = 0;
    let migrados = 0;
    let erros = 0;

    // 4) UPSERT single (fallback)
    const upsertSqlSingle = `
      INSERT INTO departments (
        id, name, status, company_id, color, transfer_type,
        inactivity_active, inactivity_seconds, inactivity_action, inactivity_target_id,
        open_inactivity_active, open_inactivity_seconds, open_inactivity_action, open_inactivity_target_id,
        email_on_close_enabled,
        rating_enabled, rating_flow_id, rating_timeout_message, rating_timeout_seconds,
        created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, 'queue',
        $6, $7, $8, $9,
        $10, $11, $12, $13,
        $14,
        $15, $16, $17, $18,
        $19, $20
      )
      ON CONFLICT (id) DO UPDATE SET
        name                      = EXCLUDED.name,
        status                    = EXCLUDED.status,
        company_id                = EXCLUDED.company_id,
        color                     = EXCLUDED.color,
        transfer_type             = EXCLUDED.transfer_type,
        inactivity_active         = EXCLUDED.inactivity_active,
        inactivity_seconds        = EXCLUDED.inactivity_seconds,
        inactivity_action         = EXCLUDED.inactivity_action,
        inactivity_target_id      = EXCLUDED.inactivity_target_id,
        open_inactivity_active    = EXCLUDED.open_inactivity_active,
        open_inactivity_seconds   = EXCLUDED.open_inactivity_seconds,
        open_inactivity_action    = EXCLUDED.open_inactivity_action,
        open_inactivity_target_id = EXCLUDED.open_inactivity_target_id,
        email_on_close_enabled    = EXCLUDED.email_on_close_enabled,
        rating_enabled            = EXCLUDED.rating_enabled,
        rating_flow_id            = EXCLUDED.rating_flow_id,
        rating_timeout_message    = EXCLUDED.rating_timeout_message,
        rating_timeout_seconds    = EXCLUDED.rating_timeout_seconds,
        updated_at                = EXCLUDED.updated_at
    `;

    // 5) loop por lote
    while (true) {
      const batch = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!batch || batch.length === 0) break;

      const placeholders = [];
      const values = [];
      const perRowParams = []; // fallback se batch falhar

      batch.forEach((row, i) => {
        const id = row.id;
        const name = safeName(row.name, id);
        const status = toBool(row.status, true);
        const companyId = row.company_id;

        // defaults novos do DEST
        const color = ""; // ou escolha uma cor padrão se quiser
        const inactivityActive = false;
        const inactivitySeconds = 0;
        const inactivityAction = null;
        const inactivityTargetId = null;

        const openInactivityActive = false;
        const openInactivitySeconds = 0;
        const openInactivityAction = null;
        const openInactivityTargetId = null;

        const emailOnCloseEnabled = false;

        const ratingEnabled = false;
        const ratingFlowId = null;
        const ratingTimeoutMessage = null;
        const ratingTimeoutSeconds = null;

        const createdAt = row.createdAt;
        const updatedAt = row.updatedAt;

        const v = [
          id, // 1
          name, // 2
          status, // 3
          companyId, // 4
          color, // 5
          inactivityActive, // 6
          inactivitySeconds, // 7
          inactivityAction, // 8
          inactivityTargetId, // 9
          openInactivityActive, // 10
          openInactivitySeconds, // 11
          openInactivityAction, // 12
          openInactivityTargetId, // 13
          emailOnCloseEnabled, // 14
          ratingEnabled, // 15
          ratingFlowId, // 16
          ratingTimeoutMessage, // 17
          ratingTimeoutSeconds, // 18
          createdAt, // 19
          updatedAt, // 20
        ];

        perRowParams.push(v);

        const base = i * 20;
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, 'queue', ` +
            `$${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, ` +
            `$${base + 10}, $${base + 11}, $${base + 12}, $${base + 13}, ` +
            `$${base + 14}, ` +
            `$${base + 15}, $${base + 16}, $${base + 17}, $${base + 18}, ` +
            `$${base + 19}, $${base + 20})`,
        );

        values.push(...v);
      });

      const upsertSqlBatch = `
        INSERT INTO departments (
          id, name, status, company_id, color, transfer_type,
          inactivity_active, inactivity_seconds, inactivity_action, inactivity_target_id,
          open_inactivity_active, open_inactivity_seconds, open_inactivity_action, open_inactivity_target_id,
          email_on_close_enabled,
          rating_enabled, rating_flow_id, rating_timeout_message, rating_timeout_seconds,
          created_at, updated_at
        ) VALUES
          ${placeholders.join(",")}
        ON CONFLICT (id) DO UPDATE SET
          name                      = EXCLUDED.name,
          status                    = EXCLUDED.status,
          company_id                = EXCLUDED.company_id,
          color                     = EXCLUDED.color,
          transfer_type             = EXCLUDED.transfer_type,
          inactivity_active         = EXCLUDED.inactivity_active,
          inactivity_seconds        = EXCLUDED.inactivity_seconds,
          inactivity_action         = EXCLUDED.inactivity_action,
          inactivity_target_id      = EXCLUDED.inactivity_target_id,
          open_inactivity_active    = EXCLUDED.open_inactivity_active,
          open_inactivity_seconds   = EXCLUDED.open_inactivity_seconds,
          open_inactivity_action    = EXCLUDED.open_inactivity_action,
          open_inactivity_target_id = EXCLUDED.open_inactivity_target_id,
          email_on_close_enabled    = EXCLUDED.email_on_close_enabled,
          rating_enabled            = EXCLUDED.rating_enabled,
          rating_flow_id            = EXCLUDED.rating_flow_id,
          rating_timeout_message    = EXCLUDED.rating_timeout_message,
          rating_timeout_seconds    = EXCLUDED.rating_timeout_seconds,
          updated_at                = EXCLUDED.updated_at
      `;

      try {
        await dest.query("BEGIN");
        await dest.query("SET LOCAL synchronous_commit TO OFF");
        await dest.query(upsertSqlBatch, values);
        await dest.query("COMMIT");
        migrados += batch.length;
      } catch (batchErr) {
        await dest.query("ROLLBACK");

        // fallback: registro a registro
        for (const v of perRowParams) {
          try {
            await dest.query("BEGIN");
            await dest.query("SET LOCAL synchronous_commit TO OFF");
            await dest.query(upsertSqlSingle, v);
            await dest.query("COMMIT");
            migrados += 1;
          } catch (rowErr) {
            await dest.query("ROLLBACK");
            erros += 1;
            console.error(
              `❌ Erro ao migrar department id=${v[0]}: ${rowErr.message}`,
            );
          }
        }
      }

      processed += batch.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = (processed / Math.max(1, elapsed)).toFixed(1);
      bar.update(processed, { rate });
    }

    bar.stop();
    await new Promise((resolve, reject) =>
      cursor.close((err) => (err ? reject(err) : resolve())),
    );

    // ✅ Ajusta sequence do DEST (evita colisões futuras)
    try {
      await dest.query(
        `SELECT setval('departments_id_seq', (SELECT COALESCE(MAX(id), 1) FROM departments))`,
      );
    } catch (e) {
      console.warn(
        `⚠️  Não foi possível ajustar sequence departments_id_seq: ${e.message}`,
      );
    }

    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `✅ Migrados ${migrados}/${total} departamento(s) em ${secs}s.${erros ? ` (${erros} com erro)` : ""}`,
    );
  } finally {
    await source.end();
    await dest.end();
  }
};

// helpers
function safeName(name, id) {
  const n = (name || "").toString().trim();
  return n.length ? n : `Departamento ${id}`;
}

function toBool(v, def = false) {
  if (typeof v === "boolean") return v;
  if (v == null) return def;
  const s = String(v).trim().toLowerCase();
  return (
    s === "true" || s === "1" || s === "t" || s === "yes" || s === "enabled"
  );
}
