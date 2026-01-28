// migrations/migrateTags.batched.js
"use strict";

require("dotenv").config();
const { Client } = require("pg");
const Cursor = require("pg-cursor");
const cliProgress = require("cli-progress");

module.exports = async function migrateTags(ctx = {}) {
  console.log('🏷️ Migrando "Tags" → "tags"...');

  // 9 params/linha → 2500 linhas ≈ 22.5k params (< 65535)
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2500);
  const DEBUG = readBool(process.env.TAGS_DEBUG, false);

  // se true, ao final faz tag_users: todas tags da empresa em todos users da empresa
  const ASSIGN_ALL_USERS = readBool(process.env.TAGS_ASSIGN_ALL_USERS, true);

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
    application_name: "migrateTags:source",
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: "migrateTags:dest",
  });

  await source.connect();
  await dest.connect();

  try {
    // 1) COUNT p/ barra/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."Tags"
      ${tenantId ? 'WHERE "tenantId" = $1' : ""}
    `;
    const { rows: crows } = await source.query(
      countSql,
      tenantId ? [tenantId] : [],
    );
    const total = Number(crows[0]?.total || 0);
    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhuma tag encontrada para TENANT_ID=${tenantId}.`
          : "⚠️  Nenhuma tag encontrada na origem.",
      );
      return;
    }

    // 2) Cursor server-side (ordem estável)
    const selectSql = `
      SELECT
        id,
        "tag"       AS name,
        "color",
        COALESCE("isActive", true) AS active,
        "tenantId"  AS company_id,
        "createdAt",
        "updatedAt"
      FROM "public"."Tags"
      ${tenantId ? 'WHERE "tenantId" = $1' : ""}
      ORDER BY id
    `;
    const cursor = source.query(
      new Cursor(selectSql, tenantId ? [tenantId] : []),
    );

    // 3) Barra de progresso
    const bar = new cliProgress.SingleBar(
      {
        format:
          "Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s",
        hideCursor: true,
      },
      cliProgress.Presets.shades_classic,
    );
    bar.start(total, 0, { rate: "0.0" });

    // 4) Upsert single (fallback)
    const upsertById = `
      INSERT INTO tags (
        id, name, color, active, company_id, is_public, owner_id, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, true, NULL, $6, $7
      )
      ON CONFLICT (id) DO UPDATE SET
        name       = EXCLUDED.name,
        color      = EXCLUDED.color,
        active     = EXCLUDED.active,
        company_id = EXCLUDED.company_id,
        is_public  = true,
        owner_id   = NULL,
        updated_at = EXCLUDED.updated_at
    `;

    // fallback quando estoura unique (name, company_id)
    const updateByNameCompany = `
      UPDATE tags
      SET
        color = $3,
        active = $4,
        is_public = true,
        owner_id = NULL,
        updated_at = $7
      WHERE
        company_id = $5
        AND name = $2
      RETURNING id
    `;

    const startedAt = Date.now();
    let processed = 0;
    let migradas = 0;
    let erros = 0;
    let fallbackUnique = 0;

    // 5) Loop por lote
    while (true) {
      const batch = await readCursor(cursor, BATCH_SIZE);
      if (!batch.length) break;

      const placeholders = [];
      const values = [];
      const perRowParams = [];

      batch.forEach((row, i) => {
        const name = safeName(row.name, row.id);
        const color = normalizeColor(row.color);
        const active = row.active !== false;

        const v = [
          row.id, // 1
          name, // 2
          color, // 3
          active, // 4
          row.company_id, // 5
          row.createdAt, // 6
          row.updatedAt, // 7
        ];
        perRowParams.push(v);

        const base = i * 7;
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, true, NULL, $${base + 6}, $${base + 7})`,
        );
        values.push(...v);
      });

      try {
        await dest.query("BEGIN");
        await dest.query("SET LOCAL synchronous_commit TO OFF");
        await dest.query(
          `
          INSERT INTO tags (
            id, name, color, active, company_id, is_public, owner_id, created_at, updated_at
          ) VALUES
            ${placeholders.join(",")}
          ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            color      = EXCLUDED.color,
            active     = EXCLUDED.active,
            company_id = EXCLUDED.company_id,
            is_public  = true,
            owner_id   = NULL,
            updated_at = EXCLUDED.updated_at
          `,
          values,
        );
        await dest.query("COMMIT");
        migradas += batch.length;
      } catch (batchErr) {
        await dest.query("ROLLBACK");

        for (const v of perRowParams) {
          try {
            await dest.query("BEGIN");
            await dest.query("SET LOCAL synchronous_commit TO OFF");
            await dest.query(upsertById, v);
            await dest.query("COMMIT");
            migradas += 1;
          } catch (rowErr) {
            await dest.query("ROLLBACK");

            if (isUniqueNameCompanyError(rowErr)) {
              try {
                await dest.query("BEGIN");
                await dest.query("SET LOCAL synchronous_commit TO OFF");
                const upd = await dest.query(updateByNameCompany, v);
                await dest.query("COMMIT");
                fallbackUnique += 1;
                migradas += 1;

                if (DEBUG) {
                  const existingId = upd.rows?.[0]?.id;
                  console.log(
                    `🧩 fallback unique -> name="${v[1]}" company=${v[4]} idExistente=${existingId}`,
                  );
                }
              } catch (updErr) {
                await dest.query("ROLLBACK");
                erros += 1;
                console.error(
                  `❌ Erro fallback UPDATE tag (name,company) idOrig=${v[0]} name="${v[1]}" company=${v[4]}: ${updErr.message}`,
                );
              }
            } else {
              erros += 1;
              console.error(
                `❌ Erro ao migrar tag id=${v[0]}: ${rowErr.message}`,
              );
            }
          }
        }
      }

      processed += batch.length;
      bar.update(processed, { rate: calcRate(processed, startedAt) });
    }

    bar.stop();
    await new Promise((resolve, reject) =>
      cursor.close((err) => (err ? reject(err) : resolve())),
    );

    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `✅ Migradas ${migradas}/${total} tag(s) em ${secs}s.` +
        (fallbackUnique
          ? ` (fallback unique name+company: ${fallbackUnique})`
          : "") +
        (erros ? ` (${erros} com erro)` : ""),
    );

    // Ajusta sequence
    try {
      await dest.query(
        `SELECT setval('tags_id_seq', (SELECT COALESCE(MAX(id), 1) FROM tags))`,
      );
    } catch {
      // ignora
    }

    // ✅ Agora: marcar todas as tags para todos usuários da empresa
    if (ASSIGN_ALL_USERS) {
      const info = tenantId ? `TENANT_ID=${tenantId}` : "todas empresas";
      console.log(`👥 Vinculando tags → users (${info}) em tag_users...`);
      const { inserted, existing } = await assignAllTagsToAllUsers(
        dest,
        tenantId,
      );
      console.log(
        `✅ tag_users: inseridos=${inserted} | já existiam=${existing}`,
      );
    }
  } finally {
    await source.end();
    await dest.end();
  }
};

// ---------------------------------------------------------
// Vincula TODAS as tags da empresa em TODOS os users da empresa
// Usa NOT EXISTS pra não depender de constraint na pivô.
// ---------------------------------------------------------
async function assignAllTagsToAllUsers(dest, tenantId) {
  // total atual (pra calcular diferença)
  const beforeRes = await dest.query(
    `SELECT COUNT(*)::bigint AS c FROM tag_users`,
  );
  const before = Number(beforeRes.rows?.[0]?.c || 0);

  // Inserção em massa
  // Importante: GORM geralmente cria pivô como (tag_id, user_id)
  // Se sua pivô tiver colunas extras (id, timestamps), me fala que eu ajusto.
  const insertSql = tenantId
    ? `
      INSERT INTO tag_users (tag_id, user_id)
      SELECT t.id, u.id
      FROM tags t
      JOIN users u ON u.company_id = t.company_id
      WHERE t.company_id = $1
        AND NOT EXISTS (
          SELECT 1 FROM tag_users tu
          WHERE tu.tag_id = t.id AND tu.user_id = u.id
        )
    `
    : `
      INSERT INTO tag_users (tag_id, user_id)
      SELECT t.id, u.id
      FROM tags t
      JOIN users u ON u.company_id = t.company_id
      WHERE NOT EXISTS (
        SELECT 1 FROM tag_users tu
        WHERE tu.tag_id = t.id AND tu.user_id = u.id
      )
    `;

  await dest.query("BEGIN");
  await dest.query("SET LOCAL synchronous_commit TO OFF");
  await dest.query(insertSql, tenantId ? [tenantId] : []);
  await dest.query("COMMIT");

  const afterRes = await dest.query(
    `SELECT COUNT(*)::bigint AS c FROM tag_users`,
  );
  const after = Number(afterRes.rows?.[0]?.c || 0);

  const inserted = Math.max(0, after - before);

  // estimativa de "já existiam":
  // total possíveis - inserted (não é perfeito mas ajuda)
  const possibleRes = tenantId
    ? await dest.query(
        `
        SELECT (COUNT(*)::bigint) AS c
        FROM tags t
        JOIN users u ON u.company_id = t.company_id
        WHERE t.company_id = $1
      `,
        [tenantId],
      )
    : await dest.query(`
        SELECT (COUNT(*)::bigint) AS c
        FROM tags t
        JOIN users u ON u.company_id = t.company_id
      `);

  const possible = Number(possibleRes.rows?.[0]?.c || 0);
  const existing = Math.max(0, possible - inserted);

  return { inserted, existing };
}

// —— helpers
function readBool(v, def = false) {
  if (v == null) return def;
  const s = String(v).trim().toLowerCase();
  return ["1", "true", "t", "yes", "y"].includes(s);
}
async function readCursor(cursor, size) {
  return await new Promise((resolve, reject) => {
    cursor.read(size, (err, rows) => (err ? reject(err) : resolve(rows || [])));
  });
}
function calcRate(processed, startedAtMs) {
  const elapsed = (Date.now() - startedAtMs) / 1000;
  return (processed / Math.max(1, elapsed)).toFixed(1);
}
function safeName(name, id) {
  const n = (name || "").toString().trim();
  return n.length ? n : `Tag ${id}`;
}
function normalizeColor(v) {
  if (!v) return "#999999";
  let s = String(v).trim();

  const mRgb = /^rgb\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*\)$/i.exec(
    s,
  );
  if (mRgb) {
    const [r, g, b] = mRgb.slice(1, 4).map((n) => clampInt(+n, 0, 255));
    return `#${toHex2(r)}${toHex2(g)}${toHex2(b)}`;
  }

  const mHex3 = /^#?([0-9a-f]{3})$/i.exec(s);
  if (mHex3) {
    const [a, b, c] = mHex3[1].split("");
    return `#${a}${a}${b}${b}${c}${c}`.toLowerCase();
  }

  const mHex6 = /^#?([0-9a-f]{6})$/i.exec(s);
  if (mHex6) return `#${mHex6[1].toLowerCase()}`;

  return "#999999";
}
function clampInt(n, min, max) {
  if (!Number.isFinite(n)) return min;
  return Math.max(min, Math.min(max, Math.trunc(n)));
}
function toHex2(n) {
  const s = clampInt(n, 0, 255).toString(16);
  return s.length === 1 ? "0" + s : s;
}
function isUniqueNameCompanyError(err) {
  const msg = (err && err.message ? String(err.message) : "").toLowerCase();
  return (
    msg.includes("duplicate key") &&
    (msg.includes("idx_name_companyid") ||
      (msg.includes("name") && msg.includes("company")))
  );
}
