// migrations/migrateContacts.batched.js
"use strict";

require("dotenv").config();
const { Client } = require("pg");
const Cursor = require("pg-cursor");
const cliProgress = require("cli-progress");

module.exports = async function migrateContacts(ctx = {}) {
  console.log(
    '📇 Migrando "Contacts" → "contacts"... (tags only, sem carteiras)',
  );

  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);

  // LOG_MODE: batch | row | both
  const LOG_MODE = (process.env.CONTACTS_LOG_MODE || "batch").toLowerCase();
  const LOG_EVERY_BATCH = Number(process.env.CONTACTS_LOG_EVERY_BATCH || 1);
  const LOG_EVERY_ROW = Number(process.env.CONTACTS_LOG_EVERY_ROW || 200);

  // debug: mostra amostra dos primeiros N contatos do primeiro batch
  const DEBUG_SAMPLE = readBool(process.env.DEBUG_CONTACTS_SAMPLE, false);
  const DEBUG_SAMPLE_N = Number(process.env.DEBUG_CONTACTS_SAMPLE_N || 10);

  const tenantId =
    ctx.tenantId != null && String(ctx.tenantId).trim() !== ""
      ? String(ctx.tenantId).trim()
      : process.env.TENANT_ID
        ? String(process.env.TENANT_ID).trim()
        : null;

  // ✅ ORIGEM: 2 conexões (cursor + aux)
  const sourceCur = new Client({
    host: process.env.SRC_HOST,
    port: process.env.SRC_PORT,
    user: process.env.SRC_USER,
    password: process.env.SRC_PASS,
    database: process.env.SRC_DB,
    application_name: "migrateContacts:source:cursor",
  });

  const sourceAux = new Client({
    host: process.env.SRC_HOST,
    port: process.env.SRC_PORT,
    user: process.env.SRC_USER,
    password: process.env.SRC_PASS,
    database: process.env.SRC_DB,
    application_name: "migrateContacts:source:aux",
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: "migrateContacts:dest",
  });

  await sourceCur.connect();
  await sourceAux.connect();
  await dest.connect();

  // opcional: evita “congelar” se alguma query auxiliar travar
  const AUX_TIMEOUT = String(process.env.CONTACTS_AUX_TIMEOUT || "").trim();
  if (AUX_TIMEOUT) {
    await sourceAux.query(`SET statement_timeout TO '${AUX_TIMEOUT}'`);
  }

  let cursor;

  try {
    // ---------------------------
    // 0) Descobrir colunas do DEST (pra não quebrar quando algo não existe)
    // ---------------------------
    const destCols = await loadDestColumns(dest, "contacts");
    const has = (c) => destCols.has(c);

    // ✅ como você NÃO quer carteira, a gente ignora channel_* e channel_assignments
    const targetCols = [
      "id",
      "name",

      has("phone_number") ? "phone_number" : null,
      has("j_id") ? "j_id" : null,
      has("lid") ? "lid" : null,

      has("telephone_number") ? "telephone_number" : null,
      has("whatsapp") ? "whatsapp" : null,
      has("instagram") ? "instagram" : null,
      has("instagram_id") ? "instagram_id" : null,
      has("telegram") ? "telegram" : null,
      has("messenger") ? "messenger" : null,
      has("email") ? "email" : null,
      has("send_email") ? "send_email" : null,

      has("profile_pic_url") ? "profile_pic_url" : null,
      has("push_name") ? "push_name" : null,

      has("is_wa_contact") ? "is_wa_contact" : null,
      has("is_group") ? "is_group" : null,
      has("type") ? "type" : null,

      has("cpf") ? "cpf" : null,
      has("cnpj") ? "cnpj" : null, // origem não tem -> null
      has("birth_date") ? "birth_date" : null,

      has("address") ? "address" : null,
      has("annotations") ? "annotations" : null,

      // ❌ removidos: channel_id, queue_id, channel_assignments
      has("last_campaign_sent_at") ? "last_campaign_sent_at" : null,
      has("tags") ? "tags" : null,

      "company_id",
      "created_at",
      "updated_at",
      has("deleted_at") ? "deleted_at" : null,
    ].filter(Boolean);

    // ---------------------------
    // 1) Pré-carregar IDs válidos de TAGS do DEST (por empresa)
    // ---------------------------
    const whereCompany = tenantId ? "WHERE company_id = $1" : "";
    const paramsCompany = tenantId ? [tenantId] : [];

    const tagsRes = has("tags")
      ? await dest.query(
          `SELECT id, company_id FROM tags ${whereCompany}`,
          paramsCompany,
        )
      : { rows: [] };

    const tagsByCompany = idsByCompany(tagsRes.rows);

    // ---------------------------
    // 2) COUNT para barra/ETA
    // ---------------------------
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."Contacts"
      ${tenantId ? 'WHERE "tenantId" = $1' : ""}
    `;
    const { rows: crows } = await sourceAux.query(
      countSql,
      tenantId ? [tenantId] : [],
    );
    const total = Number(crows[0]?.total || 0);

    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhum contato encontrado para TENANT_ID=${tenantId}.`
          : "⚠️  Nenhum contato encontrado na origem.",
      );
      return;
    }
    console.log(`📦 Total de contatos: ${total}`);

    // ---------------------------
    // 3) Cursor server-side (ORIGEM)
    // ---------------------------
    const selectSql = `
      SELECT
        id,
        name,
        number,
        "profilePicUrl" AS profile_pic_url,
        email,
        "isGroup" AS is_group,
        "tenantId" AS company_id,
        pushname AS push_name,
        "isWAContact" AS is_wa_contact,
        cpf,
        "birthdayDate" AS birth_date,
        "businessName" AS business_name,
        "firstName" AS first_name,
        "lastName" AS last_name,
        lid,
        "isLid" AS is_lid,
        "telegramId" AS telegram_id,
        "instagramPK" AS instagram_pk,
        "messengerId" AS messenger_id,
        blocked,
        "createdAt" AS created_at,
        "updatedAt" AS updated_at
      FROM "public"."Contacts"
      ${tenantId ? 'WHERE "tenantId" = $1' : ""}
      ORDER BY id
    `;
    cursor = sourceCur.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    // ---------------------------
    // 4) Barra de progresso
    // ---------------------------
    const bar = new cliProgress.SingleBar(
      {
        format:
          "Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s | {status}",
        hideCursor: true,
        clearOnComplete: false,
      },
      cliProgress.Presets.shades_classic,
    );

    bar.start(total, 0, { status: "Iniciando..." });

    // ---------------------------
    // 5) SQL UPSERT (dinâmico)
    // ---------------------------
    const insertColsSql = targetCols.join(", ");
    const updateCols = targetCols
      .filter((c) => c !== "id" && c !== "created_at")
      .map((c) => `${c}=EXCLUDED.${c}`);

    const upsertBatch = (placeholdersSql) => `
      INSERT INTO contacts (${insertColsSql})
      VALUES ${placeholdersSql}
      ON CONFLICT (id) DO UPDATE SET
        ${updateCols.join(", ")}
    `;

    // ---------------------------
    // 6) Loop por batch
    // ---------------------------
    const startedAt = Date.now();
    let processed = 0;
    let migrados = 0;
    let erros = 0;
    let batchIndex = 0;

    while (true) {
      const batch = await readCursor(cursor, BATCH_SIZE);
      if (!batch.length) break;

      batchIndex++;

      const batchFirstId = batch[0]?.id;
      const batchLastId = batch[batch.length - 1]?.id;

      // --- Pré-carrega tags deste batch (✅ somente ContactTags)
      const ids = batch
        .map((r) => Number(r.id))
        .filter((n) => Number.isInteger(n));
      if (!ids.length) {
        processed += batch.length;
        const elapsed = (Date.now() - startedAt) / 1000;
        bar.update(processed, {
          status: `Batch #${batchIndex} → carregando tags (${batchFirstId}..${batchLastId})`,
        });
        continue;
      }

      const ctRes = await sourceAux.query(
        `
        SELECT "contactId" AS contact_id, array_agg(DISTINCT "tagId") AS tag_ids
        FROM "public"."ContactTags"
        WHERE "contactId" = ANY($1::int[])
        GROUP BY "contactId"
        `,
        [ids],
      );

      const tagsIdMap = new Map(
        ctRes.rows.map((r) => [Number(r.contact_id), r.tag_ids || []]),
      );

      // --- Construir INSERT batch
      const values = [];
      const placeholders = [];
      const perRowParams = [];

      const debugLimit = DEBUG_SAMPLE && batchIndex === 1 ? DEBUG_SAMPLE_N : 0;

      for (let i = 0; i < batch.length; i++) {
        const row = batch[i];
        const companyId = Number(row.company_id);
        const companyKey = String(companyId);

        const globalIndex = processed + i + 1;

        const contactId = Number(row.id);

        const rawNumber = onlyDigits(row.number);
        const phoneNumber = rawNumber ? truncate(rawNumber, 20) : null;

        const isGroup = row.is_group === true;
        const isWaContact = row.is_wa_contact === true || !!phoneNumber;

        const whatsapp = phoneNumber;
        const jId = phoneNumber
          ? `${phoneNumber}${isGroup ? "@g.us" : "@s.whatsapp.net"}`
          : null;

        const lidVal = row.is_lid && row.lid ? String(row.lid) : null;

        const cpf = row.cpf ? String(row.cpf).trim() : null;

        // origem não tem cnpj -> sempre null
        const cnpj = null;

        const birthDate = row.birth_date
          ? normalizeBirthDate(row.birth_date)
          : null;

        const telegram =
          row.telegram_id != null ? String(row.telegram_id) : null;
        const instagramId =
          row.instagram_pk != null ? String(row.instagram_pk) : null;
        const instagram = instagramId;
        const messenger = row.messenger_id ? String(row.messenger_id) : null;

        const email = row.email ? String(row.email).trim().toLowerCase() : null;

        // annotations com extras úteis
        const annotations = buildAnnotations({
          businessName: row.business_name,
          firstName: row.first_name,
          lastName: row.last_name,
          blocked: row.blocked,
        });

        // type: se tiver business_name -> assume PJ (2), senão PF (1)
        const type = row.business_name ? 2 : 1;

        // ✅ tags NOVO formato [{tag, auto_assign}]
        let tags = [];
        if (has("tags")) {
          const validTagsSet = tagsByCompany.get(companyKey);
          const tagIds = tagsIdMap.get(contactId) || [];
          tags = (Array.isArray(tagIds) ? tagIds : [])
            .map((t) => toInt(t, null))
            .filter((t) => Number.isInteger(t))
            .filter((t) => (validTagsSet ? validTagsSet.has(t) : true))
            .map((t) => ({ tag: t, auto_assign: false }));
        }

        const rowOut = buildRowByCols(targetCols, {
          id: contactId,
          name: safeName(row.name, contactId),

          phone_number: phoneNumber,
          j_id: jId,
          lid: lidVal,

          telephone_number: null,
          whatsapp,
          instagram,
          instagram_id: instagramId,
          telegram,
          messenger,
          email,
          send_email: has("send_email") ? true : undefined,

          profile_pic_url: row.profile_pic_url || null,
          push_name: row.push_name || null,

          is_wa_contact: isWaContact,
          is_group: isGroup,
          type,

          cpf,
          cnpj,
          birth_date: birthDate,

          address: "",
          annotations: annotations || "",

          last_campaign_sent_at: null,
          tags: JSON.stringify(tags),

          company_id: companyId,
          created_at: row.created_at,
          updated_at: row.updated_at,
          deleted_at: null,
        });

        const base = i * targetCols.length;
        placeholders.push(
          `(${targetCols.map((_, j) => `$${base + j + 1}`).join(", ")})`,
        );
        values.push(...rowOut);
        perRowParams.push(rowOut);
      }

      if (!placeholders.length) {
        processed += batch.length;
        const elapsed = (Date.now() - startedAt) / 1000;
        bar.update(processed, {
          status: `Batch #${batchIndex} → inserindo no destino (${batch.length})`,
        });
        continue;
      }

      const sql = upsertBatch(placeholders.join(","));

      try {
        await dest.query("BEGIN");
        await dest.query("SET LOCAL synchronous_commit TO OFF");
        await dest.query(sql, values);
        await dest.query("COMMIT");

        migrados += batch.length;
      } catch (batchErr) {
        try {
          await dest.query("ROLLBACK");
        } catch {
          /* ignore */
        }
        console.error(
          `❌ Batch falhou ids=${batchFirstId}..${batchLastId}: ${batchErr.message}`,
        );

        // fallback row-by-row
        for (let i = 0; i < perRowParams.length; i++) {
          const rowVals = perRowParams[i];
          const contactId = rowVals[targetCols.indexOf("id")];

          try {
            const rowPlaceholders = `(${targetCols.map((_, j) => `$${j + 1}`).join(", ")})`;
            const rowSql = upsertBatch(rowPlaceholders);

            await dest.query("BEGIN");
            await dest.query("SET LOCAL synchronous_commit TO OFF");
            await dest.query(rowSql, rowVals);
            await dest.query("COMMIT");

            migrados += 1;
          } catch (rowErr) {
            try {
              await dest.query("ROLLBACK");
            } catch {
              /* ignore */
            }
            erros += 1;
            console.error(`❌ Erro contato id=${contactId}: ${rowErr.message}`);
          }
        }
      }

      processed += batch.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = (processed / Math.max(1, elapsed)).toFixed(1);
      bar.update(processed + batch.length, {
        status: `Batch #${batchIndex} ✔ commit OK`,
      });
    }

    bar.stop();

    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `✅ Migrados ${migrados}/${total} contato(s) em ${secs}s.${erros ? ` (${erros} com erro)` : ""}`,
    );
  } finally {
    try {
      if (cursor) await new Promise((resolve) => cursor.close(() => resolve()));
    } catch {
      /* ignore */
    }
    await sourceCur.end();
    await sourceAux.end();
    await dest.end();
  }
};

// --------------------- helpers ---------------------

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

function onlyDigits(v) {
  if (v == null) return null;
  const s = String(v).replace(/\D+/g, "");
  return s.length ? s : null;
}

function truncate(v, max) {
  if (v == null) return v;
  const s = String(v);
  return s.length > max ? s.slice(0, max) : s;
}

function safeName(name, id) {
  const n = (name || "").toString().trim();
  return n.length ? n : `Contato ${id}`;
}

function toInt(v, def = null) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : def;
}

function idsByCompany(rows) {
  const map = new Map(); // company_id -> Set(ids)
  for (const r of rows || []) {
    const k = String(r.company_id);
    if (!map.has(k)) map.set(k, new Set());
    map.get(k).add(Number(r.id));
  }
  return map;
}

async function loadDestColumns(client, tableName) {
  const res = await client.query(
    `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name=$1
    `,
    [tableName],
  );
  return new Set(res.rows.map((r) => r.column_name));
}

function buildRowByCols(targetCols, obj) {
  return targetCols.map((c) => (c in obj ? obj[c] : null));
}

function normalizeBirthDate(v) {
  if (!v) return null;
  const s = String(v).trim();
  if (!s) return null;
  return truncate(s, 10);
}

function buildAnnotations(extra) {
  const parts = [];
  for (const [k, v] of Object.entries(extra || {})) {
    if (v == null || v === "") continue;
    parts.push(`${k}: ${String(v)}`);
  }
  return parts.join("\n");
}
