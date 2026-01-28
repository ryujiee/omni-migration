// migrations/migrateTenants.batched.js
"use strict";

require("dotenv").config();
const { Client } = require("pg");
const Cursor = require("pg-cursor");
const cliProgress = require("cli-progress");

module.exports = async function migrateTenants(ctx = {}) {
  console.log('📦 Migrando "Tenants" → "companies"...');

  // 18 params/linha → 2000 linhas ≈ 36k params (< 65535) — seguro
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);
  const INCLUDE_MASTER = readBool(process.env.TENANTS_INCLUDE_MASTER, false); // se true, permite id=1

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
    application_name: "migrateTenants:source",
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: "migrateTenants:dest",
  });

  await source.connect();
  await dest.connect();

  try {
    // —— 0) Carrega CNPJs/Subdomains já existentes no DEST pra garantir unicidade
    // (uni_companies_cnpj é UNIQUE, então precisamos garantir não colidir)
    const existingRes = await dest.query(`
      SELECT
        COALESCE(LOWER(subdomain),'') AS subdomain,
        COALESCE(cnpj,'') AS cnpj
      FROM companies
    `);

    const existingSubdomains = new Set(
      existingRes.rows.map((r) => r.subdomain).filter(Boolean),
    );

    // no destino pode ter CNPJ com máscara ou sem — normaliza pra dígitos
    const existingCnpjs = new Set(
      existingRes.rows.map((r) => normalizeCnpjDigits(r.cnpj)).filter(Boolean),
    );

    // —— 1) COUNT para barra/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."Tenants"
      ${tenantId ? 'WHERE "id" = $1' : INCLUDE_MASTER ? "" : 'WHERE "id" != 1'}
    `;
    const countRes = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(countRes.rows[0]?.total || 0);

    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhum tenant encontrado com ID ${tenantId}.`
          : INCLUDE_MASTER
            ? "⚠️  Nenhum tenant encontrado."
            : "⚠️  Nenhum tenant elegível encontrado (id != 1).",
      );
      return;
    }

    // —— 2) Cursor server-side
    // ✅ REMOVIDO "cnpj" (não existe na origem)
    const selectSql = `
      SELECT
        "id",
        "name",
        COALESCE("maxUsers", 0) AS users_allowed,
        "status",
        "createdAt",
        "updatedAt"
      FROM "public"."Tenants"
      ${tenantId ? 'WHERE "id" = $1' : INCLUDE_MASTER ? "" : 'WHERE "id" != 1'}
      ORDER BY "id"
    `;
    const cursor = source.query(
      new Cursor(selectSql, tenantId ? [tenantId] : []),
    );

    // —— 3) Barra de progresso
    const bar = new cliProgress.SingleBar(
      {
        format:
          "Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s",
        hideCursor: true,
      },
      cliProgress.Presets.shades_classic,
    );
    bar.start(total, 0, { rate: "0.0" });

    // —— 4) UPSERT single (fallback)
    const upsertSqlSingle = `
      INSERT INTO companies (
        id, name, cnpj, users_allowed, status, plan, address, price_per_user,
        is_master, logo, theme, background, subdomain, omni_name, favicon,
        resale_id, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6::jsonb, $7, $8,
        $9, $10, $11::jsonb, $12, $13, $14, $15,
        $16, $17, $18
      )
      ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        cnpj = EXCLUDED.cnpj,
        users_allowed = EXCLUDED.users_allowed,
        status = EXCLUDED.status,
        plan = EXCLUDED.plan,
        address = EXCLUDED.address,
        price_per_user = EXCLUDED.price_per_user,
        is_master = EXCLUDED.is_master,
        logo = EXCLUDED.logo,
        theme = EXCLUDED.theme,
        background = EXCLUDED.background,
        subdomain = EXCLUDED.subdomain,
        omni_name = EXCLUDED.omni_name,
        favicon = EXCLUDED.favicon,
        resale_id = EXCLUDED.resale_id,
        updated_at = EXCLUDED.updated_at
    `;

    const startedAt = Date.now();
    let processed = 0;
    let migrados = 0;
    let erros = 0;

    // —— 5) Loop por lote
    while (true) {
      const batch = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!batch || batch.length === 0) break;

      const placeholders = [];
      const values = [];
      const perRowParams = [];

      // unicidade intra-batch
      const batchCnpjs = new Set();
      const batchSubs = new Set();

      batch.forEach((row, i) => {
        const id = row.id;
        const name = safeName(row.name, id);
        const status = normalizeStatus(row.status);
        const createdAt = row.createdAt;
        const updatedAt = row.updatedAt;

        // ✅ gera CNPJ válido determinístico por id
        // (14 dígitos SEM máscara)
        let cnpj = generateDeterministicCnpjFromId(id);

        // garante não colidir com o que já existe (dest + batch)
        // se colidir, tenta variações (seed increment)
        let seed = 0;
        while (existingCnpjs.has(cnpj) || batchCnpjs.has(cnpj)) {
          seed += 1;
          cnpj = generateDeterministicCnpjFromId(id, seed);
        }
        batchCnpjs.add(cnpj);

        // subdomain — slug + (tenta) unicidade global (dest + batch)
        let subdomain = slugSubdomain(name, id);
        while (existingSubdomains.has(subdomain) || batchSubs.has(subdomain)) {
          subdomain = ensureLength(
            `${slugSubdomain(name)}-${id}`.toLowerCase(),
            3,
            63,
          );
          if (existingSubdomains.has(subdomain) || batchSubs.has(subdomain)) {
            subdomain = ensureLength(
              `${slugSubdomain(name)}-${id}-${rand4()}`.toLowerCase(),
              3,
              63,
            );
          }
        }
        batchSubs.add(subdomain);

        // defaults fixos
        const plan = defaultPlan();
        const theme = defaultTheme();
        const address = "";
        const pricePerUser = 0.0;
        const isMaster = false;
        const logo = "default.png";
        const background = "default.png";
        const favicon = "default.png";
        const resaleId = null;
        const omniName = name;

        const v = [
          id,
          name,
          cnpj,
          row.users_allowed,
          status,
          JSON.stringify(plan),
          address,
          pricePerUser,
          isMaster,
          logo,
          JSON.stringify(theme),
          background,
          subdomain,
          omniName,
          favicon,
          resaleId,
          createdAt,
          updatedAt,
        ];
        perRowParams.push(v);

        const base = i * 18;
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}::jsonb, $${base + 7}, $${base + 8}, ` +
            `$${base + 9}, $${base + 10}, $${base + 11}::jsonb, $${base + 12}, $${base + 13}, $${base + 14}, $${base + 15}, ` +
            `$${base + 16}, $${base + 17}, $${base + 18})`,
        );
        values.push(...v);
      });

      try {
        await dest.query("BEGIN");
        await dest.query("SET LOCAL synchronous_commit TO OFF");
        await dest.query(
          `
          INSERT INTO companies (
            id, name, cnpj, users_allowed, status, plan, address, price_per_user,
            is_master, logo, theme, background, subdomain, omni_name, favicon,
            resale_id, created_at, updated_at
          ) VALUES
            ${placeholders.join(",")}
          ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            cnpj = EXCLUDED.cnpj,
            users_allowed = EXCLUDED.users_allowed,
            status = EXCLUDED.status,
            plan = EXCLUDED.plan,
            address = EXCLUDED.address,
            price_per_user = EXCLUDED.price_per_user,
            is_master = EXCLUDED.is_master,
            logo = EXCLUDED.logo,
            theme = EXCLUDED.theme,
            background = EXCLUDED.background,
            subdomain = EXCLUDED.subdomain,
            omni_name = EXCLUDED.omni_name,
            favicon = EXCLUDED.favicon,
            resale_id = EXCLUDED.resale_id,
            updated_at = EXCLUDED.updated_at
          `,
          values,
        );
        await dest.query("COMMIT");

        // consolida unicidade global após sucesso do lote
        for (const s of batchSubs) existingSubdomains.add(s);
        for (const c of Array.from(new Set(perRowParams.map((v) => v[2]))))
          existingCnpjs.add(c);

        migrados += batch.length;
      } catch (batchErr) {
        await dest.query("ROLLBACK");

        // fallback registro a registro
        for (const v of perRowParams) {
          try {
            await dest.query("BEGIN");
            await dest.query("SET LOCAL synchronous_commit TO OFF");
            await dest.query(upsertSqlSingle, v);
            await dest.query("COMMIT");
            migrados += 1;

            existingCnpjs.add(v[2]);
            existingSubdomains.add(v[12]);
          } catch (rowErr) {
            await dest.query("ROLLBACK");
            erros += 1;
            console.error(
              `❌ Erro ao migrar company id=${v[0]}: ${rowErr.message}`,
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

    // ✅ Ajusta sequence do DEST
    try {
      await dest.query(
        `SELECT setval('companies_id_seq', (SELECT COALESCE(MAX(id), 1) FROM companies))`,
      );
    } catch (e) {
      console.warn(
        `⚠️  Não foi possível ajustar sequence companies_id_seq: ${e.message}`,
      );
    }

    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `✅ Migrados ${migrados}/${total} tenant(s) → companies em ${secs}s.${erros ? ` (${erros} com erro)` : ""}`,
    );
  } finally {
    await source.end();
    await dest.end();
  }
};

// —— helpers
function safeName(name, id) {
  const n = (name || "").toString().trim();
  return n.length ? n : `Empresa ${id}`;
}

function normalizeStatus(s) {
  const v = (s || "").toString().toLowerCase().trim();
  return (
    v === "active" ||
    v === "ativo" ||
    v === "ativado" ||
    v === "true" ||
    v === "1" ||
    v === "enabled"
  );
}

function defaultPlan() {
  return {
    Webchat: { price: 0, amount: 0, enabled: false },
    Telegram: { price: 0, amount: 0, enabled: false },
    Instagram: { price: 0, amount: 0, enabled: false },
    Messenger: { price: 0, amount: 0, enabled: false },
    Telefonia: { price: 0, amount: 0, enabled: true },
    WhatsAppQRCode: { price: 0, amount: 0, enabled: false },
    WhatsAppCloudAPI: { price: 0, amount: 0, enabled: false },
  };
}

function defaultTheme() {
  return { primary: "#610659", secondary: "#3a054e" };
}

// pega só dígitos; retorna string 14 se tiver 14, senão null
function normalizeCnpjDigits(v) {
  if (!v) return null;
  const digits = String(v).replace(/\D+/g, "");
  return digits.length === 14 ? digits : null;
}

/**
 * Gera um CNPJ válido (14 dígitos) determinístico baseado no tenant id.
 * - monta os 12 primeiros dígitos (base) usando id+seed
 * - calcula os 2 dígitos verificadores (DV)
 */
function generateDeterministicCnpjFromId(id, seed = 0) {
  // 12 primeiros dígitos:
  // - 8 dígitos base: id + seed (mod 100000000)
  // - 4 dígitos filial: 0001
  const base8 = String((Number(id) + Number(seed)) % 100000000).padStart(
    8,
    "0",
  );
  const branch4 = "0001";
  const base12 = `${base8}${branch4}`;

  const dv1 = calcCnpjDv(base12, 1);
  const dv2 = calcCnpjDv(`${base12}${dv1}`, 2);
  return `${base12}${dv1}${dv2}`;
}

function calcCnpjDv(base, which) {
  // base: 12 (for dv1) or 13 (for dv2) digits string
  const digits = base.split("").map((c) => Number(c));

  const weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2];
  const weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2];
  const weights = which === 1 ? weights1 : weights2;

  let sum = 0;
  for (let i = 0; i < weights.length; i++) {
    sum += digits[i] * weights[i];
  }

  const mod = sum % 11;
  return mod < 2 ? 0 : 11 - mod;
}

function slugSubdomain(name, id) {
  const base = slugSubdomainBase(name);
  if (base.length >= 3) return ensureLength(base, 3, 63);
  return ensureLength(`tenant-${id}`, 3, 63);
}

function slugSubdomainBase(name) {
  let s = (name || "")
    .toString()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  s = s
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return s;
}

function ensureLength(s, min, max) {
  if (s.length < min) s = s.padEnd(min, "x");
  if (s.length > max) s = s.slice(0, max).replace(/-+$/g, "");
  if (s.length < min) s = s.padEnd(min, "x");
  return s;
}

function rand4() {
  return Math.random().toString(36).slice(2, 6);
}

function readBool(v, def = false) {
  if (v == null) return def;
  const s = String(v).trim().toLowerCase();
  return ["1", "true", "t", "yes", "y"].includes(s);
}
