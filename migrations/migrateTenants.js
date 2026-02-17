// migrations/migrateTenants.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateTenants(ctx = {}) {
  console.log('📦 Migrando "Tenants" → "companies"...');

  // 20 params/linha → 2000 linhas ≈ 40k params (< 65535) — seguro
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);
  const INCLUDE_MASTER = readBool(process.env.TENANTS_INCLUDE_MASTER, false); // se true, permite id=1

  // Usa TENANT_ID do ctx (preferencial) ou do .env
  const tenantId =
    ctx.tenantId != null && String(ctx.tenantId).trim() !== ''
      ? String(ctx.tenantId).trim()
      : (process.env.TENANT_ID ? String(process.env.TENANT_ID).trim() : null);

  const source = new Client({
    host: process.env.SRC_HOST,
    port: process.env.SRC_PORT,
    user: process.env.SRC_USER,
    password: process.env.SRC_PASS,
    database: process.env.SRC_DB,
    application_name: 'migrateTenants:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateTenants:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // —— 0) Carrega CNPJs/Subdomains já existentes no DEST pra garantir unicidade
    const existingRes = await dest.query(`SELECT COALESCE(LOWER(subdomain),'') AS subdomain, COALESCE(cnpj,'') AS cnpj FROM companies`);
    const existingSubdomains = new Set(existingRes.rows.map(r => r.subdomain).filter(Boolean));
    const existingCnpjs      = new Set(existingRes.rows.map(r => normalizeCnpj(r.cnpj)).filter(Boolean));

    // —— 1) COUNT para barra/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."Tenants"
      ${tenantId ? 'WHERE "id" = $1' : (INCLUDE_MASTER ? '' : 'WHERE "id" != 1')}
    `;
    const countRes = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(countRes.rows[0]?.total || 0);
    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhum tenant encontrado com ID ${tenantId}.`
          : (INCLUDE_MASTER ? '⚠️  Nenhum tenant encontrado.' : '⚠️  Nenhum tenant elegível encontrado (id != 1).')
      );
      return;
    }

    // —— 2) Cursor server-side
    const selectSql = `
      SELECT
        "id",
        "name",
        "cnpj",
        COALESCE("maxUsers", 0) AS users_allowed,
        "status",
        "createdAt",
        "updatedAt"
      FROM "public"."Tenants"
      ${tenantId ? 'WHERE "id" = $1' : (INCLUDE_MASTER ? '' : 'WHERE "id" != 1')}
      ORDER BY "id"
    `;
    const cursor = source.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    // —— 3) Barra de progresso
    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    // —— 4) UPSERT single (fallback)
    const upsertSqlSingle = `
      INSERT INTO companies (
        id, name, cnpj, users_allowed, status, plan, address, price_per_user,
        discount, is_master, logo, theme, background, subdomain, omni_name, favicon,
        modules, resale_id, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6::jsonb, $7, $8,
        $9::jsonb, $10, $11, $12::jsonb, $13, $14, $15, $16,
        $17::jsonb, $18, $19, $20
      )
      ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        cnpj = EXCLUDED.cnpj,
        users_allowed = EXCLUDED.users_allowed,
        status = EXCLUDED.status,
        plan = EXCLUDED.plan,
        address = EXCLUDED.address,
        price_per_user = EXCLUDED.price_per_user,
        discount = EXCLUDED.discount,
        is_master = EXCLUDED.is_master,
        logo = EXCLUDED.logo,
        theme = EXCLUDED.theme,
        background = EXCLUDED.background,
        subdomain = EXCLUDED.subdomain,
        omni_name = EXCLUDED.omni_name,
        favicon = EXCLUDED.favicon,
        modules = EXCLUDED.modules,
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

      // conjuntos de unicidade intra-batch (evita colisões dentro do lote)
      const batchCnpjs = new Set();
      const batchSubs  = new Set();

      batch.forEach((row, i) => {
        const id = row.id;
        const name = safeName(row.name, id);
        const status = normalizeStatus(row.status); // 'active'/'inactive' → boolean
        const createdAt = row.createdAt;
        const updatedAt = row.updatedAt;

        // CNPJ — normaliza e garante unicidade global (dest + batch)
        let cnpj = normalizeCnpj(row.cnpj);
        if (!cnpj) cnpj = generateFakeCnpj();
        while (existingCnpjs.has(cnpj) || batchCnpjs.has(cnpj)) {
          cnpj = generateFakeCnpj();
        }
        batchCnpjs.add(cnpj);

        // subdomain — slug + unicidade global (dest + batch)
        let subdomain = slugSubdomain(name, id);
        while (existingSubdomains.has(subdomain) || batchSubs.has(subdomain)) {
          // tenta um sufixo determinístico pelo id
          subdomain = ensureLength(`${slugSubdomain(name)}-${id}`.toLowerCase(), 3, 63);
          // se ainda colidir (teoricamente raro), acrescenta rand curto
          if (existingSubdomains.has(subdomain) || batchSubs.has(subdomain)) {
            subdomain = ensureLength(`${slugSubdomain(name)}-${id}-${rand4()}`.toLowerCase(), 3, 63);
          }
        }
        batchSubs.add(subdomain);

        // defaults fixos
        const plan = defaultPlan();
        const discount = defaultDiscount();
        const modules = defaultModules();
        const theme = defaultTheme();
        const address = '';
        const pricePerUser = 0.0;
        const isMaster = false;
        const logo = 'default.png';
        const background = 'default.png';
        const favicon = 'default.png';
        const resaleId = null;
        const omniName = name;

        const v = [
          id, name, cnpj, row.users_allowed, status, JSON.stringify(plan), address, pricePerUser,
          JSON.stringify(discount), isMaster, logo, JSON.stringify(theme), background, subdomain, omniName, favicon,
          JSON.stringify(modules), resaleId, createdAt, updatedAt
        ];
        perRowParams.push(v);

        const base = i * 20;
        placeholders.push(
          `($${base+1}, $${base+2}, $${base+3}, $${base+4}, $${base+5}, $${base+6}::jsonb, $${base+7}, $${base+8}, ` +
          `$${base+9}::jsonb, $${base+10}, $${base+11}, $${base+12}::jsonb, $${base+13}, $${base+14}, $${base+15}, $${base+16}, ` +
          `$${base+17}::jsonb, $${base+18}, $${base+19}, $${base+20})`
        );
        values.push(...v);
      });

      // Execução do lote
      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(
          `
          INSERT INTO companies (
            id, name, cnpj, users_allowed, status, plan, address, price_per_user,
            discount, is_master, logo, theme, background, subdomain, omni_name, favicon,
            modules, resale_id, created_at, updated_at
          ) VALUES
            ${placeholders.join(',')}
          ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            cnpj = EXCLUDED.cnpj,
            users_allowed = EXCLUDED.users_allowed,
            status = EXCLUDED.status,
            plan = EXCLUDED.plan,
            address = EXCLUDED.address,
            price_per_user = EXCLUDED.price_per_user,
            discount = EXCLUDED.discount,
            is_master = EXCLUDED.is_master,
            logo = EXCLUDED.logo,
            theme = EXCLUDED.theme,
            background = EXCLUDED.background,
            subdomain = EXCLUDED.subdomain,
            omni_name = EXCLUDED.omni_name,
            favicon = EXCLUDED.favicon,
            modules = EXCLUDED.modules,
            resale_id = EXCLUDED.resale_id,
            updated_at = EXCLUDED.updated_at
          `,
          values
        );
        await dest.query('COMMIT');

        // consolida unicidade global após sucesso do lote
        for (const s of batchSubs) existingSubdomains.add(s);
        for (const c of Array.from(new Set(
          perRowParams.map(v => v[2]) // cnpj final usado no insert
        ))) {
          existingCnpjs.add(c);
        }

        migrados += placeholders.length;
      } catch (batchErr) {
        await dest.query('ROLLBACK');
        // fallback: registro a registro
        for (const v of perRowParams) {
          try {
            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');
            await dest.query(upsertSqlSingle, v);
            await dest.query('COMMIT');
            migrados += 1;

            // consolida unicidade global após sucesso individual
            existingCnpjs.add(v[2]);        // cnpj
            existingSubdomains.add(v[13]);  // subdomain
          } catch (rowErr) {
            await dest.query('ROLLBACK');
            erros += 1;
            console.error(`❌ Erro ao migrar company id=${v[0]}: ${rowErr.message}`);
          }
        }
      }

      processed += batch.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = (processed / Math.max(1, elapsed)).toFixed(1);
      bar.update(processed, { rate });
    }

    bar.stop();
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`✅ Migrados ${migrados}/${total} tenant(s) → companies em ${secs}s.${erros ? ` (${erros} com erro)` : ''}`);
  } finally {
    await source.end();
    await dest.end();
  }
};

// —— helpers
function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Empresa ${id}`;
}
function normalizeStatus(s) {
  const v = (s || '').toString().toLowerCase().trim();
  return v === 'active' || v === 'ativo' || v === 'ativado' || v === 'true' || v === '1';
}
function defaultPlan() {
  return {
    Webchat:        { price: 0, amount: 999, enabled: false },
    Telegram:       { price: 0, amount: 999, enabled: false },
    Instagram:      { price: 0, amount: 999, enabled: false },
    Messenger:      { price: 0, amount: 999, enabled: false },
    Telefonia:      { price: 0, amount: 999, enabled: true  },
    WhatsAppQRCode: { price: 0, amount: 999, enabled: false },
    WhatsAppAPI:    { price: 0, amount: 999, enabled: false }
  };
}
function defaultDiscount() {
  return {
    user: 0,
    Telefonia: 0,
    WhatsAppQRCode: 0,
    Telegram: 0,
    Instagram: 0,
    WhatsAppAPI: 0,
    Webchat: 0,
    Messenger: 0
  };
}
function defaultModules() {
  return {
    support_tickets: {
      enabled: false,
      price: 0
    }
  };
}
function defaultTheme() {
  return {
    primary: '#1976d2',
    secondary: '#42a5f5',
    light: { primary: '#1976d2', secondary: '#42a5f5', logo: 'default.png', background: 'default.png', favicon: 'default.png' },
    dark: { primary: '#1976d2', secondary: '#42a5f5', logo: 'default.png', background: 'default.png', favicon: 'default.png' },
    useSamePrimaryColor: true,
    useSameSecondaryColor: true,
    useSameLogo: true,
    useSameBackground: true,
    useSameFavicon: true
  };
}

function normalizeCnpj(v) {
  if (!v) return null;
  const digits = String(v).replace(/\D+/g, '');
  // aceita apenas CNPJ "cheio" (14 dígitos). Se quiser validar dígitos verificadores, dá pra plugar
  return digits.length === 14 ? digits : null;
}
function generateFakeCnpj() {
  // "FAKE" + 12 dígitos (texto) — compatível com dest tipo TEXT/VARCHAR
  const rnd = Math.floor(Math.random() * 1e12).toString().padStart(12, '0');
  return `FAKE${rnd}`;
}

function slugSubdomain(name, id) {
  const base = slugSubdomainBase(name);
  if (base.length >= 3) return ensureLength(base, 3, 63);
  return ensureLength(`tenant-${id}`, 3, 63);
}
function slugSubdomainBase(name) {
  let s = (name || '').toString().normalize('NFD').replace(/[\u0300-\u036f]/g, ''); // remove acentos
  s = s.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
  return s;
}
function ensureLength(s, min, max) {
  if (s.length < min) s = s.padEnd(min, 'x');
  if (s.length > max) s = s.slice(0, max).replace(/-+$/g, ''); // evita terminar com '-'
  if (s.length < min) s = s.padEnd(min, 'x');
  return s;
}
function rand4() {
  return Math.random().toString(36).slice(2, 6);
}
function readBool(v, def=false) {
  if (v == null) return def;
  const s = String(v).trim().toLowerCase();
  return ['1','true','t','yes','y'].includes(s);
}
