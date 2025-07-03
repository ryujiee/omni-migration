require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateTenants() {
  console.log('📦 Migrando "Tenants" → "companies"...');

  const source = new Client({
    host: process.env.SRC_HOST,
    port: process.env.SRC_PORT,
    user: process.env.SRC_USER,
    password: process.env.SRC_PASS,
    database: process.env.SRC_DB,
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
  });

  await source.connect();
  await dest.connect();

 const result = await source.query(`
  SELECT
    "id",
    "name",
    "cnpj",
    COALESCE("maxUsers", 0) AS users_allowed,
    CASE WHEN "status" = 'active' THEN true ELSE false END AS status,
    '{
      "Webchat": {"price": 0, "amount": 0, "enabled": false},
      "Telegram": {"price": 0, "amount": 0, "enabled": false},
      "Instagram": {"price": 0, "amount": 0, "enabled": false},
      "Messenger": {"price": 0, "amount": 0, "enabled": false},
      "Telefonia": {"price": 0, "amount": 0, "enabled": true},
      "WhatsAppQRCode": {"price": 0, "amount": 0, "enabled": false},
      "WhatsAppCloudAPI": {"price": 0, "amount": 0, "enabled": false}
    }'::jsonb AS plan,
    '' AS address,
    0.0 AS price_per_user,
    false AS is_master,
    'default.png' AS logo,
    '{"primary":"#1976d2","secondary":"#42a5f5"}'::jsonb AS theme,
    'default.png' AS background,
    LOWER(REPLACE("name", ' ', '_')) AS subdomain,
    "name" AS omni_name,
    'default.png' AS favicon,
    NULL::int AS resale_id,
    "createdAt",
    "updatedAt"
  FROM "public"."Tenants"
  WHERE "id" != 1
`);

  for (const row of result.rows) {
    await dest.query(
      `INSERT INTO companies (
        id, name, cnpj, users_allowed, status, plan, address, price_per_user,
        is_master, logo, theme, background, subdomain, omni_name, favicon,
        resale_id, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8,
        $9, $10, $11, $12, $13, $14, $15,
        $16, $17, $18
      )`,
      [
        row.id, row.name, row.cnpj, row.users_allowed, row.status, row.plan, row.address,
        row.price_per_user, row.is_master, row.logo, row.theme, row.background, row.subdomain,
        row.omni_name, row.favicon, row.resale_id, row.createdAt, row.updatedAt
      ]
    );
  }

  console.log(`✅ Migrados ${result.rowCount} tenants → companies.`);

  await source.end();
  await dest.end();
};
