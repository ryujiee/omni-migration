require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateFlows() {
  console.log('🔄 Migrando "ChatFlow" → "flows"...');

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
      id,
      name,
      "tenantId" AS company_id,
      "createdAt",
      "updatedAt"
    FROM "public"."ChatFlow"
    WHERE "isDeleted" IS DISTINCT FROM true
  `);

  for (const row of result.rows) {
    await dest.query(`
      INSERT INTO flows (
        id, name, flow, company_id, created_at, updated_at
      ) VALUES (
        $1, $2, '{}'::jsonb, $3, $4, $5
      )
    `, [
      row.id,
      row.name,
      row.company_id,
      row.createdAt,
      row.updatedAt
    ]);
  }

  console.log(`✅ Migrados ${result.rowCount} fluxos.`);

  await source.end();
  await dest.end();
};
