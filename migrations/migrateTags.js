require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateTags() {
  console.log('🏷️ Migrando "Tags"...');

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
      "tag" AS name,
      "color",
      "isActive" AS active,
      "tenantId" AS company_id,
      "createdAt",
      "updatedAt"
    FROM "public"."Tags"
  `);

  for (const row of result.rows) {
    await dest.query(`
      INSERT INTO tags (
        id, name, color, active, company_id,
        created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5,
        $6, $7
      )
    `, [
      row.id,
      row.name,
      row.color,
      row.active,
      row.company_id,
      row.createdAt,
      row.updatedAt
    ]);
  }

  console.log(`✅ Migradas ${result.rowCount} tags.`);

  await source.end();
  await dest.end();
};
