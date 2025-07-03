require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateSettings() {
  console.log('⚙️  Migrando "Settings"...');

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
    SELECT DISTINCT "tenantId" AS company_id
    FROM "public"."Settings"
  `);

  let count = 0;

  for (const row of result.rows) {
    await dest.query(`
      INSERT INTO settings (company_id, message_signature, view_chatbot, created_at, updated_at)
      VALUES ($1, $2, $3, NOW(), NOW())
    `, [row.company_id, true, true]);

    count++;
  }

  console.log(`✅ Migradas ${count} configurações únicas.`);

  await source.end();
  await dest.end();
};
