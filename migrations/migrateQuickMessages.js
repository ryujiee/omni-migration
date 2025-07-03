require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateQuickMessages() {
  console.log('⚡ Migrando "FastReply" → "quick_messages"...');

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
      "tenantId" AS company_id,
      name,
      messages,
      "createdAt",
      "updatedAt"
    FROM "public"."FastReply"
  `);

  for (const row of result.rows) {
    await dest.query(`
      INSERT INTO quick_messages (
        id, name, messages, company_id,
        created_at, updated_at
      ) VALUES (
        $1, $2, $3::jsonb, $4,
        $5, $6
      )
    `, [
      row.id,
      row.name || 'Sem nome',
      JSON.stringify(row.messages || []),
      row.company_id,
      row.createdAt,
      row.updatedAt
    ]);
  }

  console.log(`✅ Migradas ${result.rowCount} mensagens rápidas.`);

  await source.end();
  await dest.end();
};
