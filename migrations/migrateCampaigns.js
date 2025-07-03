require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateCampaigns() {
  console.log('📢 Migrando "Campaigns"...');

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
      start,
      status,
      "sessionId" AS channel_id,
      message1,
      message2,
      message3,
      "mediaUrl",
      delay,
      "tenantId" AS company_id,
      "createdAt",
      "updatedAt"
    FROM "public"."Campaigns"
  `);

  for (const row of result.rows) {
    const messages = [
      row.message1,
      row.message2,
      row.message3
    ].filter(msg => !!msg); // remove mensagens vazias

    await dest.query(`
      INSERT INTO campaigns (
        id, name, start_at, status, delay_seconds,
        messages, media_path, template_id,
        company_id, channel_id, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5,
        $6::jsonb, $7, NULL,
        $8, $9, $10, $11
      )
    `, [
      row.id,
      row.name,
      row.start,
      normalizeStatus(row.status),
      row.delay,
      JSON.stringify(messages),
      row.mediaUrl || null,
      row.company_id,
      row.channel_id || 1, // fallback para canal 1 se nulo
      row.createdAt,
      row.updatedAt
    ]);
  }

  console.log(`✅ Migradas ${result.rowCount} campanhas.`);

  await source.end();
  await dest.end();
};

// Função opcional para normalizar status da campanha
function normalizeStatus(oldStatus) {
  switch (oldStatus) {
    case 'pending':
      return 'scheduled';
    case 'in_progress':
      return 'running';
    case 'completed':
      return 'finished';
    case 'canceled':
    case 'cancelled':
      return 'canceled';
    default:
      return 'scheduled';
  }
}
