require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateCampaignContacts() {
  console.log('👥 Migrando "CampaignContacts" → "campaign_contacts"...');

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
      "campaignId",
      "contactId",
      ack,
      "timestamp",
      "createdAt",
      "updatedAt"
    FROM "public"."CampaignContacts"
  `);

  for (const row of result.rows) {
    let status = 'pending';
    let responded = false;
    let respondedAt = null;

    if (row.ack >= 1) status = 'sent';
    if (row.ack === -1) status = 'error';

    // Se tiver timestamp (resposta registrada), considerar como respondido
    if (row.timestamp && row.timestamp !== 0) {
      responded = true;
      respondedAt = new Date(row.timestamp * 1000); // timestamp em segundos
    }

    await dest.query(`
      INSERT INTO campaign_contacts (
        id, campaign_id, contact_id, status, error_msg, responded,
        responded_at, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, NULL, $5,
        $6, $7, $8
      )
    `, [
      row.id,
      row.campaignId,
      row.contactId,
      status,
      responded,
      respondedAt,
      row.createdAt,
      row.updatedAt
    ]);
  }

  console.log(`✅ Migrados ${result.rowCount} contatos de campanha.`);

  await source.end();
  await dest.end();
};
