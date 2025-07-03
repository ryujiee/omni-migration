require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateChannels() {
  console.log('📡 Migrando "whatsapps" → "channels"...');

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
      type,
      "tenantId" AS company_id,
      status,
      number,
      session,
      qrcode,
      "tokenAPI",
      "chatFlowId",
      "queueId",
      "is_open_ia",
      "isDeleted",
      "createdAt",
      "updatedAt"
    FROM "public"."Whatsapps"
  `);

  console.log(`🔍 Total de canais encontrados: ${result.rows.length}`);

  let migrated = 0;

  for (const row of result.rows) {
    const typeMap = {
      whatsapp: 'WhatsAppQRCode',
      waba: 'WhatsAppCloudAPI',
      instagram: 'Instagram',
      telegram: 'Telegram',
      messenger: 'Messenger'
    };

    const channelType = typeMap[row.type] || 'Unknown';

    if (channelType === 'Unknown') {
      console.warn(`⚠️ Tipo desconhecido em canal ID ${row.id}, pulando...`);
      continue;
    }

    const configObj = {};
    if (row.tokenAPI) configObj.tokenAPI = row.tokenAPI;
    const configJson = JSON.stringify(configObj);

    const deletedAt = row.isDeleted ? new Date() : null;

    let flowId = row.chatFlowId;

    // Valida se o flow existe na nova base
    if (flowId) {
      const flowExists = await dest.query(
        'SELECT id FROM flows WHERE id = $1',
        [flowId]
      );

      if (flowExists.rowCount === 0 && row.isDeleted) {
        const fallbackFlow = await dest.query(
          'SELECT id FROM flows WHERE company_id = $1 LIMIT 1',
          [row.company_id]
        );

        if (fallbackFlow.rowCount > 0) {
          flowId = fallbackFlow.rows[0].id;
        } else {
          flowId = null;
        }
      }
    }

    try {
      await dest.query(`
  INSERT INTO channel_instances (
    id, name, type, company_id, status, j_id, session, qr_code, config,
    flow_id, department_id, enable_chatbot_for_groups, open_ticket_for_groups,
    created_at, updated_at, deleted_at
  ) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9,
    $10, $11, $12, $13,
    $14, $15, $16
  )
`, [
        row.id,
        row.name,
        channelType,
        row.company_id,
        row.status || 'disconnected',
        row.number || '',
        row.session || '', // 👈 aqui agora como string normal
        row.qrcode || '',
        configJson,
        flowId,
        row.queueId,
        row.is_open_ia || false,
        row.is_open_ia || false,
        row.createdAt,
        row.updatedAt,
        deletedAt
      ]);

      migrated++;
    } catch (err) {
      console.error(`❌ Erro ao migrar canal ID ${row.id}: ${err.message}`);
      console.error('📦 Canal problemático:', row);
    }
  }

  console.log(`✅ Migrados ${migrated} canais com sucesso.`);
  await source.end();
  await dest.end();
};
