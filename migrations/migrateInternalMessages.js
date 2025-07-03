require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateInternalMessages() {
  console.log('📨 Migrando "InternalMessages"...');

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

  const result = await source.query(`SELECT * FROM "public"."InternalMessage"`);
  console.log(`📦 Total de mensagens internas encontradas: ${result.rows.length}`);

  const get = (row, key) => row[key] ?? row[key.toLowerCase()] ?? null;

  let migrated = 0;

  for (const row of result.rows) {
    try {
      let mediaType = get(row, 'mediaType') || null;
      if (['conversation', 'extendedTextMessage', 'chat'].includes(mediaType)) {
        mediaType = 'text';
      }

      const ack = get(row, 'read') === true ? 'read' : 'sent';

      await dest.query(`
        INSERT INTO internal_messages (
          id, body, media_type, media_name, media_url, data_json, ack,
          is_deleted, sender_id, recipient_id, group_id, is_group_message,
          created_at, updated_at
        ) VALUES (
          $1, $2, $3, '', $4, '{}', $5,
          false, $6, $7, $8, $9,
          $10, $11
        )
      `, [
        get(row, 'id'),
        get(row, 'text') || '',
        mediaType,
        get(row, 'mediaUrl') || '',
        ack,
        get(row, 'senderId'),
        get(row, 'receiverId'),
        get(row, 'groupId') || null,
        !!get(row, 'groupId'),
        get(row, 'createdAt'),
        get(row, 'updatedAt')
      ]);

      migrated++;
    } catch (err) {
      console.error(`❌ Erro ao migrar mensagem interna ID ${get(row, 'id')}: ${err.message}`);
      console.error('📦 Conteúdo:', row);
    }
  }

  console.log(`✅ Total migrado: ${migrated} mensagens internas.`);

  await source.end();
  await dest.end();
};
