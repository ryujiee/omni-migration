require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateMessages() {
    console.log('💬 Migrando "Messages"...');

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

    const result = await source.query(`SELECT * FROM "public"."Messages"`);

    const oldToNewIdMap = new Map();
    let migrated = 0;
    let reactionCount = 0;

    const get = (row, key) => row[key] ?? row[key.toLowerCase()] ?? null;

    for (const row of result.rows) {
        try {
            // 🛠️ Correção do tipo da mídia
            let mediaType = get(row, 'mediaType') || 'text';
            if (['conversation', 'extendedTextMessage', 'chat'].includes(mediaType)) {
                mediaType = 'text';
            }
            const res = await dest.query(`
        INSERT INTO messages (
          ticket_id, body, edited_body, media_type, media_name, message_id,
          data_json, ack, is_deleted, from_me, user_id, contact_id,
          schedule_date, created_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, '', $5,
          $6, $7, $8, $9, $10, $11,
          $12, $13, $14
        ) RETURNING id
      `, [
                get(row, 'ticketId'),
                get(row, 'body'),
                get(row, 'edited'),
                mediaType,
                get(row, 'messageId') || '',
                get(row, 'dataJson') || '{}',
                String(get(row, 'status') || 'sent'),
                get(row, 'isDeleted') || false,
                get(row, 'fromMe'),
                get(row, 'userId'),
                get(row, 'contactId'),
                get(row, 'scheduleDate'),
                get(row, 'createdAt'),
                get(row, 'updatedAt')
            ]);

            const newId = res.rows[0].id;
            oldToNewIdMap.set(row.id, newId);
            migrated++;

            // Migra reações se houver
            let reactions = [];
            const rawReaction = get(row, 'reaction');

            if (Array.isArray(rawReaction)) {
                reactions = rawReaction;
            } else if (typeof rawReaction === 'string') {
                try {
                    const parsed = JSON.parse(rawReaction);
                    if (Array.isArray(parsed)) {
                        reactions = parsed;
                    } else {
                        console.warn(`⚠️ Ignorando reaction inválida (não é array) na mensagem ${row.id}:`, parsed);
                    }
                } catch (err) {
                    console.warn(`⚠️ JSON inválido no campo reaction da mensagem ${row.id}:`, err.message);
                }
            } else if (typeof rawReaction === 'object' && rawReaction !== null) {
                console.warn(`⚠️ Reaction em formato inesperado (objeto simples) na mensagem ${row.id}:`, rawReaction);
            } else if (typeof rawReaction === 'string' && rawReaction.trim().length > 0) {
                console.warn(`⚠️ Reaction não suportada na mensagem ${row.id}: "${rawReaction}"`);
            }

        } catch (err) {
            console.error(`❌ Erro ao migrar mensagem ID antigo ${row.id}: ${err.message}`);
        }
    }

    // Atualiza mensagens com quotedMsgId após todas migrações
    for (const row of result.rows) {
        const newId = oldToNewIdMap.get(row.id);
        const quotedOld = get(row, 'quotedMsgId');
        const quotedNew = quotedOld ? oldToNewIdMap.get(quotedOld) : null;

        if (!newId || !quotedNew) continue;

        try {
            await dest.query(
                `UPDATE messages SET quoted_msg_id = $1 WHERE id = $2`,
                [quotedNew, newId]
            );
        } catch (err) {
            console.error(`⚠️ Falha ao atualizar quotedMsgId da mensagem ${newId}: ${err.message}`);
        }
    }

    console.log(`✅ Mensagens migradas: ${migrated}`);
    console.log(`✅ Reações migradas: ${reactionCount}`);

    await source.end();
    await dest.end();
};
