require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateTickets() {
    console.log('🎫 Migrando "Tickets"...');

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

    const result = await source.query(`SELECT * FROM "public"."Tickets"`);

    let migrated = 0;
    console.log('Total buscado:', result.rows.length);
    const get = (row, key) => row[key] ?? row[key.toLowerCase()] ?? null;

    for (const row of result.rows) {
        const closedAtRaw = get(row, 'closedAt');
        const lastMessageAtRaw = get(row, 'lastMessageAt');

        const closedAt = closedAtRaw ? new Date(Number(closedAtRaw)) : null;
        const lastMessageAt = lastMessageAtRaw ? new Date(Number(lastMessageAtRaw)) : null;

        const participants = row.participants ? JSON.stringify(row.participants) : '[]';
        const silenced = row.silenced ? JSON.stringify(row.silenced) : '[]';

        const values = {
            id: get(row, 'id'),
            status: get(row, 'status'),
            lastMessage: get(row, 'lastMessage') || '',
            channelId: get(row, 'whatsappId'),
            contactId: get(row, 'contactId'),
            userId: get(row, 'userId'),
            departmentId: get(row, 'queueId'),
            flowId: null,
            flowStepId: null,
            flowAttempts: 0,
            lastMessageAt,
            closedAt,
            isGroup: get(row, 'isGroup') || false,
            participants,
            silenced,
            companyId: get(row, 'tenantId'),
            createdAt: get(row, 'createdAt'),
            updatedAt: get(row, 'updatedAt')
        };

        try {
            await dest.query(`
        INSERT INTO tickets (
          id, status, last_message, channel_id, contact_id, user_id,
          department_id, flow_id, flow_step_id, flow_attempts,
          last_message_at, closed_at, is_group, participants, silenced,
          company_id, created_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, $6,
          $7, $8, $9, $10,
          $11, $12, $13, $14, $15,
          $16, $17, $18
        )
      `, [
                values.id,
                values.status,
                values.lastMessage,
                values.channelId,
                values.contactId,
                values.userId,
                values.departmentId,
                values.flowId,
                values.flowStepId,
                values.flowAttempts,
                values.lastMessageAt,
                values.closedAt,
                values.isGroup,
                values.participants,
                values.silenced,
                values.companyId,
                values.createdAt,
                values.updatedAt
            ]);

            migrated++;
        } catch (err) {
            console.error(`❌ Erro ao migrar ticket ID ${values.id}: ${err.message}`);
            console.error('📦 Dados do ticket com erro:', values);
        }
    }

    console.log(`✅ Total migrado: ${migrated} tickets.`);

    await source.end();
    await dest.end();
};
