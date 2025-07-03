require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateContacts() {
    console.log('📇 Migrando "Contacts"...');

    function truncate(value, maxLength) {
        if (typeof value !== 'string') return value;
        return value.length > maxLength ? value.substring(0, maxLength) : value;
    }


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

    const contacts = await source.query(`
    SELECT * FROM "public"."Contacts"
  `);

    const tagsByContact = await source.query(`
    SELECT "contactId", array_agg("tagId") as tag_ids
    FROM "public"."ContactTags"
    GROUP BY "contactId"
  `);

    const channelsByContact = await source.query(`
    SELECT "contactId", array_agg("channelId") as channel_ids
    FROM "public"."ContactWallets"
    WHERE "channelId" IS NOT NULL
    GROUP BY "contactId"
  `);

    // Mapear por ID
    const tagsMap = Object.fromEntries(tagsByContact.rows.map(r => [r.contactId, r.tag_ids]));
    const channelsMap = Object.fromEntries(channelsByContact.rows.map(r => [r.contactId, r.channel_ids]));

    for (const row of contacts.rows) {
        const contactId = row.id;
        const tags = tagsMap[contactId] || [];
        const channelIds = channelsMap[contactId] || [];

        const channelAssignments = {};
        for (const channelId of channelIds) {
            channelAssignments[channelId] = { assigned: true };
        }

        await dest.query(`
      INSERT INTO contacts (
        id, name, phone_number, profile_pic_url, email,
        is_group, is_wa_contact, telegram, instagram, messenger,
        type, cpf, cnpj, birth_date, address,
        annotations, company_id, created_at, updated_at,
        tags, channel_assignments, push_name
      ) VALUES (
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15,
        $16, $17, $18, $19,
        $20, $21, $22
      )
    `, [
            contactId,
            row.name,
            truncate(row.number, 20),
            row.profilePicUrl,
            row.email,
            row.isGroup,
            row.isWAContact ?? false,
            row.telegramId?.toString() ?? null,
            row.instagramPK?.toString() ?? null,
            row.messengerId,
            row.tipo === 'Pessoa Jurídica' ? 2 : 1,
            row.cpf,
            row.cnpj,
            row.dataNascimento,
            [row.rua, row.bairro, row.cep, row.cidade, row.estado, row.pais].filter(Boolean).join(', ') || '',
            '', // annotations (ignorado razaoSocial)
            row.tenantId,
            row.createdAt,
            row.updatedAt,
            JSON.stringify(tags),
            JSON.stringify(channelAssignments),
            row.pushname
        ]);
    }

    console.log(`✅ Migrados ${contacts.rowCount} contatos.`);

    await source.end();
    await dest.end();
};
