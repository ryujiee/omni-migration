require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateUsers() {
    console.log('👤 Migrando "users"...');

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

    console.log('📥 Lendo "UsersQueues"...');
    const uqRes = await source.query(`SELECT "userId", "queueId" FROM "public"."UsersQueues"`);

    const userDeptMap = {};
    for (const { userId, queueId } of uqRes.rows) {
        if (!userDeptMap[userId]) userDeptMap[userId] = [];
        userDeptMap[userId].push(queueId);
    }

    console.log('📥 Lendo "Users"...');
    const usersRes = await source.query(`
    SELECT
      "id", "name", "email", "passwordHash", "isInactive",
      "isSupervisor", "supervisedUsers", "profileId", "tenantId",
      "profilePicUrl", "createdAt", "updatedAt"
    FROM "public"."Users"
    WHERE "id" != 1
  `);

    for (const row of usersRes.rows) {
        const departments = userDeptMap[row.id] || [];

        await dest.query(`
            INSERT INTO users (
                id, name, email, password, is_master, status, support,
                is_supervisor, supervised_users, departments, permission_id,
                company_id, avatar_url, email_confirmed, confirmation_token,
                token_expires_at, quick_message_groups, is_api_user, api_token,
                first_access, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, false, $5, false,
                $6, $7, $8, $9,
                $10, $11, true, '', NOW(),
                '[]', false, '', false, $12, $13
            )
            `, [
            row.id,
            row.name,
            row.email,
            row.passwordHash,
            !row.isInactive,
            row.isSupervisor,
            JSON.stringify(row.supervisedUsers || []),
            JSON.stringify(userDeptMap[row.id] || []),
            row.profileId || 1,
            row.tenantId,
            row.profilePicUrl || '',
            row.createdAt,
            row.updatedAt
        ]);
    }

    console.log(`✅ Migrados ${usersRes.rowCount} usuários (exceto ID 1).`);

    await source.end();
    await dest.end();
};
