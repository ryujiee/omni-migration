require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateDepartments() {
    console.log('🏢 Migrando "queues" → "departments"...');

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

    const res = await source.query(`
  SELECT
    "id",
    "queue" AS name,
    COALESCE("isActive", true) AS status,
    "tenantId" AS company_id,
    'queue' AS transfer_type,
    COALESCE("inactivity_enabled", false) AS inactivity_active,
    COALESCE("inactivity_timeout", 0) AS inactivity_seconds,
    "inactivity_action" AS inactivity_action,
    "inactivity_target" AS inactivity_target_id,
    "createdAt",
    "updatedAt"
  FROM "public"."Queues"
`);

    for (const row of res.rows) {
        await dest.query(`
      INSERT INTO departments (
        id, name, status, company_id, transfer_type,
        inactivity_active, inactivity_seconds, inactivity_action,
        inactivity_target_id, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, 'queue',
        $5, $6, $7,
        $8, $9, $10
      )
    `, [
            row.id, row.name, row.status, row.company_id,
            row.inactivity_active, row.inactivity_seconds, row.inactivity_action,
            row.inactivity_target, row.createdAt, row.updatedAt
        ]);
    }

    console.log(`✅ Migrados ${res.rowCount} departamentos.`);

    await source.end();
    await dest.end();
};
