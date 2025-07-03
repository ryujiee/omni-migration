require('dotenv').config();
const { Client } = require('pg');

module.exports = async function migrateTasks() {
  console.log('📝 Migrando "TodoLists" → "tasks"...');

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

  // Busca todos os tipos de tarefa da base destino
  const typesRes = await dest.query(`SELECT id, name, company_id FROM task_types`);
  const taskTypeMap = new Map();

  for (const type of typesRes.rows) {
    const key = `${type.company_id}|${type.name.trim().toLowerCase()}`;
    taskTypeMap.set(key, type.id);
  }

  const result = await source.query(`
    SELECT
      id,
      "tenantId" AS company_id,
      "ownerId" AS created_by_id,
      "userId" AS assigned_to_id,
      name,
      description,
      type,
      "createdAt",
      "limitDate" AS due_date,
      priority,
      status,
      comments,
      "updatedAt"
    FROM "public"."TodoLists"
  `);

  let ignoradas = 0;

  for (const row of result.rows) {
    const typeKey = `${row.company_id}|${(row.type || '').trim().toLowerCase()}`;
    const task_type_id = taskTypeMap.get(typeKey);

    if (!task_type_id) {
      console.warn(`⚠️ Tipo de tarefa '${row.type}' não encontrado para empresa ${row.company_id}, tarefa '${row.name}' ignorada.`);
      ignoradas++;
      continue;
    }

    let extraInfo = [];
    if (row.comments && row.comments.trim() !== '') {
      extraInfo.push({
        title: 'Comentário Migrado',
        content: row.comments,
        required: false
      });
    }

    await dest.query(`
      INSERT INTO tasks (
        id, company_id, created_by_id, assigned_to_id, name, description,
        task_type_id, created_at, due_date, priority, status, extra_info, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6,
        $7, $8, $9, $10, $11, $12, $13
      )
    `, [
      row.id,
      row.company_id,
      row.created_by_id,
      row.assigned_to_id,
      row.name,
      row.description,
      task_type_id,
      row.createdAt,
      row.due_date,
      row.priority,
      row.status,
      JSON.stringify(extraInfo),
      row.updatedAt
    ]);
  }

  console.log(`✅ Migradas ${result.rowCount - ignoradas} tarefas. (${ignoradas} ignoradas por tipo não encontrado)`);

  await source.end();
  await dest.end();
};
