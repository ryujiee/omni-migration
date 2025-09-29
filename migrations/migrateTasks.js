// migrations/migrateTasks.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateTasks(ctx = {}) {
  console.log('📝 Migrando "TodoLists" → "tasks"...');

  // 13 params/linha → 2500 linhas ≈ 32.5k params (< 65535)
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2500);
  const USE_FALLBACK_TYPE = readBool(process.env.TASKS_FALLBACK_TYPE, true);

  // Usa TENANT_ID do ctx (preferencial) ou do .env
  const tenantId =
    ctx.tenantId != null && String(ctx.tenantId).trim() !== ''
      ? String(ctx.tenantId).trim()
      : (process.env.TENANT_ID ? String(process.env.TENANT_ID).trim() : null);

  const source = new Client({
    host: process.env.SRC_HOST,
    port: process.env.SRC_PORT,
    user: process.env.SRC_USER,
    password: process.env.SRC_PASS,
    database: process.env.SRC_DB,
    application_name: 'migrateTasks:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateTasks:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // —— 1) Carrega tipos e fallback de tipos no destino
    const typesSql = tenantId
      ? `SELECT id, name, company_id FROM task_types WHERE company_id = $1`
      : `SELECT id, name, company_id FROM task_types`;
    const typesParams = tenantId ? [tenantId] : [];
    const { rows: typesRows } = await dest.query(typesSql, typesParams);

    const taskTypeMap = new Map(); // key: company_id|lower(name) -> id
    for (const t of typesRows) {
      const key = `${t.company_id}|${(t.name || '').trim().toLowerCase()}`;
      taskTypeMap.set(key, t.id);
    }

    const fallbackTypeByCompany = new Map(
      (await dest.query(
        tenantId
          ? 'SELECT company_id, MIN(id) AS type_id FROM task_types WHERE company_id = $1 GROUP BY company_id'
          : 'SELECT company_id, MIN(id) AS type_id FROM task_types GROUP BY company_id',
        tenantId ? [tenantId] : []
      )).rows.map(r => [String(r.company_id), r.type_id])
    );

    // —— 2) COUNT para barra/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."TodoLists"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
    `;
    const { rows: crows } = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(crows[0]?.total || 0);
    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhuma tarefa encontrada para TENANT_ID=${tenantId}.`
          : '⚠️  Nenhuma tarefa encontrada na origem.'
      );
      return;
    }

    // —— 3) Cursor server-side (ordem estável)
    const selectSql = `
      SELECT
        id,
        "tenantId"   AS company_id,
        "ownerId"    AS created_by_id,
        "userId"     AS assigned_to_id,
        name,
        description,
        type,
        "createdAt",
        "limitDate"  AS due_date,
        priority,
        status,
        comments,
        "updatedAt"
      FROM "public"."TodoLists"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
      ORDER BY id
    `;
    const cursor = source.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    // —— 4) Barra de progresso
    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    // —— 5) UPSERT single (fallback)
    const upsertSqlSingle = `
      INSERT INTO tasks (
        id, company_id, created_by_id, assigned_to_id, name, description,
        task_type_id, created_at, due_date, priority, status, extra_info, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6,
        $7, $8, $9, $10, $11, $12, $13
      )
      ON CONFLICT (id) DO UPDATE SET
        company_id     = EXCLUDED.company_id,
        created_by_id  = EXCLUDED.created_by_id,
        assigned_to_id = EXCLUDED.assigned_to_id,
        name           = EXCLUDED.name,
        description    = EXCLUDED.description,
        task_type_id   = EXCLUDED.task_type_id,
        due_date       = EXCLUDED.due_date,
        priority       = EXCLUDED.priority,
        status         = EXCLUDED.status,
        extra_info     = EXCLUDED.extra_info,
        updated_at     = EXCLUDED.updated_at
    `;

    const startedAt = Date.now();
    let processed = 0;
    let migradas = 0;
    let ignoradasTipo = 0;
    let erros = 0;

    // —— 6) Loop por lote
    while (true) {
      const batch = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!batch || batch.length === 0) break;

      const placeholders = [];
      const values = [];
      const perRowParams = [];

      for (let i = 0; i < batch.length; i++) {
        const row = batch[i];
        const typeKey = `${row.company_id}|${(row.type || '').trim().toLowerCase()}`;
        let task_type_id = taskTypeMap.get(typeKey) || null;

        if (!task_type_id && USE_FALLBACK_TYPE) {
          const fb = fallbackTypeByCompany.get(String(row.company_id));
          if (fb) task_type_id = fb;
        }

        if (!task_type_id) {
          ignoradasTipo++;
          continue;
        }

        const extraInfo = [];
        if (row.comments && String(row.comments).trim() !== '') {
          extraInfo.push({
            title: 'Comentário Migrado',
            content: String(row.comments),
            required: false
          });
        }

        const v = [
          row.id,                               // 1  id
          row.company_id,                       // 2  company_id
          row.created_by_id || null,            // 3  created_by_id
          row.assigned_to_id || null,           // 4  assigned_to_id
          safeName(row.name, row.id),           // 5  name
          row.description || '',                // 6  description
          task_type_id,                         // 7  task_type_id
          row.createdAt,                        // 8  created_at
          row.due_date || null,                 // 9  due_date
          toInt(row.priority, null),            // 10 priority
          row.status || null,                   // 11 status
          JSON.stringify(extraInfo),            // 12 extra_info::jsonb
          row.updatedAt                         // 13 updated_at
        ];
        perRowParams.push(v);

        const base = placeholders.length * 13;
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12}::jsonb, $${base + 13})`
        );
        values.push(...v);
      }

      if (placeholders.length === 0) {
        processed += batch.length;
        const elapsed = (Date.now() - startedAt) / 1000;
        bar.update(processed, { rate: (processed / Math.max(1, elapsed)).toFixed(1) });
        continue;
      }

      // —— 7) Execução do lote
      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(
          `
          INSERT INTO tasks (
            id, company_id, created_by_id, assigned_to_id, name, description,
            task_type_id, created_at, due_date, priority, status, extra_info, updated_at
          ) VALUES
            ${placeholders.join(',')}
          ON CONFLICT (id) DO UPDATE SET
            company_id     = EXCLUDED.company_id,
            created_by_id  = EXCLUDED.created_by_id,
            assigned_to_id = EXCLUDED.assigned_to_id,
            name           = EXCLUDED.name,
            description    = EXCLUDED.description,
            task_type_id   = EXCLUDED.task_type_id,
            due_date       = EXCLUDED.due_date,
            priority       = EXCLUDED.priority,
            status         = EXCLUDED.status,
            extra_info     = EXCLUDED.extra_info,
            updated_at     = EXCLUDED.updated_at
          `,
          values
        );
        await dest.query('COMMIT');
        migradas += placeholders.length;
      } catch (batchErr) {
        await dest.query('ROLLBACK');
        // fallback: registro a registro
        for (const v of perRowParams) {
          try {
            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');
            await dest.query(upsertSqlSingle, v);
            await dest.query('COMMIT');
            migradas += 1;
          } catch (rowErr) {
            await dest.query('ROLLBACK');
            erros += 1;
            console.error(`❌ Erro ao migrar task id=${v[0]}: ${rowErr.message}`);
          }
        }
      }

      processed += batch.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = (processed / Math.max(1, elapsed)).toFixed(1);
      bar.update(processed, { rate });
    }

    bar.stop();
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `✅ Migradas ${migradas}/${total} tarefa(s) em ${secs}s. `
      + (ignoradasTipo ? `(${ignoradasTipo} ignoradas por tipo ausente)` : '')
      + (erros ? `, ${erros} com erro` : '')
    );
  } finally {
    await source.end();
    await dest.end();
  }
};

// —— helpers
function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Tarefa ${id}`;
}
function toInt(v, def = null) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : def;
}
function readBool(v, def = false) {
  if (v == null) return def;
  const s = String(v).trim().toLowerCase();
  return ['1', 'true', 't', 'yes', 'y'].includes(s);
}
