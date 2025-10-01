// scripts/migrate-user-departments.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');

/**
 * ENV esperadas:
 *  # origem (plataforma antiga)
 *  SRC_HOST=... SRC_PORT=5432 SRC_USER=... SRC_PASS=... SRC_DB=...
 *  # destino (plataforma nova)
 *  DST_HOST=... DST_PORT=5432 DST_USER=... DST_PASS=... DST_DB=...
 *
 * Opções (CLI):
 *  --company-id=2            # filtra por tenant/empresa na origem e destino (opcional, mas recomendado)
 *  --match-by=id|email       # como casar usuário origem→destino (default: id)
 *  --json-objects            # grava [{id,name}] ao invés de [id]
 *  --clear-missing           # zera departments=[] para usuários do destino (da company) que não vierem no SRC
 *  --dry-run                 # não grava, só mostra o que faria
 *
 * Exemplos:
 *  node scripts/migrate-user-departments.js --company-id=2
 *  node scripts/migrate-user-departments.js --company-id=2 --match-by=email --json-objects
 */

function arg(name, def = null) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? hit.split('=').slice(1).join('=') : def;
}
function flag(name) {
  return process.argv.includes(`--${name}`);
}

const COMPANY_ID  = arg('company-id');         // opcional
const MATCH_BY    = (arg('match-by', 'id') || 'id').toLowerCase(); // 'id'|'email'
const JSON_OBJECTS = flag('json-objects');     // grava [{id,name}]
const CLEAR_MISSING = flag('clear-missing');   // seta [] pra quem não veio do SRC
const DRY_RUN = flag('dry-run');

function clientFrom(prefix) {
  return new Client({
    host: process.env[`${prefix}_HOST`],
    port: Number(process.env[`${prefix}_PORT`] || 5432),
    user: process.env[`${prefix}_USER`],
    password: process.env[`${prefix}_PASS`],
    database: process.env[`${prefix}_DB`],
    application_name: 'migrate-user-departments'
  });
}

async function toRegClass(db, fqtn) {
  const { rows } = await db.query('SELECT to_regclass($1) AS r', [fqtn]);
  return rows[0]?.r || null;
}

async function tableExists(db, candidates) {
  for (const name of candidates) {
    if (await toRegClass(db, name)) return name;
  }
  return null;
}

async function main() {
  if (!process.env.SRC_HOST || !process.env.DST_HOST) {
    console.error('❌ Configure SRC_* e DST_* no .env');
    process.exit(1);
  }

  const src = clientFrom('SRC');
  const dst = clientFrom('DST');
  await src.connect();
  await dst.connect();

  try {
    // ---- Descobrir tabelas relevantes
    const UQ_TBL = await tableExists(src, ['public."UsersQueues"', 'public.usersqueues', '"UsersQueues"']);
    if (!UQ_TBL) throw new Error('Tabela de origem "UsersQueues" não encontrada.');

    // Para MATCH_BY=email e/ou filtro por COMPANY_ID na origem, precisamos de "Users"
    const SRC_USERS_TBL = await tableExists(src, ['public."Users"', 'public.users', '"Users"']);
    if (MATCH_BY === 'email' && !SRC_USERS_TBL)
      throw new Error('MATCH_BY=email requer tabela de origem "Users" com e-mail.');

    // Checar destino users
    const DST_USERS_TBL =
      (await tableExists(dst, ['public.users', '"users"', 'users'])) ||
      (await tableExists(dst, ['public."Users"', '"Users"', 'Users']));
    if (!DST_USERS_TBL) throw new Error('Tabela de destino "users" não encontrada.');

    // (Opcional) tabelas de nomes dos departamentos
    const NAMES_TBL =
      (await tableExists(src, ['public."Queues"', 'public.queues', '"Queues"'])) ||
      (await tableExists(src, ['public."Departments"', 'public.departments', '"Departments"']));

    console.log('🔎 SRC tables:', { UsersQueues: UQ_TBL, Users: SRC_USERS_TBL || '(não usada)' });
    if (NAMES_TBL) console.log('🔎 SRC names table:', NAMES_TBL);
    console.log('🎯 DST users table:', DST_USERS_TBL);
    if (COMPANY_ID) console.log('🏢 company/tenant filter:', COMPANY_ID);
    console.log('🔗 match-by:', MATCH_BY, '• json-objects:', JSON_OBJECTS, '• clear-missing:', CLEAR_MISSING, '• dry-run:', DRY_RUN);

    // ---- Buscar departamentos por usuário na ORIGEM
    let rows;
    if (MATCH_BY === 'email') {
      const sql = `
        SELECT u."email" AS email,
               u."tenantId" AS company_id,
               array_agg(DISTINCT uq."queueId" ORDER BY uq."queueId") AS dept_ids
        FROM ${UQ_TBL} uq
        JOIN ${SRC_USERS_TBL} u ON u."id" = uq."userId"
        ${COMPANY_ID ? 'WHERE u."tenantId" = $1' : ''}
        GROUP BY u."email", u."tenantId"
      `;
      const res = await src.query(sql, COMPANY_ID ? [COMPANY_ID] : []);
      rows = res.rows; // {email, company_id, dept_ids}
    } else {
      // match-by=id
      // Se filtrar por empresa, ainda precisamos casar com Users da origem pra saber tenantId
      if (COMPANY_ID && SRC_USERS_TBL) {
        const sql = `
          SELECT uq."userId"   AS user_id,
                 u."tenantId"  AS company_id,
                 array_agg(DISTINCT uq."queueId" ORDER BY uq."queueId") AS dept_ids
          FROM ${UQ_TBL} uq
          JOIN ${SRC_USERS_TBL} u ON u."id" = uq."userId"
          WHERE u."tenantId" = $1
          GROUP BY uq."userId", u."tenantId"
        `;
        const res = await src.query(sql, [COMPANY_ID]);
        rows = res.rows; // {user_id, company_id, dept_ids}
      } else {
        const sql = `
          SELECT uq."userId" AS user_id,
                 array_agg(DISTINCT uq."queueId" ORDER BY uq."queueId") AS dept_ids
          FROM ${UQ_TBL} uq
          GROUP BY uq."userId"
        `;
        const res = await src.query(sql);
        rows = res.rows; // {user_id, dept_ids}
      }
    }

    if (!rows.length) {
      console.log('⚠️ Nenhum vínculo usuário↔departamento encontrado na origem com os filtros atuais.');
      return;
    }
    console.log(`📦 Encontrados ${rows.length} usuário(s) com departamentos no SRC.`);

    // ---- Se vamos gravar objetos {id,name}, buscar nomes
    let nameMap = new Map(); // id -> name
    if (JSON_OBJECTS && NAMES_TBL) {
      const { rows: nameRows } = await src.query(`SELECT id, name FROM ${NAMES_TBL}`);
      for (const r of nameRows) nameMap.set(Number(r.id), r.name || `Dept ${r.id}`);
    }

    // ---- Montar payloads para update no destino
    const updates = []; // {dst_id?, email?, company_id?, departments_json}

    if (MATCH_BY === 'email') {
      for (const r of rows) {
        const ids = (r.dept_ids || []).map(Number).filter(n => Number.isFinite(n));
        const departments = JSON_OBJECTS
          ? ids.map(id => ({ id, name: nameMap.get(id) || `Dept ${id}` }))
          : ids;
        updates.push({
          email: r.email,
          company_id: r.company_id != null ? Number(r.company_id) : null,
          departments
        });
      }
    } else {
      for (const r of rows) {
        const ids = (r.dept_ids || []).map(Number).filter(n => Number.isFinite(n));
        const departments = JSON_OBJECTS
          ? ids.map(id => ({ id, name: nameMap.get(id) || `Dept ${id}` }))
          : ids;
        updates.push({
          user_id: Number(r.user_id),
          company_id: r.company_id != null ? Number(r.company_id) : (COMPANY_ID ? Number(COMPANY_ID) : null),
          departments
        });
      }
    }

    // ---- Aplicar updates no DESTINO
    if (DRY_RUN) {
      // mostra alguns exemplos e sai
      console.log('🔎 DRY-RUN — exemplos de atualização:');
      console.log(updates.slice(0, 5));
      console.log(`… total de updates planejados: ${updates.length}`);
      return;
    }

    await dst.query('BEGIN');
    await dst.query('SET LOCAL synchronous_commit TO OFF');

    let ok = 0, miss = 0;

    if (MATCH_BY === 'email') {
      // update por email + (opcional) company_id
      for (const u of updates) {
        const params = [JSON.stringify(u.departments), u.email];
        let where = `WHERE email = $2`;
        if (COMPANY_ID) {
          params.push(Number(COMPANY_ID));
          where += ` AND company_id = $${params.length}`;
        } else if (u.company_id != null) {
          params.push(u.company_id);
          where += ` AND company_id = $${params.length}`;
        }
        const { rowCount } = await dst.query(
          `UPDATE ${DST_USERS_TBL} SET departments = $1 ${where}`, params
        );
        if (rowCount) ok++; else miss++;
      }
    } else {
      // update por id + (opcional) company_id
      for (const u of updates) {
        const params = [JSON.stringify(u.departments), u.user_id];
        let where = `WHERE id = $2`;
        if (COMPANY_ID || u.company_id != null) {
          params.push(Number(COMPANY_ID || u.company_id));
          where += ` AND company_id = $${params.length}`;
        }
        const { rowCount } = await dst.query(
          `UPDATE ${DST_USERS_TBL} SET departments = $1 ${where}`, params
        );
        if (rowCount) ok++; else miss++;
      }
    }

    // (Opcional) limpar quem não veio do SRC
    if (CLEAR_MISSING) {
      if (COMPANY_ID) {
        // zera departments apenas de usuários da company que não bateram no update acima
        // estratégia: marca quem atualizamos em temp table e zera o resto
        await dst.query('CREATE TEMP TABLE tmp_updated (id bigint primary key)');
        if (MATCH_BY === 'email') {
          // marcar ids atualizados via email
          const inEmails = updates.map(u => u.email);
          // dividir em blocos para não estourar placeholders
          const chunk = 1000;
          for (let i = 0; i < inEmails.length; i += chunk) {
            const slice = inEmails.slice(i, i + chunk);
            const ph = slice.map((_, j) => `$${j+1}`).join(',');
            const { rows: ids } = await dst.query(
              `SELECT id FROM ${DST_USERS_TBL} WHERE company_id = $${slice.length+1} AND email IN (${ph})`,
              [...slice, Number(COMPANY_ID)]
            );
            if (ids.length) {
              const ph2 = ids.map((_, j) => `($${j+1})`).join(',');
              await dst.query(`INSERT INTO tmp_updated (id) VALUES ${ph2} ON CONFLICT DO NOTHING`,
                              ids.map(r => r.id));
            }
          }
        } else {
          // marcar ids atualizados via id
          const ids = updates.map(u => u.user_id);
          const chunk = 1000;
          for (let i = 0; i < ids.length; i += chunk) {
            const slice = ids.slice(i, i + chunk);
            const ph = slice.map((_, j) => `$${j+1}`).join(',');
            await dst.query(
              `INSERT INTO tmp_updated (id)
               SELECT id FROM ${DST_USERS_TBL}
               WHERE company_id = $${slice.length+1} AND id IN (${ph})
               ON CONFLICT DO NOTHING`,
               [...slice, Number(COMPANY_ID)]
            );
          }
        }
        // zera os demais da company
        const { rowCount: cleared } = await dst.query(
          `UPDATE ${DST_USERS_TBL}
             SET departments = '[]'::jsonb
           WHERE company_id = $1
             AND id NOT IN (SELECT id FROM tmp_updated)`,
          [Number(COMPANY_ID)]
        );
        console.log(`🧹 CLEAR_MISSING: departments=[] aplicados para ${cleared} usuário(s) sem vínculo no SRC.`);
      } else {
        console.log('ℹ️ CLEAR_MISSING ignorado (requer --company-id).');
      }
    }

    await dst.query('COMMIT');
    console.log(`✅ Atualizados: ${ok} usuário(s). Não encontrados/ignorados: ${miss}.`);

  } catch (e) {
    try { await dst.query('ROLLBACK'); } catch {}
    console.error('❌ Erro:', e.message);
    throw e;
  } finally {
    await src.end();
    await dst.end();
  }
}

main().catch(() => process.exit(1));
