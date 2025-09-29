// migrations/migrateUsers.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateUsers(ctx = {}) {
  console.log('👤 Migrando "Users" → "users"...');

  // 22 params/linha → 2000 linhas ≈ 44k params (< 65535)
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);

  // Se true: quando passwordHash vier vazio, marca first_access=true
  const FIRST_ACCESS_IF_NO_PASSWORD = readBool(process.env.USERS_FIRST_ACCESS_IF_NO_PASSWORD, true);

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
    application_name: 'migrateUsers:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateUsers:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // —— 0) Pré-carrega FKs leves do DESTINO (por empresa) para filtrar/normalizar rápido
    const whereCompany = tenantId ? 'WHERE company_id = $1' : '';
    const paramsCompany = tenantId ? [tenantId] : [];

    const [deptsRes, permsRes] = await Promise.all([
      dest.query(`SELECT id, company_id FROM departments ${whereCompany}`, paramsCompany),
      dest.query(`SELECT id, company_id FROM permissions ${whereCompany}`, paramsCompany),
    ]);

    const deptsByCompany = idsByCompany(deptsRes.rows);
    const firstPermByCompany = new Map();
    for (const r of permsRes.rows) {
      const k = String(r.company_id);
      if (!firstPermByCompany.has(k)) firstPermByCompany.set(k, r.id);
    }

    // —— 1) Carrega mapeamento Users → Queues (departamentos) da origem (filtrado por tenant quando aplicável)
    console.log('📥 Lendo "UsersQueues"...');
    let uqQuery, uqParams;
    if (tenantId) {
      uqQuery = `
        SELECT uq."userId" AS user_id, uq."queueId" AS department_id
        FROM "public"."UsersQueues" uq
        JOIN "public"."Queues" q ON q."id" = uq."queueId"
        WHERE q."tenantId" = $1
      `;
      uqParams = [tenantId];
    } else {
      uqQuery = `SELECT "userId" AS user_id, "queueId" AS department_id FROM "public"."UsersQueues"`;
      uqParams = [];
    }
    const uqRes = await source.query(uqQuery, uqParams);
    const userDeptMap = groupIds(uqRes.rows, 'user_id', 'department_id'); // { userId: [deptId,...] }

    // —— 2) COUNT de usuários (exceto id=1) para progresso/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."Users"
      WHERE "id" != 1
      ${tenantId ? 'AND "tenantId" = $1' : ''}
    `;
    const { rows: crows } = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(crows[0]?.total || 0);
    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhum usuário encontrado para TENANT_ID=${tenantId} (exceto ID 1).`
          : '⚠️  Nenhum usuário encontrado na origem (exceto ID 1).'
      );
      return;
    }

    // —— 3) Cursor server-side de usuários (estável)
    console.log('📥 Lendo "Users"...');
    const selectSql = `
      SELECT
        "id", "name", "email", "passwordHash", "isInactive",
        "isSupervisor", "supervisedUsers", "profileId", "tenantId",
        "profilePicUrl", "createdAt", "updatedAt"
      FROM "public"."Users"
      WHERE "id" != 1
      ${tenantId ? 'AND "tenantId" = $1' : ''}
      ORDER BY "id"
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
      INSERT INTO users (
        id, name, email, password, is_master, status, support,
        is_supervisor, supervised_users, departments, permission_id,
        company_id, avatar_url, email_confirmed, confirmation_token,
        token_expires_at, quick_message_groups, is_api_user, api_token,
        first_access, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, false, $5, false,
        $6, $7::jsonb, $8::jsonb, $9,
        $10, $11, true, '', $12,
        '[]'::jsonb, false, '',
        $13, $14, $15
      )
      ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        email = EXCLUDED.email,
        password = EXCLUDED.password,
        status = EXCLUDED.status,
        support = EXCLUDED.support,
        is_supervisor = EXCLUDED.is_supervisor,
        supervised_users = EXCLUDED.supervised_users,
        departments = EXCLUDED.departments,
        permission_id = EXCLUDED.permission_id,
        company_id = EXCLUDED.company_id,
        avatar_url = EXCLUDED.avatar_url,
        email_confirmed = EXCLUDED.email_confirmed,
        updated_at = EXCLUDED.updated_at
    `;

    const startedAt = Date.now();
    let processed = 0;
    let migrados = 0;
    let erros = 0;

    // —— 6) Loop por lote
    while (true) {
      const batch = await readCursor(cursor, BATCH_SIZE);
      if (!batch.length) break;

      const placeholders = [];
      const values = [];
      const perRowParams = [];

      for (let i = 0; i < batch.length; i++) {
        const row = batch[i];
        const companyId = row.tenantId;
        const companyKey = String(companyId);

        const name = safeName(row.name, row.id);
        const email = safeEmail(row.email, row.id, companyId);
        const pass = row.passwordHash || '';

        const status = !row.isInactive;
        const isSupervisor = !!row.isSupervisor;
        const avatar = row.profilePicUrl || '';

        // supervisedUsers pode vir array, json string ou null
        const supervised = asArray(row.supervisedUsers)
          .map(n => toInt(n, null))
          .filter(n => Number.isInteger(n) && n !== row.id); // evita self

        // departments do UsersQueues filtrados por company + existência no destino
        const rawDepts = asArray(userDeptMap[row.id] || [])
          .map(n => toInt(n, null))
          .filter(Number.isInteger);
        const deptSet = deptsByCompany.get(companyKey);
        const departments = deptSet
          ? rawDepts.filter(d => deptSet.has(d))
          : [];

        // permission_id: tenta usar profileId se existir no destino; senão primeiro da empresa; senão 1
        let permissionId = toInt(row.profileId, null);
        if (permissionId == null || !firstPermByCompany.has(companyKey)) {
          permissionId = firstPermByCompany.get(companyKey) ?? 1;
        }

        // token_expires_at = agora (mesma semântica do seu NOW())
        const tokenExpiresAt = new Date();

        // first_access: opcional true se sem senha
        const firstAccess = FIRST_ACCESS_IF_NO_PASSWORD && !pass;

        const createdAt = row.createdAt;
        const updatedAt = row.updatedAt;

        const v = [
          row.id,               // 1  id
          name,                 // 2  name
          email,                // 3  email
          pass,                 // 4  password
          status,               // 5  status
          isSupervisor,         // 6  is_supervisor
          JSON.stringify(supervised),   // 7  supervised_users::jsonb
          JSON.stringify(departments),  // 8  departments::jsonb
          permissionId,         // 9  permission_id
          companyId,            // 10 company_id
          avatar,               // 11 avatar_url
          tokenExpiresAt,       // 12 token_expires_at
          firstAccess,          // 13 first_access
          createdAt,            // 14 created_at
          updatedAt             // 15 updated_at
        ];
        perRowParams.push(v);

        const base = i * 15;
        placeholders.push(
          `($${base+1}, $${base+2}, $${base+3}, $${base+4}, false, $${base+5}, false, ` +
          `$${base+6}, $${base+7}::jsonb, $${base+8}::jsonb, $${base+9}, ` +
          `$${base+10}, $${base+11}, true, '', $${base+12}, ` +
          `'[]'::jsonb, false, '', $${base+13}, $${base+14}, $${base+15})`
        );
        values.push(...v);
      }

      // Execução do lote (multi-VALUES)
      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(
          `
          INSERT INTO users (
            id, name, email, password, is_master, status, support,
            is_supervisor, supervised_users, departments, permission_id,
            company_id, avatar_url, email_confirmed, confirmation_token,
            token_expires_at, quick_message_groups, is_api_user, api_token,
            first_access, created_at, updated_at
          ) VALUES
            ${placeholders.join(',')}
          ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            password = EXCLUDED.password,
            status = EXCLUDED.status,
            support = EXCLUDED.support,
            is_supervisor = EXCLUDED.is_supervisor,
            supervised_users = EXCLUDED.supervised_users,
            departments = EXCLUDED.departments,
            permission_id = EXCLUDED.permission_id,
            company_id = EXCLUDED.company_id,
            avatar_url = EXCLUDED.avatar_url,
            email_confirmed = EXCLUDED.email_confirmed,
            updated_at = EXCLUDED.updated_at
          `,
          values
        );
        await dest.query('COMMIT');
        migrados += batch.length;
      } catch (batchErr) {
        await dest.query('ROLLBACK');
        // fallback: registro a registro (pra não perder o lote inteiro)
        for (const v of perRowParams) {
          try {
            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');
            await dest.query(upsertSqlSingle, v);
            await dest.query('COMMIT');
            migrados += 1;
          } catch (rowErr) {
            await dest.query('ROLLBACK');
            erros += 1;
            console.error(`❌ Erro ao migrar user id=${v[0]}: ${rowErr.message}`);
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
    console.log(`✅ Migrados ${migrados}/${total} usuário(s) (exceto ID 1) em ${secs}s.${erros ? ` (${erros} com erro)` : ''}`);
  } finally {
    await source.end();
    await dest.end();
  }
};

// —— helpers —— //
function readBool(v, def=false) {
  if (v == null) return def;
  const s = String(v).trim().toLowerCase();
  return ['1','true','t','yes','y'].includes(s);
}
async function readCursor(cursor, size) {
  return await new Promise((resolve, reject) => {
    cursor.read(size, (err, rows) => (err ? reject(err) : resolve(rows || [])));
  });
}
function idsByCompany(rows) {
  const map = new Map(); // company_id -> Set(ids)
  for (const r of rows) {
    const k = String(r.company_id);
    if (!map.has(k)) map.set(k, new Set());
    map.get(k).add(r.id);
  }
  return map;
}
function groupIds(rows, keyId, keyVal) {
  const m = {};
  for (const r of rows) {
    const id = r[keyId];
    const val = r[keyVal];
    if (id == null || val == null) continue;
    if (!m[id]) m[id] = [];
    m[id].push(val);
  }
  return m;
}
function asArray(val) {
  if (Array.isArray(val)) return val;
  if (val == null) return [];
  if (typeof val === 'string') {
    try { const p = JSON.parse(val); return Array.isArray(p) ? p : []; } catch { return []; }
  }
  return [];
}
function toInt(v, def=null) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : def;
}
function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Usuário ${id}`;
}
function safeEmail(email, id, companyId) {
  const e = (email || '').toString().trim().toLowerCase();
  if (e && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e)) return e;
  return `user${id}.${companyId}@placeholder.local`; // fallback válido de sintaxe
}
