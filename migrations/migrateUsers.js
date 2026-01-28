// migrations/migrateUsers.batched.js
"use strict";

require("dotenv").config();
const { Client } = require("pg");
const Cursor = require("pg-cursor");
const cliProgress = require("cli-progress");

module.exports = async function migrateUsers(ctx = {}) {
  console.log('👤 Migrando "Users" → "users"...');

  // params/linha ~ 30 (mais colunas defaults) → use batch menor pra ficar sempre < 65535
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 1200);

  // Debug
  const DEBUG = readBool(process.env.USERS_DEBUG, false);
  const DEBUG_SAMPLE = Number(process.env.USERS_DEBUG_SAMPLE || 20);

  // Usa TENANT_ID do ctx (preferencial) ou do .env
  const tenantId =
    ctx.tenantId != null && String(ctx.tenantId).trim() !== ""
      ? String(ctx.tenantId).trim()
      : process.env.TENANT_ID
        ? String(process.env.TENANT_ID).trim()
        : null;

  const source = new Client({
    host: process.env.SRC_HOST,
    port: process.env.SRC_PORT,
    user: process.env.SRC_USER,
    password: process.env.SRC_PASS,
    database: process.env.SRC_DB,
    application_name: "migrateUsers:source",
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: "migrateUsers:dest",
  });

  await source.connect();
  await dest.connect();

  try {
    // =========================================
    // 0) Carrega mapas necessários
    // =========================================

    // 0.1) Permissions do DEST (pra escolher permission_id)
    const whereCompany = tenantId ? "WHERE company_id = $1" : "";
    const paramsCompany = tenantId ? [tenantId] : [];

    const permsRes = await safeQuery(
      dest,
      `SELECT id, company_id, name FROM permissions ${whereCompany}`,
      paramsCompany,
    );
    const permsByCompany = new Map();
    if (permsRes?.rows) {
      for (const r of permsRes.rows) {
        const k = String(r.company_id);
        if (!permsByCompany.has(k)) permsByCompany.set(k, []);
        permsByCompany
          .get(k)
          .push({ id: Number(r.id), name: (r.name || "").toString() });
      }
    }
    const firstPermByCompany = new Map();
    for (const [k, arr] of permsByCompany.entries()) {
      if (arr.length) firstPermByCompany.set(k, arr[0].id);
    }

    // 0.2) Departments do DEST: (companyId + normalizedName) -> departmentId
    console.log('📥 Lendo "departments" do DEST...');
    const depWhere = tenantId ? "WHERE company_id = $1" : "";
    const depParams = tenantId ? [tenantId] : [];

    const depRes = await dest.query(
      `SELECT id, company_id, name FROM departments ${depWhere}`,
      depParams,
    );

    const departmentMap = new Map(); // key = "companyId:normalizedName" -> departmentId
    for (const d of depRes.rows) {
      const key = `${Number(d.company_id)}:${normName(d.name)}`;
      departmentMap.set(key, Number(d.id));
    }

    if (DEBUG) {
      console.log(
        `🧪 DEBUG: departments carregados=${depRes.rows.length} | mapeados=${departmentMap.size}`,
      );
      const sample = depRes.rows
        .slice(0, 8)
        .map((d) => `${d.company_id}:${d.id}:${d.name}`);
      console.log("   sample:", sample);
    }

    // 0.3) Queues do ORIGEM: queueId -> { companyId, normalizedName }
    console.log('📥 Lendo "Queues" da ORIGEM...');
    const qWhere = tenantId ? 'WHERE "tenantId" = $1' : "";
    const qParams = tenantId ? [tenantId] : [];

    const queuesRes = await source.query(
      `
      SELECT "id", "queue" AS name, "tenantId"
      FROM "public"."Queues"
      ${qWhere}
      `,
      qParams,
    );

    const queueMap = new Map(); // queueId -> { companyId, nameNorm, nameRaw }
    for (const q of queuesRes.rows) {
      queueMap.set(Number(q.id), {
        companyId: Number(q.tenantId),
        nameNorm: normName(q.name),
        nameRaw: (q.name || "").toString(),
      });
    }

    if (DEBUG) {
      console.log(
        `🧪 DEBUG: queues carregadas=${queuesRes.rows.length} | mapeadas=${queueMap.size}`,
      );
      const sample = queuesRes.rows
        .slice(0, 8)
        .map((q) => `${q.tenantId}:${q.id}:${q.name}`);
      console.log("   sample:", sample);
    }

    // 0.4) UsersQueues ORIGEM: userId -> [queueId,...]
    console.log('📥 Lendo "UsersQueues"...');
    const userQueueMap = await loadUsersQueues(source, tenantId);

    if (DEBUG) {
      const usersWithLinks = Object.keys(userQueueMap).length;
      const totalLinks = Object.values(userQueueMap).reduce(
        (acc, arr) => acc + (arr?.length || 0),
        0,
      );
      console.log(
        `🧪 DEBUG: UsersQueues -> users com vínculo=${usersWithLinks}, vínculos total=${totalLinks}`,
      );
      for (const uid of Object.keys(userQueueMap).slice(0, 5)) {
        console.log(
          `   - userId=${uid} queueIds=${JSON.stringify(userQueueMap[uid])}`,
        );
      }
    }

    // =========================================
    // 1) COUNT de usuários (exceto id=1)
    // =========================================
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."Users"
      WHERE "id" != 1
      ${tenantId ? 'AND "tenantId" = $1' : ""}
    `;
    const { rows: crows } = await source.query(
      countSql,
      tenantId ? [tenantId] : [],
    );
    const total = Number(crows[0]?.total || 0);

    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhum usuário encontrado para TENANT_ID=${tenantId} (exceto ID 1).`
          : "⚠️  Nenhum usuário encontrado na origem (exceto ID 1).",
      );
      return;
    }

    // =========================================
    // 2) Cursor de Users (schema antigo real)
    // =========================================
    console.log('📥 Lendo "Users"...');
    const selectSql = `
      SELECT
        "id",
        "name",
        "email",
        "passwordHash",
        "profile",
        "tenantId",
        "restrictedUser",
        "createdAt",
        "updatedAt"
      FROM "public"."Users"
      WHERE "id" != 1
      ${tenantId ? 'AND "tenantId" = $1' : ""}
      ORDER BY "id"
    `;
    const cursor = source.query(
      new Cursor(selectSql, tenantId ? [tenantId] : []),
    );

    // =========================================
    // 3) Barra de progresso
    // =========================================
    const bar = new cliProgress.SingleBar(
      {
        format:
          "Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s",
        hideCursor: true,
      },
      cliProgress.Presets.shades_classic,
    );
    bar.start(total, 0, { rate: "0.0" });

    // =========================================
    // 4) UPSERT single (fallback)
    // =========================================
    const upsertSqlSingle = `
      INSERT INTO users (
        id, name, email, password, is_master, status, support,
        is_supervisor, supervised_users, departments, permission_id,
        company_id, avatar_url, email_confirmed, confirmation_token,
        token_expires_at, quick_message_groups, is_api_user, api_token,
        notification_sound, mute_all_notifications, first_access, show_release_notes,
        wss_dialer_enabled, wss_config, support_ticket_config,
        presence_status, absence_message,
        created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, false,
        false, '[]'::jsonb, $7::jsonb, $8,
        $9, '', true, '', $10,
        '[]'::jsonb, false, '',
        $11, $12, true, $13,
        $14, $15::jsonb, $16::jsonb,
        $17, $18,
        $19, $20
      )
      ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        email = EXCLUDED.email,
        password = EXCLUDED.password,
        is_master = EXCLUDED.is_master,
        status = EXCLUDED.status,
        departments = EXCLUDED.departments,
        permission_id = EXCLUDED.permission_id,
        company_id = EXCLUDED.company_id,
        email_confirmed = EXCLUDED.email_confirmed,
        notification_sound = EXCLUDED.notification_sound,
        mute_all_notifications = EXCLUDED.mute_all_notifications,
        first_access = EXCLUDED.first_access,
        show_release_notes = EXCLUDED.show_release_notes,
        wss_dialer_enabled = EXCLUDED.wss_dialer_enabled,
        wss_config = EXCLUDED.wss_config,
        support_ticket_config = EXCLUDED.support_ticket_config,
        presence_status = EXCLUDED.presence_status,
        absence_message = EXCLUDED.absence_message,
        updated_at = EXCLUDED.updated_at
    `;

    const startedAt = Date.now();
    let processed = 0;
    let migrados = 0;
    let erros = 0;

    let dbgLogged = 0;

    // =========================================
    // 5) Loop por lote
    // =========================================
    while (true) {
      const batch = await readCursor(cursor, BATCH_SIZE);
      if (!batch.length) break;

      const placeholders = [];
      const values = [];
      const perRowParams = [];

      for (let i = 0; i < batch.length; i++) {
        const row = batch[i];
        const companyId = Number(row.tenantId);
        const companyKey = String(companyId);

        const name = safeName(row.name, row.id);
        const email = safeEmail(row.email, row.id, companyId);
        const pass = (row.passwordHash || "").toString();

        // status heurística via restrictedUser
        const restricted = (row.restrictedUser || "")
          .toString()
          .toLowerCase()
          .trim();
        const status =
          restricted !== "enabled" &&
          restricted !== "true" &&
          restricted !== "1";

        // master via profile
        const profile = (row.profile || "").toString().toLowerCase().trim();
        const isMaster = profile.includes("super") || profile === "master";

        // ✅ departments: userQueueMap[userId] -> queueMap(queueId -> nameNorm) -> departmentMap(companyId:nameNorm -> id)
        const rawQueueIds = (userQueueMap[row.id] || [])
          .map((n) => toInt(n, null))
          .filter(Number.isInteger);

        const deptIds = [];
        const missing = [];

        for (const qid of rawQueueIds) {
          const q = queueMap.get(qid);
          if (!q) {
            missing.push(`queueId:${qid}(not found)`);
            continue;
          }

          // Usa companyId do user (novo mundo) + nome normalizado
          const key = `${companyId}:${q.nameNorm}`;
          const depId = departmentMap.get(key);

          if (depId) {
            deptIds.push(depId);
          } else {
            missing.push(`"${q.nameRaw}"`);
          }
        }

        const departmentsJson = JSON.stringify(uniqueInts(deptIds));

        // permission_id: tenta bater por nome (profile), senão primeiro da empresa, senão 1
        const permissionId = choosePermissionId(
          profile,
          permsByCompany.get(companyKey) || [],
          firstPermByCompany.get(companyKey) ?? 1,
        );

        const tokenExpiresAt = new Date();

        // defaults extras do model novo
        const notificationSound = "default";
        const muteAllNotifications = false;

        // ✅ pedido: first_access TRUE pra todos
        const showReleaseNotes = true;
        const wssDialerEnabled = false;
        const wssConfig = JSON.stringify({});
        const supportTicketConfig = JSON.stringify({});
        const presenceStatus = "online";
        const absenceMessage = "";

        const createdAt = row.createdAt;
        const updatedAt = row.updatedAt;

        if (DEBUG && dbgLogged < DEBUG_SAMPLE) {
          console.log(`🧪 DEBUG user=${row.id} company=${companyId}`);
          console.log(`   queueIds=${JSON.stringify(rawQueueIds)}`);
          console.log(
            `   deptIds(mapped)=${JSON.stringify(uniqueInts(deptIds))}`,
          );
          if (missing.length)
            console.log(`   missing=${JSON.stringify(missing)}`);
          dbgLogged++;
        }

        // ordem params para batch:
        const v = [
          row.id, // 1
          name, // 2
          email, // 3
          pass, // 4
          isMaster, // 5
          status, // 6
          departmentsJson, // 7
          permissionId, // 8
          companyId, // 9
          tokenExpiresAt, // 10
          notificationSound, // 11
          muteAllNotifications, // 12
          showReleaseNotes, // 13
          wssDialerEnabled, // 14
          wssConfig, // 15
          supportTicketConfig, // 16
          presenceStatus, // 17
          absenceMessage, // 18
          createdAt, // 19
          updatedAt, // 20
        ];
        perRowParams.push(v);

        const base = i * 20;
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, false, ` +
            `false, '[]'::jsonb, $${base + 7}::jsonb, $${base + 8}, ` +
            `$${base + 9}, '', true, '', $${base + 10}, ` +
            `'[]'::jsonb, false, '', ` +
            `$${base + 11}, $${base + 12}, true, $${base + 13}, ` +
            `$${base + 14}, $${base + 15}::jsonb, $${base + 16}::jsonb, ` +
            `$${base + 17}, $${base + 18}, ` +
            `$${base + 19}, $${base + 20})`,
        );
        values.push(...v);
      }

      // Execução do lote
      try {
        await dest.query("BEGIN");
        await dest.query("SET LOCAL synchronous_commit TO OFF");
        await dest.query(
          `
          INSERT INTO users (
            id, name, email, password, is_master, status, support,
            is_supervisor, supervised_users, departments, permission_id,
            company_id, avatar_url, email_confirmed, confirmation_token,
            token_expires_at, quick_message_groups, is_api_user, api_token,
            notification_sound, mute_all_notifications, first_access, show_release_notes,
            wss_dialer_enabled, wss_config, support_ticket_config,
            presence_status, absence_message,
            created_at, updated_at
          ) VALUES
            ${placeholders.join(",")}
          ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            password = EXCLUDED.password,
            is_master = EXCLUDED.is_master,
            status = EXCLUDED.status,
            departments = EXCLUDED.departments,
            permission_id = EXCLUDED.permission_id,
            company_id = EXCLUDED.company_id,
            email_confirmed = EXCLUDED.email_confirmed,
            notification_sound = EXCLUDED.notification_sound,
            mute_all_notifications = EXCLUDED.mute_all_notifications,
            first_access = EXCLUDED.first_access,
            show_release_notes = EXCLUDED.show_release_notes,
            wss_dialer_enabled = EXCLUDED.wss_dialer_enabled,
            wss_config = EXCLUDED.wss_config,
            support_ticket_config = EXCLUDED.support_ticket_config,
            presence_status = EXCLUDED.presence_status,
            absence_message = EXCLUDED.absence_message,
            updated_at = EXCLUDED.updated_at
          `,
          values,
        );
        await dest.query("COMMIT");
        migrados += batch.length;
      } catch (batchErr) {
        await dest.query("ROLLBACK");
        // fallback registro a registro
        for (const v of perRowParams) {
          try {
            await dest.query("BEGIN");
            await dest.query("SET LOCAL synchronous_commit TO OFF");
            await dest.query(upsertSqlSingle, v);
            await dest.query("COMMIT");
            migrados += 1;
          } catch (rowErr) {
            await dest.query("ROLLBACK");
            erros += 1;
            console.error(
              `❌ Erro ao migrar user id=${v[0]}: ${rowErr.message}`,
            );
          }
        }
      }

      processed += batch.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = (processed / Math.max(1, elapsed)).toFixed(1);
      bar.update(processed, { rate });
    }

    bar.stop();

    // ✅ Ajusta sequence do DEST
    try {
      await dest.query(
        `SELECT setval('users_id_seq', (SELECT COALESCE(MAX(id), 1) FROM users))`,
      );
    } catch (e) {
      console.warn(
        `⚠️  Não foi possível ajustar sequence users_id_seq: ${e.message}`,
      );
    }

    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `✅ Migrados ${migrados}/${total} usuário(s) em ${secs}s.${erros ? ` (${erros} com erro)` : ""}`,
    );
  } finally {
    await source.end();
    await dest.end();
  }
};

// =========================
// helpers
// =========================
function readBool(v, def = false) {
  if (v == null) return def;
  const s = String(v).trim().toLowerCase();
  return ["1", "true", "t", "yes", "y"].includes(s);
}

async function readCursor(cursor, size) {
  return await new Promise((resolve, reject) => {
    cursor.read(size, (err, rows) => (err ? reject(err) : resolve(rows || [])));
  });
}

function toInt(v, def = null) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : def;
}

function uniqueInts(arr) {
  return Array.from(new Set((arr || []).filter(Number.isInteger)));
}

function safeName(name, id) {
  const n = (name || "").toString().trim();
  return n.length ? n : `Usuário ${id}`;
}

function safeEmail(email, id, companyId) {
  const e = (email || "").toString().trim().toLowerCase();
  if (e && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e)) return e;
  return `user${id}.${companyId}@placeholder.local`;
}

function choosePermissionId(profile, perms, firstPermissionId) {
  const p = (profile || "").toString().toLowerCase();
  if (!Array.isArray(perms) || !perms.length) return firstPermissionId;

  const tryFind = (needle) =>
    perms.find((x) => (x.name || "").toString().toLowerCase().includes(needle))
      ?.id;

  if (p.includes("super"))
    return tryFind("super") ?? tryFind("admin") ?? firstPermissionId;
  if (p.includes("admin")) return tryFind("admin") ?? firstPermissionId;
  if (p.includes("view") || p.includes("visual"))
    return tryFind("visual") ?? tryFind("view") ?? firstPermissionId;
  return tryFind("user") ?? tryFind("usu") ?? firstPermissionId;
}

async function safeQuery(client, sql, params = []) {
  try {
    return await client.query(sql, params);
  } catch {
    return { rows: [] };
  }
}

function normName(v) {
  // normaliza removendo acentos e espaços duplicados, pra bater "Jurídicas" vs "Juridicas", etc.
  return (v || "")
    .toString()
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ");
}

/**
 * Lê UsersQueues e retorna { userId: [queueId,...] }.
 * Se tenantId for informado, filtra via JOIN com Queues pra manter só da empresa.
 */
async function loadUsersQueues(source, tenantId) {
  let sql, params;
  if (tenantId) {
    sql = `
      SELECT uq."userId" AS user_id, uq."queueId" AS queue_id
      FROM "public"."UsersQueues" uq
      JOIN "public"."Queues" q ON q."id" = uq."queueId"
      WHERE q."tenantId" = $1
    `;
    params = [tenantId];
  } else {
    sql = `SELECT "userId" AS user_id, "queueId" AS queue_id FROM "public"."UsersQueues"`;
    params = [];
  }

  const res = await source.query(sql, params);
  const m = {};
  for (const r of res.rows) {
    const uid = r.user_id;
    const qid = r.queue_id;
    if (uid == null || qid == null) continue;
    if (!m[uid]) m[uid] = [];
    m[uid].push(qid);
  }
  return m;
}
