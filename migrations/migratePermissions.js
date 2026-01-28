// migrations/migratePermissionsProfiles.batched.js
"use strict";

require("dotenv").config();
const { Client } = require("pg");
const Cursor = require("pg-cursor");
const cliProgress = require("cli-progress");

/**
 * Objetivo:
 * 1) Para cada empresa (company_id) no DEST, garantir 2 perfis:
 *    - "Usuário"
 *    - "Administrador"
 * 2) Atualizar users.permission_id no DEST com base no Users.profile do ORIGEM:
 *    - profile contendo "admin" ou "super" => Administrador
 *    - senão => Usuário
 *
 * Observação: IDs dos usuários são reaproveitados na migração, então dá pra mapear por (user.id, company_id).
 */

module.exports = async function migratePermissionsProfiles(ctx = {}) {
  console.log(
    "🔐 Criando perfis padrão de permissões (Usuário/Admin) e vinculando nos usuários...",
  );

  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);
  const DEBUG = readBool(process.env.PERMISSIONS_PROFILES_DEBUG, false);

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
    application_name: "migratePermissionsProfiles:source",
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: "migratePermissionsProfiles:dest",
  });

  await source.connect();
  await dest.connect();

  try {
    // ======================================================
    // 1) Carrega empresas do DEST
    // ======================================================
    const companiesRes = await dest.query(
      `SELECT id FROM companies ${tenantId ? "WHERE id = $1" : ""} ORDER BY id`,
      tenantId ? [tenantId] : [],
    );
    const companyIds = companiesRes.rows
      .map((r) => Number(r.id))
      .filter(Number.isFinite);

    if (!companyIds.length) {
      console.log(
        tenantId
          ? `⚠️  Nenhuma company encontrada com id=${tenantId} no DEST.`
          : "⚠️  Nenhuma company encontrada no DEST.",
      );
      return;
    }

    // ======================================================
    // 2) Garante perfis Usuário/Admin por empresa
    // ======================================================
    console.log(
      '🧩 Garantindo perfis "Usuário" e "Administrador" em permissions...',
    );
    const permIdsByCompany = new Map(); // companyId -> { userId, adminId }

    for (const cid of companyIds) {
      const existing = await dest.query(
        `SELECT id, name FROM permissions WHERE company_id = $1 AND name IN ('Usuário','Administrador')`,
        [cid],
      );

      let userPermId = null;
      let adminPermId = null;

      for (const r of existing.rows) {
        const nm = (r.name || "").toString();
        if (nm === "Usuário") userPermId = Number(r.id);
        if (nm === "Administrador") adminPermId = Number(r.id);
      }

      // cria se faltar
      if (!userPermId) {
        const ins = await dest.query(
          `
          INSERT INTO permissions (name, permissions, company_id, created_at, updated_at)
          VALUES ($1, $2::jsonb, $3, NOW(), NOW())
          RETURNING id
          `,
          ["Usuário", JSON.stringify(UserProfilePermissions), cid],
        );
        userPermId = Number(ins.rows[0].id);
        if (DEBUG)
          console.log(
            `✅ company=${cid} criou perfil Usuário id=${userPermId}`,
          );
      }

      if (!adminPermId) {
        const ins = await dest.query(
          `
          INSERT INTO permissions (name, permissions, company_id, created_at, updated_at)
          VALUES ($1, $2::jsonb, $3, NOW(), NOW())
          RETURNING id
          `,
          ["Administrador", JSON.stringify(DefaultPermissions), cid],
        );
        adminPermId = Number(ins.rows[0].id);
        if (DEBUG)
          console.log(
            `✅ company=${cid} criou perfil Administrador id=${adminPermId}`,
          );
      }

      permIdsByCompany.set(String(cid), { userPermId, adminPermId });
    }

    // ======================================================
    // 3) Conta usuários no ORIGEM pra barra
    // ======================================================
    const countRes = await source.query(
      `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."Users"
      WHERE "id" != 1
      ${tenantId ? 'AND "tenantId" = $1' : ""}
      `,
      tenantId ? [tenantId] : [],
    );
    const total = Number(countRes.rows[0]?.total || 0);
    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhum user no ORIGEM para tenantId=${tenantId} (exceto id=1).`
          : "⚠️  Nenhum user no ORIGEM (exceto id=1).",
      );
      return;
    }

    // ======================================================
    // 4) Cursor: lê Users do ORIGEM e atualiza users do DEST
    // ======================================================
    console.log(
      "🔁 Atualizando users.permission_id no DEST baseado em Users.profile do ORIGEM...",
    );
    const selectSql = `
      SELECT
        "id",
        "tenantId" AS company_id,
        "profile"
      FROM "public"."Users"
      WHERE "id" != 1
      ${tenantId ? 'AND "tenantId" = $1' : ""}
      ORDER BY "id"
    `;
    const cursor = source.query(
      new Cursor(selectSql, tenantId ? [tenantId] : []),
    );

    const bar = new cliProgress.SingleBar(
      {
        format:
          "Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s",
        hideCursor: true,
      },
      cliProgress.Presets.shades_classic,
    );
    bar.start(total, 0, { rate: "0.0" });

    const startedAt = Date.now();
    let processed = 0;
    let updated = 0;
    let missingCompany = 0;
    let erros = 0;

    while (true) {
      const batch = await readCursor(cursor, BATCH_SIZE);
      if (!batch.length) break;

      // Monta VALUES para update em lote:
      // (user_id, company_id, permission_id)
      const rowsToUpdate = [];

      for (const row of batch) {
        const userId = Number(row.id);
        const companyId = Number(row.company_id);
        const key = String(companyId);

        const perms = permIdsByCompany.get(key);
        if (!perms) {
          missingCompany += 1;
          continue;
        }

        const profile = (row.profile || "").toString().toLowerCase().trim();
        const isAdmin = profile.includes("admin") || profile.includes("super");

        const permId = isAdmin ? perms.adminPermId : perms.userPermId;
        rowsToUpdate.push([userId, companyId, permId]);
      }

      if (!rowsToUpdate.length) {
        processed += batch.length;
        bar.update(processed, { rate: calcRate(processed, startedAt) });
        continue;
      }

      // UPDATE users u SET permission_id = v.permission_id
      // FROM (VALUES ... ) v(user_id, company_id, permission_id)
      // WHERE u.id=v.user_id AND u.company_id=v.company_id
      try {
        const { sql, params } = buildUpdateUsersPermissionSql(rowsToUpdate);
        await dest.query("BEGIN");
        await dest.query("SET LOCAL synchronous_commit TO OFF");
        const res = await dest.query(sql, params);
        await dest.query("COMMIT");

        // rowCount aqui pode ser menor se algum user não existir no DEST (ex: não migrou)
        updated += Number(res.rowCount || 0);
      } catch (e) {
        await dest.query("ROLLBACK");
        erros += 1;
        console.error(`❌ Erro no update batch permission_id: ${e.message}`);

        // fallback row-by-row
        for (const [uid, cid, pid] of rowsToUpdate) {
          try {
            await dest.query(
              `UPDATE users SET permission_id = $1, updated_at = NOW() WHERE id = $2 AND company_id = $3`,
              [pid, uid, cid],
            );
            updated += 1;
          } catch (rowErr) {
            erros += 1;
            console.error(
              `❌ Erro ao atualizar user id=${uid} company=${cid}: ${rowErr.message}`,
            );
          }
        }
      }

      processed += batch.length;
      bar.update(processed, { rate: calcRate(processed, startedAt) });
    }

    bar.stop();

    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`✅ Perfis garantidos para ${companyIds.length} empresa(s).`);
    console.log(
      `✅ Users atualizados (permission_id) = ${updated} em ${secs}s.${erros ? ` (${erros} erro(s))` : ""}${missingCompany ? ` | missingCompany=${missingCompany}` : ""}`,
    );

    // Ajuste sequence permissions (opcional)
    try {
      await dest.query(
        `SELECT setval('permissions_id_seq', (SELECT COALESCE(MAX(id), 1) FROM permissions))`,
      );
    } catch (e) {
      // não quebra
    }
  } finally {
    await source.end();
    await dest.end();
  }
};

// ======================================================
// Permissions JSON (espelho do Go)
// ======================================================

const DefaultPermissions = {
  Dashboard: { access: true, geral: true, atendente: true },
  Tickets: {
    access: true,
    sendMessage: true,
    openTicket: true,
    filterSeeAll: true,
    manage: true,
    viewTicketContact: true,
  },
  InternalChat: { access: true },
  Users: { access: true, edit: true, create: true },
  Departments: { access: true, edit: true, create: true },
  Permissions: { access: true, edit: true, create: true },
  Channels: { access: true, edit: true, create: true },
  Tags: { access: true, edit: true, create: true, delete: true, seeAll: true },
  TicketReasons: { access: true, edit: true, create: true, delete: true },
  Providers: { access: true, edit: true, create: true, delete: true },
  Contacts: { access: true, edit: true, create: true, delete: true },
  QuickMessages: { access: true, edit: true, create: true, delete: true },
  Flows: { access: true, edit: true, create: true, delete: true },
  Settings: { access: true, edit: true, create: true },
  Campaign: { access: true, edit: true, create: true, delete: true },
  Task: { access: true, edit: true, create: true, delete: true },
  TaskType: { access: true, edit: true, create: true, delete: true },
  MyTask: { access: true, edit: true, create: true, delete: true },
  TaskAutomation: { access: true, edit: true, create: true, delete: true },
  AttendancePeriods: { access: true, edit: true, create: true, delete: true },
  Holidays: { access: true, create: true, delete: true },
  ReportsMessage: { access: true },
  ReportsTicket: { access: true },
  ReportsRating: { access: true },
  ApiDocs: { access: true },
  ReportsContact: { access: true },
  Timeline: { access: true, seeAll: true },
  TicketsPanel: { access: true },
  Kanban: { access: true, create: true, edit: true, delete: true },
  KanbanBoard: { access: true, create: true, edit: true, delete: true },
  VirtualAgents: { access: true, edit: true, create: true, delete: true },
  SupportUsers: {
    access: false,
    list: false,
    create: false,
    edit: false,
    delete: false,
  },
  SGP: { access: true, create: true, edit: true, delete: true },
};

const UserProfilePermissions = {
  Dashboard: { access: true, geral: false, atendente: true },
  Tickets: {
    access: true,
    sendMessage: true,
    openTicket: true,
    filterSeeAll: true,
    manage: true,
    viewTicketContact: true,
  },
  InternalChat: { access: true },
  ApiDocs: { access: true },

  Users: { access: false, edit: false, create: false },
  Departments: { access: false, edit: false, create: false },
  Permissions: { access: false, edit: false, create: false },
  Channels: { access: false, edit: false, create: false },

  Tags: {
    access: false,
    edit: false,
    create: false,
    delete: false,
    seeAll: false,
  },
  TicketReasons: { access: false, edit: false, create: false, delete: false },
  Providers: { access: false, edit: false, create: false, delete: false },
  Contacts: { access: false, edit: false, create: false, delete: false },
  QuickMessages: { access: false, edit: false, create: false, delete: false },
  Flows: { access: false, edit: false, create: false, delete: false },
  Settings: { access: false, edit: false, create: false },
  Campaign: { access: false, edit: false, create: false, delete: false },

  Task: { access: false, edit: false, create: false, delete: false },
  TaskType: { access: false, edit: false, create: false, delete: false },
  MyTask: { access: false, edit: false, create: false, delete: false },
  TaskAutomation: { access: false, edit: false, create: false, delete: false },
  AttendancePeriods: {
    access: false,
    edit: false,
    create: false,
    delete: false,
  },
  Holidays: { access: false, create: false, delete: false },

  ReportsMessage: { access: false },
  ReportsRating: { access: false },
  ReportsTicket: { access: false },
  ReportsContact: { access: false },
  TicketsPanel: { access: false },

  Timeline: { access: true, seeAll: false },

  Kanban: { access: true, edit: true, create: true, delete: true },
  KanbanBoard: { access: true, edit: true, create: true, delete: true },

  VirtualAgents: { access: false, edit: false, create: false, delete: false },
  SupportUsers: {
    access: false,
    list: false,
    create: false,
    edit: false,
    delete: false,
  },
  SGP: { access: false, create: false, edit: false, delete: false },
};

// ======================================================
// Helpers
// ======================================================
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

function calcRate(processed, startedAtMs) {
  const elapsed = (Date.now() - startedAtMs) / 1000;
  return (processed / Math.max(1, elapsed)).toFixed(1);
}

/**
 * Gera SQL:
 * UPDATE users u
 * SET permission_id = v.permission_id, updated_at = NOW()
 * FROM (VALUES ($1,$2,$3), ...) v(user_id, company_id, permission_id)
 * WHERE u.id=v.user_id AND u.company_id=v.company_id
 */
function buildUpdateUsersPermissionSql(rows) {
  const values = [];
  const placeholders = [];

  for (let i = 0; i < rows.length; i++) {
    const base = i * 3;
    placeholders.push(`($${base + 1}, $${base + 2}, $${base + 3})`);
    values.push(rows[i][0], rows[i][1], rows[i][2]);
  }

  const sql = `
    UPDATE users u
    SET permission_id = v.permission_id,
        updated_at = NOW()
    FROM (VALUES ${placeholders.join(",")}) AS v(user_id, company_id, permission_id)
    WHERE u.id = v.user_id
      AND u.company_id = v.company_id
  `;

  return { sql, params: values };
}
