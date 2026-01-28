// migrations/migrateChannels.batched.js
"use strict";

require("dotenv").config();
const { Client } = require("pg");
const Cursor = require("pg-cursor");
const cliProgress = require("cli-progress");

module.exports = async function migrateChannels(ctx = {}) {
  console.log(
    '📡 Migrando "Whatsapps" → "channel_instances" + backfill "virtual_agents"...',
  );

  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 1500);
  const DEBUG = readBool(process.env.CHANNELS_DEBUG, false);

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
    application_name: "migrateChannels:source",
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: "migrateChannels:dest",
  });

  await source.connect();
  await dest.connect();

  try {
    // ------------------------------------------------------------
    // 0) Pré-carregar coisas do DEST
    // ------------------------------------------------------------
    // flows (pra legado flow_id e para VA)
    const validFlowIds = new Set(
      (await dest.query("SELECT id FROM flows")).rows.map((r) => String(r.id)),
    );
    const flowFallbackByCompany = new Map(
      (
        await dest.query(
          "SELECT company_id, MIN(id)::bigint AS flow_id FROM flows GROUP BY company_id",
        )
      ).rows.map((r) => [String(r.company_id), r.flow_id]),
    );

    // departments fallback por company
    const deptFallbackByCompany = new Map(
      (
        await dest.query(
          "SELECT company_id, MIN(id)::bigint AS department_id FROM departments GROUP BY company_id",
        )
      ).rows.map((r) => [String(r.company_id), r.department_id]),
    );

    // ------------------------------------------------------------
    // 1) Mapa Queues(origem) -> Departments(dest) por (company_id + nome)
    // ------------------------------------------------------------
    console.log(
      "📥 Carregando mapa Queues (origem) e Departments (destino) para resolver departmentId...",
    );

    // ORIGEM: Queues tem coluna "queue", não "name"
    const qSql = `
      SELECT
        "id"::bigint         AS queue_id,
        "tenantId"::bigint   AS company_id,
        "queue"              AS name
      FROM "public"."Queues"
      ${tenantId ? 'WHERE "tenantId" = $1' : ""}
    `;
    const qRes = await source.query(qSql, tenantId ? [tenantId] : []);
    const queueNameById = new Map(); // queueId -> {company_id,name}
    for (const r of qRes.rows) {
      queueNameById.set(String(r.queue_id), {
        company_id: String(r.company_id),
        name: (r.name || "").toString().trim(),
      });
    }

    const dSql = `
      SELECT
        id::bigint AS department_id,
        company_id::bigint AS company_id,
        name
      FROM departments
      ${tenantId ? "WHERE company_id = $1" : ""}
    `;
    const dRes = await dest.query(dSql, tenantId ? [tenantId] : []);
    const deptIdByCompanyAndName = new Map(); // "company#nameLower" -> deptId
    for (const r of dRes.rows) {
      const key = `${String(r.company_id)}#${String(r.name || "")
        .trim()
        .toLowerCase()}`;
      // se duplicar nomes, fica o primeiro — mas idealmente não duplica
      if (!deptIdByCompanyAndName.has(key))
        deptIdByCompanyAndName.set(key, Number(r.department_id));
    }

    if (DEBUG) {
      console.log(`🧪 DEBUG: Queues carregadas=${qRes.rows.length}`);
      console.log(`🧪 DEBUG: Departments carregados=${dRes.rows.length}`);
    }

    // ------------------------------------------------------------
    // 2) Contagem
    // ------------------------------------------------------------
    const countRes = await source.query(
      `SELECT COUNT(*)::bigint AS total FROM "public"."Whatsapps" ${tenantId ? 'WHERE "tenantId" = $1' : ""}`,
      tenantId ? [tenantId] : [],
    );
    const total = Number(countRes.rows[0]?.total || 0);
    if (!total) {
      console.log(
        tenantId
          ? `⚠️ Nenhum canal para TENANT_ID=${tenantId}.`
          : "⚠️ Nenhum canal na origem.",
      );
      return;
    }

    // ------------------------------------------------------------
    // 3) Cursor de canais (sem is_open_ia)
    // ------------------------------------------------------------
    const selectSql = `
      SELECT
        id,
        name,
        type,
        number,
        "tenantId" AS company_id,
        status,
        session,
        qrcode,
        "tokenAPI",
        "tokenTelegram",
        "chatFlowId",
        "queueId",
        "isDeleted",
        "createdAt",
        "updatedAt"
      FROM "public"."Whatsapps"
      ${tenantId ? 'WHERE "tenantId" = $1' : ""}
      ORDER BY id
    `;
    const cursor = source.query(
      new Cursor(selectSql, tenantId ? [tenantId] : []),
    );

    // ------------------------------------------------------------
    // 4) Barra de progresso
    // ------------------------------------------------------------
    const bar = new cliProgress.SingleBar(
      {
        format:
          "Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s",
        hideCursor: true,
      },
      cliProgress.Presets.shades_classic,
    );
    bar.start(total, 0, { rate: "0.0" });

    const typeMap = {
      whatsapp: "WhatsAppQRCode",
      meow: "WhatsAppQRCode",
      baileys: "WhatsAppQRCode",
      waba: "WhatsAppCloudAPI",
      instagram: "Instagram",
      telegram: "Telegram",
      messenger: "Messenger",
    };

    // cache de VA por canal (a gente vai criar 1 por canal: company#channelId)
    const vaCache = new Map(); // key: "company#channelId" -> vaId

    const startedAt = Date.now();
    let processed = 0,
      migradas = 0,
      ignoradasTipo = 0,
      erros = 0;

    while (true) {
      const rows = await readCursor(cursor, BATCH_SIZE);
      if (!rows.length) break;

      // ------------------------------------------------------------
      // 5) Criar virtual_agents para o lote (1 por canal)
      // ------------------------------------------------------------
      const toCreate = [];
      for (const row of rows) {
        const key = `${row.company_id}#${row.id}`;
        if (!vaCache.has(key)) {
          toCreate.push({
            company_id: row.company_id,
            channel_id: row.id,
            name: `Agente do Canal #${row.id}`,
          });
        }
      }

      if (toCreate.length) {
        // buscar existentes
        const pairs = [
          ...new Set(toCreate.map((x) => `${x.company_id}#${x.channel_id}`)),
        ];
        const ph = pairs
          .map((_, i) => `($${i * 2 + 1}::bigint,$${i * 2 + 2}::bigint)`)
          .join(",");
        const vals = pairs.flatMap((k) => {
          const [c, ch] = k.split("#");
          return [c, ch];
        });

        // vamos armazenar channel_id no campo "config"? não — melhor via nome estável
        // então aqui: tenta achar pelo name + company_id
        // (mais robusto do que inventar coluna)
        const existing = await dest.query(
          `
          SELECT id, company_id, name
          FROM virtual_agents
          WHERE (company_id, name) IN (
            ${pairs.map((_, i) => `($${i * 2 + 1}::bigint,$${i * 2 + 2})`).join(",")}
          )
          `,
          pairs.flatMap((k) => {
            const [c, ch] = k.split("#");
            return [c, `Agente do Canal #${ch}`];
          }),
        );
        for (const r of existing.rows) {
          const chId = String(r.name).split("#")[1];
          if (chId) vaCache.set(`${r.company_id}#${chId}`, r.id);
        }

        const missing = toCreate.filter(
          (x) => !vaCache.has(`${x.company_id}#${x.channel_id}`),
        );
        if (missing.length) {
          const tuples = [];
          const vals2 = [];
          missing.forEach((n, i) => {
            tuples.push(
              `($${i * 5 + 1},$${i * 5 + 2}::bigint,NULL,$${i * 5 + 3}::boolean,$${i * 5 + 4},$${i * 5 + 5})`,
            );
            vals2.push(n.name, n.company_id, true, new Date(), new Date());
          });

          try {
            const ins = await dest.query(
              `
              INSERT INTO virtual_agents (name, company_id, flow_id, active, created_at, updated_at)
              VALUES ${tuples.join(",")}
              RETURNING id, company_id, name
              `,
              vals2,
            );
            for (const r of ins.rows) {
              const chId = String(r.name).split("#")[1];
              if (chId) vaCache.set(`${r.company_id}#${chId}`, r.id);
            }
          } catch (e) {
            // se duplicou no meio, recarrega
            console.warn(
              "⚠️  Falha ao criar alguns VirtualAgents; tentando recarregar:",
              e.message,
            );
            const existing2 = await dest.query(
              `
              SELECT id, company_id, name
              FROM virtual_agents
              WHERE (company_id, name) IN (
                ${pairs.map((_, i) => `($${i * 2 + 1}::bigint,$${i * 2 + 2})`).join(",")}
              )
              `,
              pairs.flatMap((k) => {
                const [c, ch] = k.split("#");
                return [c, `Agente do Canal #${ch}`];
              }),
            );
            for (const r of existing2.rows) {
              const chId = String(r.name).split("#")[1];
              if (chId) vaCache.set(`${r.company_id}#${chId}`, r.id);
            }
          }
        }
      }

      // ------------------------------------------------------------
      // 6) INSERT/UPSERT canais
      // ------------------------------------------------------------
      const values = [];
      const placeholders = [];
      const perRowParams = [];

      // IMPORTANTE: como a gente pode "pular" tipos, usamos idx separado
      let idx = 0;

      for (const row of rows) {
        const mappedType = typeMap[String(row.type || "").toLowerCase()];
        if (!mappedType) {
          ignoradasTipo++;
          continue;
        }

        // status: normaliza simples (legacy pode vir "CONNECTED"/etc)
        const status = normalizeStatus(row.status);

        // j_id: usa number se tiver, senão vazio
        const jId = row.number || "";

        // session bytea: mantenha null (evita mismatch de tipo)
        const session = null;

        const qrCode = row.qrcode || "";

        // config: guarda tokens relevantes
        const cfg = {};
        if (row.tokenAPI) cfg.tokenAPI = row.tokenAPI;
        if (row.tokenTelegram) cfg.tokenTelegram = row.tokenTelegram;
        const config = JSON.stringify(cfg);

        // flow_id (LEGACY)
        let flowId = row.chatFlowId ? String(row.chatFlowId) : null;
        if (flowId && !validFlowIds.has(flowId)) flowId = null;
        if (!flowId) {
          const fb = flowFallbackByCompany.get(String(row.company_id));
          flowId = fb ? String(fb) : null;
        }

        // department_id: resolve por nome (Queues.queue -> departments.name)
        let departmentId = null;
        if (row.queueId != null) {
          const q = queueNameById.get(String(row.queueId));
          if (q && q.name) {
            const key = `${String(row.company_id)}#${q.name.trim().toLowerCase()}`;
            departmentId = deptIdByCompanyAndName.get(key) ?? null;
          }
        }
        if (!departmentId) {
          departmentId =
            deptFallbackByCompany.get(String(row.company_id)) ?? null;
        }

        // virtual_agent_id: 1 por canal
        const vaId = vaCache.get(`${row.company_id}#${row.id}`) || null;

        const enableBotForGroups = false;
        const openTicketForGroups = false;

        const deletedAt = row.isDeleted ? row.updatedAt || new Date() : null;

        const v = [
          row.id, // 1 id
          safeName(row.name, row.id), // 2 name
          mappedType, // 3 type
          row.company_id, // 4 company_id
          status, // 5 status
          jId, // 6 j_id
          session, // 7 session
          qrCode, // 8 qr_code
          config, // 9 config
          flowId, // 10 flow_id (legacy)
          departmentId, // 11 department_id
          enableBotForGroups, // 12 enable_chatbot_for_groups
          openTicketForGroups, // 13 open_ticket_for_groups
          row.createdAt, // 14 created_at
          row.updatedAt, // 15 updated_at
          deletedAt, // 16 deleted_at
          vaId, // 17 virtual_agent_id
          true, // 18 active
          true, // 19 fetch_messages
          true, // 20 allow_all_users
          "[]", // 21 allowed_user_ids (json string)
        ];
        perRowParams.push(v);

        const base = idx * 21;
        placeholders.push(
          `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9},` +
            `$${base + 10},$${base + 11},$${base + 12},$${base + 13},$${base + 14},$${base + 15},$${base + 16},$${base + 17},` +
            `$${base + 18},$${base + 19},$${base + 20},$${base + 21}::jsonb)`,
        );
        values.push(...v);
        idx++;
      }

      if (!placeholders.length) {
        processed += rows.length;
        const elapsed = (Date.now() - startedAt) / 1000;
        bar.update(processed, {
          rate: (processed / Math.max(1, elapsed)).toFixed(1),
        });
        continue;
      }

      const upsertSql = `
        INSERT INTO channel_instances (
          id, name, type, company_id, status, j_id, session, qr_code, config,
          flow_id, department_id, enable_chatbot_for_groups, open_ticket_for_groups,
          created_at, updated_at, deleted_at, virtual_agent_id,
          active, fetch_messages, allow_all_users, allowed_user_ids
        ) VALUES
          ${placeholders.join(",")}
        ON CONFLICT (id) DO UPDATE SET
          name        = EXCLUDED.name,
          type        = EXCLUDED.type,
          company_id  = EXCLUDED.company_id,
          status      = EXCLUDED.status,
          j_id        = EXCLUDED.j_id,
          session     = EXCLUDED.session,
          qr_code     = EXCLUDED.qr_code,
          config      = EXCLUDED.config,
          flow_id     = EXCLUDED.flow_id,
          department_id = EXCLUDED.department_id,
          enable_chatbot_for_groups = EXCLUDED.enable_chatbot_for_groups,
          open_ticket_for_groups    = EXCLUDED.open_ticket_for_groups,
          virtual_agent_id          = EXCLUDED.virtual_agent_id,
          active       = EXCLUDED.active,
          fetch_messages = EXCLUDED.fetch_messages,
          allow_all_users = EXCLUDED.allow_all_users,
          allowed_user_ids = EXCLUDED.allowed_user_ids,
          updated_at  = EXCLUDED.updated_at,
          deleted_at  = EXCLUDED.deleted_at
      `;

      try {
        await dest.query("BEGIN");
        await dest.query("SET LOCAL synchronous_commit TO OFF");
        await dest.query(upsertSql, values);
        await dest.query("COMMIT");
        migradas += placeholders.length;
      } catch (batchErr) {
        await dest.query("ROLLBACK");

        // fallback por linha
        for (const v of perRowParams) {
          try {
            await dest.query("BEGIN");
            await dest.query("SET LOCAL synchronous_commit TO OFF");
            await dest.query(
              `
              INSERT INTO channel_instances (
                id, name, type, company_id, status, j_id, session, qr_code, config,
                flow_id, department_id, enable_chatbot_for_groups, open_ticket_for_groups,
                created_at, updated_at, deleted_at, virtual_agent_id,
                active, fetch_messages, allow_all_users, allowed_user_ids
              ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21::jsonb
              )
              ON CONFLICT (id) DO UPDATE SET
                name        = EXCLUDED.name,
                type        = EXCLUDED.type,
                company_id  = EXCLUDED.company_id,
                status      = EXCLUDED.status,
                j_id        = EXCLUDED.j_id,
                session     = EXCLUDED.session,
                qr_code     = EXCLUDED.qr_code,
                config      = EXCLUDED.config,
                flow_id     = EXCLUDED.flow_id,
                department_id = EXCLUDED.department_id,
                enable_chatbot_for_groups = EXCLUDED.enable_chatbot_for_groups,
                open_ticket_for_groups    = EXCLUDED.open_ticket_for_groups,
                virtual_agent_id          = EXCLUDED.virtual_agent_id,
                active       = EXCLUDED.active,
                fetch_messages = EXCLUDED.fetch_messages,
                allow_all_users = EXCLUDED.allow_all_users,
                allowed_user_ids = EXCLUDED.allowed_user_ids,
                updated_at  = EXCLUDED.updated_at,
                deleted_at  = EXCLUDED.deleted_at
              `,
              v,
            );
            await dest.query("COMMIT");
            migradas += 1;
          } catch (rowErr) {
            await dest.query("ROLLBACK");
            erros += 1;
            console.error(
              `❌ Erro ao migrar canal ID ${v[0]}: ${rowErr.message}`,
            );
          }
        }
      }

      processed += rows.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      bar.update(processed, {
        rate: (processed / Math.max(1, elapsed)).toFixed(1),
      });
    }

    bar.stop();
    await new Promise((resolve, reject) =>
      cursor.close((err) => (err ? reject(err) : resolve())),
    );

    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `✅ Migrados ${migradas}/${total} canais em ${secs}s. (${ignoradasTipo} ignorados por tipo, ${erros} com erro)`,
    );
  } finally {
    await source.end();
    await dest.end();
  }
};

// ---------------- helpers ----------------
function safeName(name, id) {
  const n = (name || "").toString().trim();
  return n.length ? n : `Canal ${id}`;
}

function normalizeStatus(v) {
  const s = String(v || "")
    .toLowerCase()
    .trim();
  if (["connected", "conectado", "online", "open"].includes(s))
    return "connected";
  if (["error", "erro", "fail", "failed"].includes(s)) return "error";
  return "disconnected";
}

async function readCursor(cursor, size) {
  return await new Promise((resolve, reject) => {
    cursor.read(size, (err, rows) => (err ? reject(err) : resolve(rows || [])));
  });
}

function readBool(v, def = false) {
  if (v == null) return def;
  const s = String(v).trim().toLowerCase();
  return ["1", "true", "t", "yes", "y"].includes(s);
}
