// migrations/migrateChannels.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateChannels(ctx = {}) {
  console.log('📡 Migrando "Whatsapps" → "channel_instances"...');

  // lote (16 params/linha → 2000 linhas ≈ 32k params < 65535)
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);

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
    application_name: 'migrateChannels:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateChannels:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // —— Pré-carrega para evitar N+1
    const validFlowIds = new Set((await dest.query('SELECT id FROM flows')).rows.map(r => String(r.id)));
    const flowFallbackByCompany = new Map(
      (await dest.query('SELECT company_id, MIN(id) AS flow_id FROM flows GROUP BY company_id'))
        .rows.map(r => [String(r.company_id), r.flow_id])
    );

    const validDeptIds = new Set((await dest.query('SELECT id FROM departments')).rows.map(r => String(r.id)));
    const deptFallbackByCompany = new Map(
      (await dest.query('SELECT company_id, MIN(id) AS department_id FROM departments GROUP BY company_id'))
        .rows.map(r => [String(r.company_id), r.department_id])
    );

    // —— Contagem p/ barra/ETA
    const countRes = await source.query(
      `SELECT COUNT(*)::bigint AS total FROM "public"."Whatsapps" ${tenantId ? 'WHERE "tenantId" = $1' : ''}`,
      tenantId ? [tenantId] : []
    );
    const total = Number(countRes.rows[0]?.total || 0);
    if (!total) {
      console.log(tenantId ? `⚠️ Nenhum canal para TENANT_ID=${tenantId}.` : '⚠️ Nenhum canal na origem.');
      return;
    }

    // —— Cursor server-side
    const selectSql = `
      SELECT
        id,
        name,
        type,
        "tenantId" AS company_id,
        status,
        number,
        session,
        qrcode,
        "tokenAPI",
        "chatFlowId",
        "queueId",
        "is_open_ia",
        "isDeleted",
        "createdAt",
        "updatedAt"
      FROM "public"."Whatsapps"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
      ORDER BY id
    `;
    const cursor = source.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    // —— Barra de progresso
    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    const startedAt = Date.now();
    let processed = 0;
    let migradas = 0;
    let ignoradasTipo = 0;
    let erros = 0;

    const typeMap = {
      whatsapp: 'WhatsAppQRCode',
      waba: 'WhatsAppCloudAPI',
      instagram: 'Instagram',
      telegram: 'Telegram',
      messenger: 'Messenger'
    };

    while (true) {
      const rows = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!rows || rows.length === 0) break;

      const values = [];
      const placeholders = [];
      const perRowParams = []; // fallback se o INSERT batelado falhar

      rows.forEach((row, i) => {
        const mappedType = typeMap[String(row.type || '').toLowerCase()];
        if (!mappedType) {
          ignoradasTipo++;
          return; // pula este registro no batch
        }

        // status deve ser obrigatoriamente 'disconnected' (minúsculo) no servidor novo
        const status = 'disconnected';

        // j_id (número), sessão, qr_code
        const jId = row.number || '';
        const session = row.session || '';
        const qrCode = row.qrcode || '';

        // config (ex.: tokenAPI)
        const config = JSON.stringify(row.tokenAPI ? { tokenAPI: row.tokenAPI } : {});

        // flow: mantém se existir; senão tenta fallback por empresa
        let flowId = row.chatFlowId ? String(row.chatFlowId) : null;
        if (flowId && !validFlowIds.has(flowId)) {
          flowId = null;
        }
        if (!flowId) {
          const fb = flowFallbackByCompany.get(String(row.company_id));
          flowId = fb || null;
        }

        // department: idem
        let departmentId = row.queueId ? String(row.queueId) : null;
        if (departmentId && !validDeptIds.has(departmentId)) {
          departmentId = null;
        }
        if (!departmentId) {
          const fb = deptFallbackByCompany.get(String(row.company_id));
          departmentId = fb || null;
        }

        const enableBotForGroups = !!row.is_open_ia;
        const openTicketForGroups = !!row.is_open_ia;

        const deletedAt = row.isDeleted ? (row.updatedAt || new Date()) : null;

        const v = [
          row.id,                          // id
          safeName(row.name, row.id),      // name
          mappedType,                      // type
          row.company_id,                  // company_id
          status,                          // status (forçado 'disconnected')
          jId,                             // j_id
          session,                         // session
          qrCode,                          // qr_code
          config,                          // config (json)
          flowId,                          // flow_id
          departmentId,                    // department_id
          enableBotForGroups,              // enable_chatbot_for_groups
          openTicketForGroups,             // open_ticket_for_groups
          row.createdAt,                   // created_at
          row.updatedAt,                   // updated_at
          deletedAt                        // deleted_at
        ];
        perRowParams.push(v);

        const base = placeholders.length * 16;
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12}, $${base + 13}, $${base + 14}, $${base + 15}, $${base + 16})`
        );
        values.push(...v);
      });

      if (placeholders.length === 0) {
        processed += rows.length;
        const elapsed = (Date.now() - startedAt) / 1000;
        bar.update(processed, { rate: (processed / Math.max(1, elapsed)).toFixed(1) });
        continue;
      }

      const upsertSql = `
        INSERT INTO channel_instances (
          id, name, type, company_id, status, j_id, session, qr_code, config,
          flow_id, department_id, enable_chatbot_for_groups, open_ticket_for_groups,
          created_at, updated_at, deleted_at
        ) VALUES
          ${placeholders.join(',')}
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
          updated_at  = EXCLUDED.updated_at,
          deleted_at  = EXCLUDED.deleted_at
      `;

      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(upsertSql, values);
        await dest.query('COMMIT');
        migradas += placeholders.length;
      } catch (batchErr) {
        await dest.query('ROLLBACK');
        // fallback row-a-row pra não perder o lote
        for (const v of perRowParams) {
          try {
            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');
            await dest.query(
              `
              INSERT INTO channel_instances (
                id, name, type, company_id, status, j_id, session, qr_code, config,
                flow_id, department_id, enable_chatbot_for_groups, open_ticket_for_groups,
                created_at, updated_at, deleted_at
              ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15, $16
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
                updated_at  = EXCLUDED.updated_at,
                deleted_at  = EXCLUDED.deleted_at
              `,
              v
            );
            await dest.query('COMMIT');
            migradas += 1;
          } catch (rowErr) {
            await dest.query('ROLLBACK');
            erros += 1;
            console.error(`❌ Erro ao migrar canal ID ${v[0]}: ${rowErr.message}`);
          }
        }
      }

      processed += rows.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = (processed / Math.max(1, elapsed)).toFixed(1);
      bar.update(processed, { rate });
    }

    bar.stop();
    await new Promise((resolve, reject) => cursor.close(err => (err ? reject(err) : resolve())));
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`✅ Migrados ${migradas}/${total} canais em ${secs}s. (${ignoradasTipo} ignorados por tipo, ${erros} com erro)`);
  } finally {
    await source.end();
    await dest.end();
  }
};

// —— helpers
function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Canal ${id}`;
}
