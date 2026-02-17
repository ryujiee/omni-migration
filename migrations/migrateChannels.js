// migrations/migrateChannels.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateChannels(ctx = {}) {
  console.log('📡 Migrando "Whatsapps" → "channel_instances" (com VirtualAgent)...');

  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);

  const tenantId =
    ctx.tenantId != null && String(ctx.tenantId).trim() !== ''
      ? String(ctx.tenantId).trim()
      : (process.env.TENANT_ID ? String(process.env.TENANT_ID).trim() : null);

  const source = new Client({
    host: process.env.SRC_HOST, port: process.env.SRC_PORT,
    user: process.env.SRC_USER, password: process.env.SRC_PASS,
    database: process.env.SRC_DB, application_name: 'migrateChannels:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST, port: process.env.DST_PORT,
    user: process.env.DST_USER, password: process.env.DST_PASS,
    database: process.env.DST_DB, application_name: 'migrateChannels:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // —— Pré-carrega estruturas de apoio
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

    // map rápido de VirtualAgent por (company, flow)
    const vaCache = new Map(); // key: "company#flow" -> id

    // Detecta schema real do destino para suportar versões com colunas diferentes
    const channelColsRes = await dest.query(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = 'channel_instances'
    `);
    const channelCols = new Set(channelColsRes.rows.map(r => r.column_name));
    const channelTypes = new Map(channelColsRes.rows.map(r => [r.column_name, r.data_type]));

    const jidColumn = channelCols.has('j_id') ? 'j_id' : (channelCols.has('jid') ? 'jid' : null);
    const qrCodeColumn = channelCols.has('qr_code') ? 'qr_code' : (channelCols.has('qrcode') ? 'qrcode' : null);
    if (!jidColumn || !qrCodeColumn) {
      throw new Error('Tabela channel_instances sem colunas de identificação esperadas (j_id/jid, qr_code/qrcode).');
    }

    const candidateColumns = [
      'id', 'name', 'type', 'company_id', 'status',
      jidColumn, 'session', qrCodeColumn, 'config',
      'flow_id', 'department_id', 'enable_chatbot_for_groups', 'open_ticket_for_groups',
      'whatsapp_name', 'telegram_token', 'active', 'fetch_messages', 'allow_all_users', 'allowed_user_ids',
      'created_at', 'updated_at', 'deleted_at', 'virtual_agent_id'
    ];
    const channelInsertColumns = candidateColumns.filter(col => channelCols.has(col));
    const channelUpdatableColumns = channelInsertColumns.filter(col => col !== 'id' && col !== 'created_at');
    const columnsSql = channelInsertColumns.join(', ');
    const upsertSetSql = channelUpdatableColumns.length
      ? channelUpdatableColumns.map(col => `${col} = EXCLUDED.${col}`).join(',\n          ')
      : 'id = EXCLUDED.id';
    const singleTupleSql = channelInsertColumns
      .map((col, idx) => `$${idx + 1}${jsonColumnCast(col, channelTypes)}`)
      .join(', ');
    const upsertSqlSingle = `
      INSERT INTO channel_instances (${columnsSql})
      VALUES (${singleTupleSql})
      ON CONFLICT (id) DO UPDATE SET
          ${upsertSetSql}
    `;

    const countRes = await source.query(
      `SELECT COUNT(*)::bigint AS total FROM "public"."Whatsapps" ${tenantId ? 'WHERE "tenantId" = $1' : ''}`,
      tenantId ? [tenantId] : []
    );
    const total = Number(countRes.rows[0]?.total || 0);
    if (!total) {
      console.log(tenantId ? `⚠️ Nenhum canal para TENANT_ID=${tenantId}.` : '⚠️ Nenhum canal na origem.');
      return;
    }

    const selectSql = `
      SELECT
        id, name, type, number,
        "tenantId" AS company_id,
        status, session, qrcode, "tokenAPI",
        "chatFlowId", "queueId",
        "is_open_ia", "isDeleted",
        "createdAt", "updatedAt"
      FROM "public"."Whatsapps"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
      ORDER BY id
    `;
    const cursor = source.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    const typeMap = {
      whatsapp: 'WhatsAppQRCode',
      waba: 'WhatsAppCloudAPI',
      instagram: 'Instagram',
      telegram: 'Telegram',
      messenger: 'Messenger'
    };

    let processed = 0, migradas = 0, ignoradasTipo = 0, erros = 0;
    const startedAt = Date.now();

    while (true) {
      const rows = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!rows?.length) break;

      // ———— garantir VirtualAgents necessários para o lote
      // 1) decidir flow_id efetivo por linha
      const needs = []; // [{company_id, flow_id, name}]
      for (const row of rows) {
        let flowId = row.chatFlowId ? String(row.chatFlowId) : null;
        if (flowId && !validFlowIds.has(flowId)) flowId = null;
        if (!flowId) {
          const fb = flowFallbackByCompany.get(String(row.company_id));
          flowId = fb ? String(fb) : null;
        }
        if (!flowId) continue; // sem flow → canal ficará sem VA
        const key = `${row.company_id}#${flowId}`;
        if (!vaCache.has(key)) needs.push({ company_id: row.company_id, flow_id: flowId, name: row.name });
      }

      if (needs.length) {
        // Consulta os VAs já existentes para estes pares
        const pairs = [...new Set(needs.map(n => `${n.company_id}#${n.flow_id}`))];
        const ph = pairs.map((_, i) => `($${i*2+1}::bigint,$${i*2+2}::bigint)`).join(',');
        const vals = pairs.flatMap(k => {
          const [c, f] = k.split('#'); return [c, f];
        });
        if (pairs.length) {
          const ex = await dest.query(
            `SELECT id, company_id, flow_id FROM virtual_agents WHERE (company_id, flow_id) IN (${ph})`, vals
          );
          for (const r of ex.rows) vaCache.set(`${r.company_id}#${r.flow_id}`, r.id);
        }

        // criar os que ainda faltam
        const toCreate = needs.filter(n => !vaCache.has(`${n.company_id}#${n.flow_id}`));
        if (toCreate.length) {
          // pegar nomes dos flows p/ compor nome do VA
          const flowIds = [...new Set(toCreate.map(n => Number(n.flow_id)))];
          const fl = await dest.query(`SELECT id, name FROM flows WHERE id = ANY($1::bigint[])`, [flowIds]);
          const flowName = new Map(fl.rows.map(r => [String(r.id), r.name || `Flow ${r.id}`]));

          const tuples = [];
          const vals2 = [];
          toCreate.forEach((n, i) => {
            const vaName = `Agente do Fluxo: ${flowName.get(String(n.flow_id)) || n.name || n.flow_id}`;
            tuples.push(`($${i*6+1},$${i*6+2}::bigint,$${i*6+3}::bigint,$${i*6+4}::boolean,$${i*6+5},$${i*6+6})`);
            vals2.push(vaName, n.company_id, n.flow_id, true, new Date(), new Date());
          });

          try {
            const ins = await dest.query(
              `INSERT INTO virtual_agents (name, company_id, flow_id, active, created_at, updated_at)
               VALUES ${tuples.join(',')}
               RETURNING id, company_id, flow_id`,
              vals2
            );
            for (const r of ins.rows) vaCache.set(`${r.company_id}#${r.flow_id}`, r.id);
          } catch (e) {
            // pode haver duplicata se outro processo criou no meio — recarrega para preencher cache
            console.warn('⚠️  Falha ao criar alguns VirtualAgents; tentando recarregar:', e.message);
            const ex2 = await dest.query(
              `SELECT id, company_id, flow_id FROM virtual_agents WHERE (company_id, flow_id) IN (${ph})`, vals
            );
            for (const r of ex2.rows) vaCache.set(`${r.company_id}#${r.flow_id}`, r.id);
          }
        }
      }

      // ———— montar INSERT dos canais
      const values = [];
      const placeholders = [];
      const perRowParams = [];

      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const mappedType = typeMap[String(row.type || '').toLowerCase()];
        if (!mappedType) { ignoradasTipo++; continue; }

        const status = 'disconnected';
        const jId = row.number || '';
        const session = null; // coluna é BYTEA na nova plataforma → manter null pra evitar type mismatch
        const qrCode = row.qrcode || '';
        const config = JSON.stringify(row.tokenAPI ? { tokenAPI: row.tokenAPI } : {});

        // flow + virtual agent
        let flowId = row.chatFlowId ? String(row.chatFlowId) : null;
        if (flowId && !validFlowIds.has(flowId)) flowId = null;
        if (!flowId) {
          const fb = flowFallbackByCompany.get(String(row.company_id));
          flowId = fb ? String(fb) : null;
        }
        const vaId = flowId ? (vaCache.get(`${row.company_id}#${flowId}`) || null) : null;

        // department
        let departmentId = row.queueId ? String(row.queueId) : null;
        if (departmentId && !validDeptIds.has(departmentId)) departmentId = null;
        if (!departmentId) {
          const fb = deptFallbackByCompany.get(String(row.company_id));
          departmentId = fb || null;
        }

        const enableBotForGroups = !!row.is_open_ia;
        const openTicketForGroups = !!row.is_open_ia;
        const deletedAt = row.isDeleted ? (row.updatedAt || new Date()) : null;
        const whatsappName = row.name || '';
        const telegramToken = mappedType === 'Telegram' ? (row.tokenAPI || '') : '';
        const active = !row.isDeleted;
        const fetchMessages = true;
        const allowAllUsers = true;
        const allowedUserIds = '[]';

        const rowValuesByColumn = {
          id: row.id,
          name: safeName(row.name, row.id),
          type: mappedType,
          company_id: row.company_id,
          status,
          [jidColumn]: jId,
          session,
          [qrCodeColumn]: qrCode,
          config,
          flow_id: flowId,
          department_id: departmentId,
          enable_chatbot_for_groups: enableBotForGroups,
          open_ticket_for_groups: openTicketForGroups,
          whatsapp_name: whatsappName,
          telegram_token: telegramToken,
          active,
          fetch_messages: fetchMessages,
          allow_all_users: allowAllUsers,
          allowed_user_ids: allowedUserIds,
          created_at: row.createdAt,
          updated_at: row.updatedAt,
          deleted_at: deletedAt,
          virtual_agent_id: vaId
        };
        const v = channelInsertColumns.map(col =>
          Object.prototype.hasOwnProperty.call(rowValuesByColumn, col) ? rowValuesByColumn[col] : null
        );
        perRowParams.push(v);

        const base = placeholders.length * channelInsertColumns.length;
        const tuple = channelInsertColumns
          .map((col, idx) => `$${base + idx + 1}${jsonColumnCast(col, channelTypes)}`)
          .join(',');
        placeholders.push(
          `(${tuple})`
        );
        values.push(...v);
      }

      if (!placeholders.length) {
        processed += rows.length;
        const elapsed = (Date.now() - startedAt) / 1000;
        bar.update(processed, { rate: (processed / Math.max(1, elapsed)).toFixed(1) });
        continue;
      }

      const upsertSql = `
        INSERT INTO channel_instances (${columnsSql})
        VALUES
          ${placeholders.join(',')}
        ON CONFLICT (id) DO UPDATE SET
          ${upsertSetSql}
      `;

      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(upsertSql, values);
        await dest.query('COMMIT');
        migradas += placeholders.length;
      } catch (batchErr) {
        await dest.query('ROLLBACK');
        // fallback por linha
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
            console.error(`❌ Erro ao migrar canal ID ${v[0]}: ${rowErr.message}`);
          }
        }
      }

      processed += rows.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      bar.update(processed, { rate: (processed / Math.max(1, elapsed)).toFixed(1) });
    }

    bar.stop();
    await new Promise((resolve, reject) => cursor.close(err => (err ? reject(err) : resolve())));
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`✅ Migrados ${migradas}/${total} canais em ${secs}s. (${ignoradasTipo} ignorados por tipo, ${erros} com erro)`);
    if (erros > 0) {
      throw new Error(`Migração de canais finalizou com ${erros} erro(s).`);
    }
  } finally {
    await source.end();
    await dest.end();
  }
};

// helpers
function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Canal ${id}`;
}
function jsonColumnCast(columnName, typesMap) {
  const t = String(typesMap.get(columnName) || '').toLowerCase();
  if (t === 'jsonb') return '::jsonb';
  if (t === 'json') return '::json';
  return '';
}
