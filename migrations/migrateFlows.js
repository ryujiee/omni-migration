// migrations/migrateFlows.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateFlows(ctx = {}) {
  console.log('🔄 Migrando "ChatFlow" → "flows" + "virtual_agents"...');

  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);

  const tenantId =
    ctx.tenantId != null && String(ctx.tenantId).trim() !== ''
      ? String(ctx.tenantId).trim()
      : (process.env.TENANT_ID ? String(process.env.TENANT_ID).trim() : null);

  const source = new Client({
    host: process.env.SRC_HOST, port: process.env.SRC_PORT,
    user: process.env.SRC_USER, password: process.env.SRC_PASS,
    database: process.env.SRC_DB, application_name: 'migrateFlows:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST, port: process.env.DST_PORT,
    user: process.env.DST_USER, password: process.env.DST_PASS,
    database: process.env.DST_DB, application_name: 'migrateFlows:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // Detecta coluna que guarda o JSON do fluxo
    const preferredCols = ['flow', 'content', 'definition', 'graph'];
    const colRes = await source.query(
      `SELECT column_name
         FROM information_schema.columns
        WHERE table_schema='public' AND table_name='ChatFlow'
          AND column_name = ANY($1)`,
      [preferredCols]
    );
    const found = new Set(colRes.rows.map(r => r.column_name));
    const flowCol = preferredCols.find(c => found.has(c)) || null;
    const flowExpr = flowCol ? `"${flowCol}"` : `'{}'::jsonb`;

    // Filtro isDeleted (se existir)
    const delRes = await source.query(
      `SELECT 1
         FROM information_schema.columns
        WHERE table_schema='public' AND table_name='ChatFlow' AND column_name='isDeleted'`
    );
    const hasIsDeleted = delRes.rowCount > 0;

    const where = [];
    const params = [];
    if (hasIsDeleted) where.push(`"isDeleted" IS DISTINCT FROM true`);
    if (tenantId) { where.push(`"tenantId" = $${params.length + 1}`); params.push(tenantId); }
    const whereClause = where.length ? `WHERE ${where.join(' AND ')}` : '';

    const { rows: [{ total }] } = await dest.query(
      `SELECT COUNT(*)::bigint AS total FROM (SELECT 1) x` // só p/ manter estrutura
    );

    const countRes = await source.query(
      `SELECT COUNT(*)::bigint AS total FROM "public"."ChatFlow" ${whereClause}`, params
    );
    const totalSrc = Number(countRes.rows[0]?.total || 0);
    if (!totalSrc) {
      console.log(tenantId ? `⚠️  Nenhum fluxo para TENANT_ID=${tenantId}.` : '⚠️  Nenhum fluxo na origem.');
      return;
    }

    const selectSql = `
      SELECT id, name, "tenantId" AS company_id, ${flowExpr} AS flow, "createdAt", "updatedAt"
        FROM "public"."ChatFlow"
        ${whereClause}
       ORDER BY id
    `;
    const cursor = source.query(new Cursor(selectSql, params));

    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(totalSrc, 0, { rate: '0.0' });

    const upsertFlowSql = `
      INSERT INTO flows (id, name, flow, company_id, created_at, updated_at)
      VALUES ($1, $2, $3::jsonb, $4, $5, $6)
      ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        company_id = EXCLUDED.company_id,
        updated_at = EXCLUDED.updated_at,
        flow = COALESCE(NULLIF(flows.flow, '{}'::jsonb), EXCLUDED.flow)
    `;

    let processed = 0, migrados = 0, vaCriados = 0, erros = 0;
    const startedAt = Date.now();

    while (true) {
      const batch = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!batch?.length) break;

      // 1) UPSERT dos flows
      await dest.query('BEGIN');
      try {
        for (const row of batch) {
          const flowJson = normalizeJsonb(row.flow);
          await dest.query(upsertFlowSql, [
            row.id,
            safeName(row.name, row.id),
            JSON.stringify(flowJson),
            row.company_id,
            row.createdAt,
            row.updatedAt
          ]);
          migrados++;
        }
        await dest.query('COMMIT');
      } catch (e) {
        await dest.query('ROLLBACK');
        erros += batch.length;
        console.error('❌ Falha ao upsert flows (lote):', e.message);
      }

      // 2) Garantir VirtualAgent (1 por company_id + flow_id)
      //    Busca existentes p/ evitar duplicatas
      const pairs = batch.map(r => [r.company_id, r.id]);
      // consulta por pares
      const ph = pairs.map((_, i) => `($${i*2+1}::bigint,$${i*2+2}::bigint)`).join(',');
      const vals = pairs.flat();
      const ex = await dest.query(
        `SELECT id, company_id, flow_id
           FROM virtual_agents
          WHERE (company_id, flow_id) IN (${ph})`, vals
      );
      const hasPair = new Set(ex.rows.map(r => `${r.company_id}#${r.flow_id}`));

      // carrega nomes dos flows p/ nomear agentes
      const flowIds = [...new Set(batch.map(r => r.id))];
      const fl = await dest.query(
        `SELECT id, name FROM flows WHERE id = ANY($1::bigint[])`, [flowIds]
      );
      const nameByFlow = new Map(fl.rows.map(r => [String(r.id), r.name || `Flow ${r.id}`]));

      const toCreate = [];
      for (const r of batch) {
        const key = `${r.company_id}#${r.id}`;
        if (!hasPair.has(key)) {
          const vaName = `Agente do Fluxo: ${nameByFlow.get(String(r.id)) || r.name || r.id}`;
          toCreate.push([vaName, r.company_id, r.id, true, r.createdAt, r.updatedAt]);
        }
      }
      if (toCreate.length) {
        const ph2 = toCreate.map((_, i) =>
          `($${i*6+1},$${i*6+2}::bigint,$${i*6+3}::bigint,$${i*6+4}::boolean,$${i*6+5},$${i*6+6})`
        ).join(',');
        const vals2 = toCreate.flat();
        try {
          await dest.query(
            `INSERT INTO virtual_agents (name, company_id, flow_id, active, created_at, updated_at)
             VALUES ${ph2}`, vals2
          );
          vaCriados += toCreate.length;
        } catch (e) {
          // Se não houver índice único (company_id, flow_id), podem aparecer duplicatas em concorrência.
          // Se isso for um problema, crie um unique index: 
          // CREATE UNIQUE INDEX IF NOT EXISTS ux_virtual_agents_company_flow ON virtual_agents(company_id, flow_id);
          console.warn('⚠️  Falha ao criar alguns VirtualAgents (possível duplicata):', e.message);
        }
      }

      processed += batch.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      bar.update(processed, { rate: (processed / Math.max(1, elapsed)).toFixed(1) });
    }

    bar.stop();
    await new Promise((res, rej) => cursor.close(err => err ? rej(err) : res()));
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`✅ Flows migrados: ${migrados}/${totalSrc} • VirtualAgents criados: ${vaCriados} • em ${secs}s. (${erros} erro(s))`);
  } finally {
    await source.end();
    await dest.end();
  }
};

// helpers
function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Fluxo ${id}`;
}
function parseJson(v) {
  if (v == null) return null;
  if (typeof v === 'string') { try { return JSON.parse(v); } catch { return null; } }
  return v;
}
function normalizeJsonb(v) {
  const parsed = parseJson(v);
  return (parsed && typeof parsed === 'object') ? parsed : {};
}
