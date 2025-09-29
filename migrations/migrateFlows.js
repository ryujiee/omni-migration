// migrations/migrateFlows.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateFlows(ctx = {}) {
  console.log('🔄 Migrando "ChatFlow" → "flows"...');

  // 6 params/linha → 5000 linhas ≈ 30k params (< 65535). Ajuste via .env
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 2000);

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
    application_name: 'migrateFlows:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateFlows:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // —— Detecta coluna de definição (preferência: flow > content > definition > graph)
    const preferredCols = ['flow', 'content', 'definition', 'graph'];
    const colRes = await source.query(
      `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name='ChatFlow'
        AND column_name = ANY($1)
      `,
      [preferredCols]
    );
    const found = new Set(colRes.rows.map(r => r.column_name));
    const flowCol = preferredCols.find(c => found.has(c)) || null;
    const flowExpr = flowCol ? `"${flowCol}"` : `'{}'::jsonb`;

    // —— Detecta se há coluna isDeleted para filtrar
    const delRes = await source.query(
      `
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name='ChatFlow' AND column_name='isDeleted'
      `
    );
    const hasIsDeleted = delRes.rowCount > 0;

    // —— Monta WHERE dinamicamente
    const whereParts = [];
    const params = [];
    if (hasIsDeleted) whereParts.push(`"isDeleted" IS DISTINCT FROM true`);
    if (tenantId) {
      whereParts.push(`"tenantId" = $${params.length + 1}`);
      params.push(tenantId);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';

    // —— Count p/ barra/ETA
    const countSql = `SELECT COUNT(*)::bigint AS total FROM "public"."ChatFlow" ${whereClause}`;
    const countRes = await source.query(countSql, params);
    const total = Number(countRes.rows[0]?.total || 0);

    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhum fluxo encontrado para TENANT_ID=${tenantId}.`
          : '⚠️  Nenhum fluxo encontrado na origem.'
      );
      return;
    }

    // —— Cursor server-side (ordem estável)
    const selectSql = `
      SELECT
        id,
        name,
        "tenantId" AS company_id,
        ${flowExpr} AS flow,
        "createdAt",
        "updatedAt"
      FROM "public"."ChatFlow"
      ${whereClause}
      ORDER BY id
    `;
    const cursor = source.query(new Cursor(selectSql, params));

    // —— Barra de progresso
    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    const startedAt = Date.now();
    let processed = 0;
    let migrados = 0;
    let erros = 0;

    // —— UPSERT (preserva flow existente se não for {})
    const upsertSql = `
      INSERT INTO flows (
        id, name, flow, company_id, created_at, updated_at
      ) VALUES (
        $1, $2, $3::jsonb, $4, $5, $6
      )
      ON CONFLICT (id) DO UPDATE SET
        name       = EXCLUDED.name,
        company_id = EXCLUDED.company_id,
        updated_at = EXCLUDED.updated_at,
        flow       = COALESCE(NULLIF(flows.flow, '{}'::jsonb), EXCLUDED.flow)
    `;

    // —— Loop por lote
    while (true) {
      const batch = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!batch || batch.length === 0) break;

      const placeholders = [];
      const values = [];
      const perRowParams = []; // fallback se batch falhar

      batch.forEach((row, i) => {
        const name = safeName(row.name, row.id);
        const flowJson = normalizeJsonb(row.flow); // garante objeto/array/json

        const v = [
          row.id,                    // 1
          name,                      // 2
          JSON.stringify(flowJson),  // 3
          row.company_id,            // 4
          row.createdAt,             // 5
          row.updatedAt              // 6
        ];
        perRowParams.push(v);

        const base = i * 6;
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}::jsonb, $${base + 4}, $${base + 5}, $${base + 6})`
        );
        values.push(...v);
      });

      // Execução do lote
      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        // multi-VALUES (mais rápido)
        await dest.query(
          `
          INSERT INTO flows (
            id, name, flow, company_id, created_at, updated_at
          ) VALUES
            ${placeholders.join(',')}
          ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            company_id = EXCLUDED.company_id,
            updated_at = EXCLUDED.updated_at,
            flow       = COALESCE(NULLIF(flows.flow, '{}'::jsonb), EXCLUDED.flow)
          `,
          values
        );
        await dest.query('COMMIT');
        migrados += batch.length;
      } catch (batchErr) {
        await dest.query('ROLLBACK');
        // fallback row-a-row pra não perder o lote
        for (const v of perRowParams) {
          try {
            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');
            await dest.query(upsertSql, v);
            await dest.query('COMMIT');
            migrados += 1;
          } catch (rowErr) {
            await dest.query('ROLLBACK');
            erros += 1;
            console.error(`❌ Erro ao migrar flow id=${v[0]}: ${rowErr.message}`);
          }
        }
      }

      processed += batch.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = (processed / Math.max(1, elapsed)).toFixed(1);
      bar.update(processed, { rate });
    }

    bar.stop();
    await new Promise((resolve, reject) => cursor.close(err => (err ? reject(err) : resolve())));
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`✅ Migrados ${migrados}/${total} fluxo(s) em ${secs}s. (${erros} com erro)`);
  } finally {
    await source.end();
    await dest.end();
  }
};

// —— helpers
function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Fluxo ${id}`;
}
function parseJson(v) {
  if (v == null) return null;
  if (typeof v === 'string') {
    try { return JSON.parse(v); } catch { return null; }
  }
  return v; // já é objeto/array/jsonb
}
function normalizeJsonb(v) {
  const parsed = parseJson(v);
  return (parsed && typeof parsed === 'object') ? parsed : {};
}
