// migrations/migrateTags.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateTags(ctx = {}) {
  console.log('🏷️ Migrando "Tags" → "tags"...');

  // 7 params/linha → 5000 linhas ≈ 35k params (< 65535)
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 3000);

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
    application_name: 'migrateTags:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateTags:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // 1) COUNT p/ barra/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."Tags"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
    `;
    const { rows: crows } = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(crows[0]?.total || 0);
    if (!total) {
      console.log(
        tenantId ? `⚠️  Nenhuma tag encontrada para TENANT_ID=${tenantId}.`
                 : '⚠️  Nenhuma tag encontrada na origem.'
      );
      return;
    }

    // 2) Cursor server-side (ordem estável)
    const selectSql = `
      SELECT
        id,
        "tag"       AS name,
        "color",
        COALESCE("isActive", true) AS active,
        "tenantId"  AS company_id,
        "createdAt",
        "updatedAt"
      FROM "public"."Tags"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
      ORDER BY id
    `;
    const cursor = source.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    // 3) Barra de progresso
    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    // 4) Upsert single (fallback)
    const upsertSqlSingle = `
      INSERT INTO tags (
        id, name, color, active, is_public, company_id, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8
      )
      ON CONFLICT (id) DO UPDATE SET
        name       = EXCLUDED.name,
        color      = EXCLUDED.color,
        active     = EXCLUDED.active,
        is_public  = EXCLUDED.is_public,
        company_id = EXCLUDED.company_id,
        updated_at = EXCLUDED.updated_at
    `;

    const startedAt = Date.now();
    let processed = 0;
    let migradas = 0;
    let erros = 0;

    // 5) Loop por lote
    while (true) {
      const batch = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!batch || batch.length === 0) break;

      const placeholders = [];
      const values = [];
      const perRowParams = [];

      batch.forEach((row, i) => {
        const name = safeName(row.name, row.id);
        const color = normalizeColor(row.color);
        const active = row.active !== false; // default true se nulo

        const v = [
          row.id,          // 1
          name,            // 2
          color,           // 3
          active,          // 4
          true,            // 5 is_public
          row.company_id,  // 6
          row.createdAt,   // 7
          row.updatedAt    // 8
        ];
        perRowParams.push(v);

        const base = i * 8;
        placeholders.push(
          `($${base+1}, $${base+2}, $${base+3}, $${base+4}, $${base+5}, $${base+6}, $${base+7}, $${base+8})`
        );
        values.push(...v);
      });

      // Execução do lote
      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(
          `
          INSERT INTO tags (
            id, name, color, active, is_public, company_id, created_at, updated_at
          ) VALUES
            ${placeholders.join(',')}
          ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            color      = EXCLUDED.color,
            active     = EXCLUDED.active,
            is_public  = EXCLUDED.is_public,
            company_id = EXCLUDED.company_id,
            updated_at = EXCLUDED.updated_at
          `,
          values
        );
        await dest.query('COMMIT');
        migradas += batch.length;
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
            console.error(`❌ Erro ao migrar tag id=${v[0]}: ${rowErr.message}`);
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
    console.log(`✅ Migradas ${migradas}/${total} tag(s) em ${secs}s.${erros ? ` (${erros} com erro)` : ''}`);
  } finally {
    await source.end();
    await dest.end();
  }
};

// —— helpers
function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Tag ${id}`;
}

function normalizeColor(v) {
  if (!v) return '#999999';
  let s = String(v).trim();

  // rgb(255, 0, 128)
  const mRgb = /^rgb\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*\)$/i.exec(s);
  if (mRgb) {
    const [r, g, b] = mRgb.slice(1, 4).map(n => clampInt(+n, 0, 255));
    return `#${toHex2(r)}${toHex2(g)}${toHex2(b)}`;
  }

  // #abc → #aabbcc
  const mHex3 = /^#?([0-9a-f]{3})$/i.exec(s);
  if (mHex3) {
    const [a,b,c] = mHex3[1].split('');
    return `#${a}${a}${b}${b}${c}${c}`.toLowerCase();
  }

  // #aabbcc
  const mHex6 = /^#?([0-9a-f]{6})$/i.exec(s);
  if (mHex6) return `#${mHex6[1].toLowerCase()}`;

  // fallback
  return '#999999';
}
function clampInt(n, min, max) {
  if (!Number.isFinite(n)) return min;
  return Math.max(min, Math.min(max, Math.trunc(n)));
}
function toHex2(n) {
  const s = clampInt(n, 0, 255).toString(16);
  return s.length === 1 ? '0' + s : s;
}
