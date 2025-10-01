// migrations/migrateQuickMessages.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

module.exports = async function migrateQuickMessages(ctx = {}) {
  console.log('⚡ Migrando "FastReply" → "quick_messages"...');

  // 6 params/linha → 5000 linhas ≈ 30k params (< 65535)
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 3000);

  // CHANGED: id de grupo para adicionar aos usuários (opcional)
  const ADD_GROUP_TO_USERS = process.env.ADD_GROUP_TO_USERS === '1'; // "1" para habilitar
  const GROUP_ID = Number(process.env.GROUP_ID || 1);

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
    application_name: 'migrateQuickMessages:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migrateQuickMessages:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // 1) COUNT para barra/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."FastReply"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
    `;
    const { rows: crows } = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(crows[0]?.total || 0);
    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhuma quick message encontrada para TENANT_ID=${tenantId}.`
          : '⚠️  Nenhuma quick message encontrada na origem.'
      );
      return;
    }

    // 2) Cursor server-side (ordem estável)
    const selectSql = `
      SELECT
        id,
        "tenantId" AS company_id,
        name,
        messages,
        "createdAt",
        "updatedAt"
      FROM "public"."FastReply"
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

    // 4) UPSERT base
    const upsertSqlSingle = `
      INSERT INTO public.quick_messages (
        id, name, messages, company_id, created_at, updated_at
      ) VALUES (
        $1, $2, $3::jsonb, $4, $5, $6
      )
      ON CONFLICT (id) DO UPDATE SET
        name       = EXCLUDED.name,
        messages   = EXCLUDED.messages,
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
      const perRowParams = []; // fallback se o batch falhar

      batch.forEach((row, i) => {
        const name = safeName(row.name, row.id);

        // CHANGED: transformar mensagens para o NOVO formato
        const msgs = normalizeMessages(row.messages);

        const v = [
          row.id,               // 1 id
          name,                 // 2 name
          JSON.stringify(msgs), // 3 messages::jsonb
          row.company_id,       // 4 company_id
          row.createdAt,        // 5 created_at
          row.updatedAt         // 6 updated_at
        ];
        perRowParams.push(v);

        const base = i * 6;
        placeholders.push(`($${base+1}, $${base+2}, $${base+3}::jsonb, $${base+4}, $${base+5}, $${base+6})`);
        values.push(...v);
      });

      // Execução do lote (multi-VALUES)
      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(
          `
          INSERT INTO public.quick_messages (
            id, name, messages, company_id, created_at, updated_at
          ) VALUES
            ${placeholders.join(',')}
          ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            messages   = EXCLUDED.messages,
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
            console.error(`❌ Erro ao migrar quick_message id=${v[0]}: ${rowErr.message}`);
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

    // CHANGED: (opcional) adicionar GROUP_ID para todos usuários do tenant
    if (ADD_GROUP_TO_USERS) {
      await addGroupToAllUsers(dest, tenantId, GROUP_ID);
    }

    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`✅ Migradas ${migradas}/${total} quick_messages em ${secs}s.${erros ? ` (${erros} com erro)` : ''}`);
    if (ADD_GROUP_TO_USERS) {
      console.log(`✅ Grupo ${GROUP_ID} adicionado ao array quick_message_groups de todos os usuários${tenantId ? ` do tenant ${tenantId}` : ''}.`);
    }
  } finally {
    await source.end();
    await dest.end();
  }
};

// --------- helpers ---------
function parseJson(v) {
  if (v == null) return null;
  if (typeof v === 'string') {
    try { return JSON.parse(v); } catch { return null; }
  }
  return v; // já é objeto/array
}

// CHANGED: normaliza para o NOVO formato de mensagens
function normalizeMessages(v) {
  const parsed = parseJson(v);

  // Se já vier no novo formato, apenas retorna (sanitizando minimamente)
  if (Array.isArray(parsed) && parsed.every(isNewMsgShape)) {
    return dedupeByShortcut(parsed.map(sanitizeNewMsg));
  }

  // Se vier no formato antigo [{key, message}]
  if (Array.isArray(parsed) && parsed.every(isOldMsgShape)) {
    const base = Date.now();
    const converted = parsed.map((it, idx) => ({
      _key: `${base + idx}-${Math.random()}`,
      shortcut: toShortcut(it.key),
      content: String(it.message ?? ''),
      active: true
    }));
    return dedupeByShortcut(converted);
  }

  // Se vier string (raro), encapsula em 1 item
  if (typeof parsed === 'string') {
    return [{
      _key: `${Date.now()}-${Math.random()}`,
      shortcut: '/msg',
      content: parsed,
      active: true
    }];
  }

  // Qualquer outra coisa vira array vazio (evita quebrar)
  return [];
}

// --------- checagens/normalizações do novo/antigo formato ---------
function isOldMsgShape(x) {
  return x && typeof x === 'object' && 'key' in x && 'message' in x;
}
function isNewMsgShape(x) {
  return x && typeof x === 'object' &&
    '_key' in x && 'shortcut' in x && 'content' in x && 'active' in x;
}
function sanitizeNewMsg(x) {
  return {
    _key: String(x._key || `${Date.now()}-${Math.random()}`),
    shortcut: toShortcut(x.shortcut),
    content: String(x.content ?? ''),
    active: Boolean(x.active !== false)
  };
}

// CHANGED: prefixo opcional com "/" (ativar com env)
function toShortcut(s) {
  const str = String(s ?? '').trim();
  if (!str) return '/vazio';
  if (process.env.NORMALIZE_SHORTCUT_SLASH === '1') {
    return str.startsWith('/') ? str : `/${str}`;
  }
  return str;
}

// Remove duplicados por "shortcut" (mantém o primeiro)
function dedupeByShortcut(arr) {
  const seen = new Set();
  const out = [];
  for (const it of arr) {
    const k = (it.shortcut || '').toLowerCase();
    if (!seen.has(k)) {
      seen.add(k);
      out.push(it);
    }
  }
  return out;
}

function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Sem nome (${id})`;
}

// CHANGED: adiciona GROUP_ID em quick_message_groups de todos usuários (tenant opcional)
async function addGroupToAllUsers(dest, tenantId, groupId) {
  const sql = `
    UPDATE public.users
    SET quick_message_groups = CASE
      WHEN quick_message_groups IS NULL THEN ARRAY[$1]::integer[]
      WHEN $1 = ANY(quick_message_groups) THEN quick_message_groups
      ELSE array_append(quick_message_groups, $1)
    END,
    updated_at = NOW()
    ${tenantId ? 'WHERE company_id = $2' : ''}
  `;
  const params = tenantId ? [groupId, tenantId] : [groupId];

  await dest.query('BEGIN');
  try {
    await dest.query(sql, params);
    await dest.query('COMMIT');
  } catch (e) {
    await dest.query('ROLLBACK');
    console.error(`❌ Erro ao atualizar quick_message_groups: ${e.message}`);
  }
}
