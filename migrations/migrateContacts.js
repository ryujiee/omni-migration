// migrations/migrateContacts.js  (versão com transação + progresso + tags no formato novo)
'use strict';

require('dotenv').config();
const { Client } = require('pg');

const LOG_EVERY = Number(process.env.LOG_EVERY || 200); // log a cada N contatos

module.exports = async function migrateContacts(ctx = {}) {
  console.log('📇 Migrando "Contacts" → "contacts"...');

  const tenantId =
    ctx.tenantId != null && String(ctx.tenantId).trim() !== ''
      ? String(ctx.tenantId).trim()
      : (process.env.TENANT_ID ? String(process.env.TENANT_ID).trim() : null);

  const source = new Client({
    host: process.env.SRC_HOST, port: process.env.SRC_PORT,
    user: process.env.SRC_USER, password: process.env.SRC_PASS,
    database: process.env.SRC_DB
  });
  const dest = new Client({
    host: process.env.DST_HOST, port: process.env.DST_PORT,
    user: process.env.DST_USER, password: process.env.DST_PASS,
    database: process.env.DST_DB
  });

  await source.connect();
  await dest.connect();

  try {
    // --- 1) Carrega contatos da origem
    const contactsSql = `
      SELECT
        id, name, number, "profilePicUrl", email, "isGroup",
        "telegramId", "instagramPK", "messengerId",
        "tipo", cpf, cnpj, "dataNascimento",
        rua, bairro, cep, cidade, estado, pais,
        pushname, "tenantId", "createdAt", "updatedAt"
      FROM "public"."Contacts"
      ${tenantId ? 'WHERE "tenantId" = $1' : ''}
      ORDER BY id
    `;
    const contacts = await source.query(contactsSql, tenantId ? [tenantId] : []);
    if (contacts.rowCount === 0) {
      console.log(tenantId
        ? `⚠️  Nenhum contato encontrado para TENANT_ID=${tenantId}.`
        : '⚠️  Nenhum contato encontrado na origem.');
      return;
    }
    const total = contacts.rowCount;

    // --- 2) Pré-carrega relações de tags/carteiras do LOTE TODO
    const ids = contacts.rows.map(r => r.id);
    // 👉 Se as colunas "contactId" forem BIGINT, troque ::int[] por ::bigint[]
    const tagsByContact = await source.query(
      `SELECT "contactId", array_agg(DISTINCT "tagId") AS tag_ids
       FROM "public"."ContactTags"
       WHERE "contactId" = ANY($1::int[])
       GROUP BY "contactId"`, [ids]
    );
    const channelsByContact = await source.query(
      `SELECT "contactId", array_agg(DISTINCT "channelId") AS channel_ids
       FROM "public"."ContactWallets"
       WHERE "contactId" = ANY($1::int[]) AND "channelId" IS NOT NULL
       GROUP BY "contactId"`, [ids]
    );

    const tagsIdMap = Object.fromEntries(
      tagsByContact.rows.map(r => [r.contactId, r.tag_ids || []])
    );
    const channelsMap = Object.fromEntries(
      channelsByContact.rows.map(r => [r.contactId, r.channel_ids || []])
    );

    // --- 3) Detecta schema do destino e prepara UPSERT dinâmico
    const contactColsRes = await dest.query(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = 'contacts'
    `);
    const contactCols = new Set(contactColsRes.rows.map(r => r.column_name));
    const contactTypes = new Map(contactColsRes.rows.map(r => [r.column_name, r.data_type]));
    const jidColumn = contactCols.has('j_id') ? 'j_id' : (contactCols.has('jid') ? 'jid' : null);
    if (!jidColumn) {
      throw new Error('Tabela contacts sem coluna j_id/jid.');
    }

    const candidateColumns = [
      'id', 'name', 'phone_number', jidColumn, 'telephone_number', 'whatsapp',
      'instagram', 'instagram_id', 'telegram', 'messenger', 'email', 'profile_pic_url', 'push_name',
      'is_wa_contact', 'is_group', 'type', 'cpf', 'cnpj', 'birth_date',
      'address', 'annotations', 'channel_id', 'company_id', 'created_at', 'updated_at',
      'tags', 'channel_assignments'
    ];
    const insertColumns = candidateColumns.filter(col => contactCols.has(col));
    const updatableColumns = insertColumns.filter(col => col !== 'id' && col !== 'created_at');
    const columnsSql = insertColumns.join(', ');
    const tupleSql = insertColumns
      .map((col, idx) => `$${idx + 1}${jsonColumnCast(col, contactTypes)}`)
      .join(', ');
    const setSql = updatableColumns.length
      ? updatableColumns.map(col => `${col} = EXCLUDED.${col}`).join(', ')
      : 'id = EXCLUDED.id';
    const upsertSql = `
      INSERT INTO contacts (${columnsSql})
      VALUES (${tupleSql})
      ON CONFLICT (id) DO UPDATE SET ${setSql}
    `;

    // --- 4) Transação + desempenho
    await dest.query('BEGIN');
    await dest.query('SET LOCAL synchronous_commit TO OFF');

    let migrados = 0;
    const t0 = Date.now();

    for (const row of contacts.rows) {
      const contactId = row.id;

      const rawNumber = onlyDigits(row.number);
      const phoneNumber = truncate(rawNumber, 20) || null;
      const whatsapp = phoneNumber;

      const isGroup = row.isGroup === true;
      const isWaContact = !!phoneNumber;
      const jId = phoneNumber ? `${phoneNumber}${isGroup ? '@g.us' : '@s.whatsapp.net'}` : null;

      const address = [row.rua, row.bairro, row.cep, row.cidade, row.estado, row.pais]
        .filter(Boolean).join(', ');

      const type = row.tipo === 'Pessoa Jurídica' ? 2 : 1;

      // Channel assignments
      const channelIds = Array.isArray(channelsMap[contactId]) ? channelsMap[contactId] : [];
      const channelAssignments = {};
      for (const chId of channelIds) channelAssignments[chId] = { assigned: true };
      const primaryChannelId = channelIds.length ? Number(channelIds[0]) : null;

      // --- TAGS no formato da plataforma nova [{tag, auto_assign}]
      const tagsOld = Array.isArray(tagsIdMap[contactId]) ? tagsIdMap[contactId] : [];
      const tags = tagsOld
        .map(tagIdNum => Number(tagIdNum))
        .filter(Number.isInteger)
        .map(tagId => ({ tag: tagId, auto_assign: false }));

      const valuesByColumn = {
        id: contactId,
        name: row.name || `Contato ${contactId}`,
        phone_number: phoneNumber,
        [jidColumn]: jId,
        telephone_number: null,
        whatsapp,
        instagram: row.instagramPK?.toString() ?? null,
        instagram_id: row.instagramPK?.toString() ?? null,
        telegram: row.telegramId?.toString() ?? null,
        messenger: row.messengerId || null,
        email: row.email || null,
        profile_pic_url: row.profilePicUrl || null,
        push_name: row.pushname || null,
        is_wa_contact: isWaContact,
        is_group: isGroup,
        type,
        cpf: row.cpf || null,
        cnpj: row.cnpj || null,
        birth_date: row.dataNascimento || null,
        address: address || '',
        annotations: '',
        channel_id: primaryChannelId,
        company_id: row.tenantId,
        created_at: row.createdAt,
        updated_at: row.updatedAt,
        tags: JSON.stringify(tags),
        channel_assignments: JSON.stringify(channelAssignments)
      };
      const values = insertColumns.map(col =>
        Object.prototype.hasOwnProperty.call(valuesByColumn, col) ? valuesByColumn[col] : null
      );
      await dest.query(upsertSql, values);

      migrados++;
      if (migrados % LOG_EVERY === 0 || migrados === total) {
        const dt = (Date.now() - t0) / 1000;
        const rps = (migrados / Math.max(0.001, dt)).toFixed(1);
        const pct = ((migrados / total) * 100).toFixed(1);
        const eta = total > 0 ? Math.max(0, (dt / migrados) * (total - migrados)) : 0;
        console.log(`→ ${migrados}/${total} (${pct}%) • ${rps} r/s • ETA ~ ${eta.toFixed(0)}s`);
      }
    }

    await dest.query('COMMIT');
    console.log(`✅ Migrados ${migrados}/${total} contato(s).`);
  } catch (e) {
    try { await dest.query('ROLLBACK'); } catch { /* ignore */ }
    throw e;
  } finally {
    await source.end();
    await dest.end();
  }
};

// helpers
function onlyDigits(v) { if (v == null) return null; return String(v).replace(/\D+/g, ''); }
function truncate(v, max) { if (v == null) return v; const s = String(v); return s.length > max ? s.slice(0, max) : s; }
function jsonColumnCast(columnName, typesMap) {
  const t = String(typesMap.get(columnName) || '').toLowerCase();
  if (t === 'jsonb') return '::jsonb';
  if (t === 'json') return '::json';
  return '';
}
