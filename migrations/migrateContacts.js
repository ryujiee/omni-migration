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

    // --- 3) Pré-carrega informações de TAGS no DESTINO por company_id
    const companyIds = [...new Set(contacts.rows.map(r => r.tenantId).filter(v => v != null))];
    // 👉 Se company_id for BIGINT, troque ::int[] por ::bigint[]
    const tagInfoRes = await dest.query(
      `SELECT id, name, color, company_id
       FROM tags
       ${companyIds.length ? 'WHERE company_id = ANY($1::int[])' : ''}`,
      companyIds.length ? [companyIds] : []
    );
    // Map<company_id, Map<tag_id, {name,color}>>
    const tagInfoByCompany = new Map();
    for (const t of tagInfoRes.rows) {
      const cid = String(t.company_id);
      if (!tagInfoByCompany.has(cid)) tagInfoByCompany.set(cid, new Map());
      tagInfoByCompany.get(cid).set(Number(t.id), { name: t.name, color: t.color });
    }

    // --- 4) UPSERT preparado
    const upsertSql = `
      INSERT INTO contacts (
        id, name, phone_number, j_id, telephone_number, whatsapp,
        instagram, telegram, messenger, email, profile_pic_url, push_name,
        is_wa_contact, is_group, type, cpf, cnpj, birth_date,
        address, annotations, company_id, created_at, updated_at,
        tags, channel_assignments
      )
      VALUES (
        $1, $2, $3, $4, NULL, $5,
        $6, $7, $8, $9, $10, $11,
        $12, $13, $14, $15, $16, $17,
        $18, $19, $20, $21, $22,
        $23::jsonb, $24::jsonb
      )
      ON CONFLICT (id) DO UPDATE SET
        name=EXCLUDED.name, phone_number=EXCLUDED.phone_number, j_id=EXCLUDED.j_id,
        whatsapp=EXCLUDED.whatsapp, instagram=EXCLUDED.instagram, telegram=EXCLUDED.telegram,
        messenger=EXCLUDED.messenger, email=EXCLUDED.email, profile_pic_url=EXCLUDED.profile_pic_url,
        push_name=EXCLUDED.push_name, is_wa_contact=EXCLUDED.is_wa_contact, is_group=EXCLUDED.is_group,
        type=EXCLUDED.type, cpf=EXCLUDED.cpf, cnpj=EXCLUDED.cnpj, birth_date=EXCLUDED.birth_date,
        address=EXCLUDED.address, annotations=EXCLUDED.annotations, company_id=EXCLUDED.company_id,
        tags=EXCLUDED.tags, channel_assignments=EXCLUDED.channel_assignments, updated_at=EXCLUDED.updated_at
    `;
    const upsertNamed = { name: 'upsert_contact', text: upsertSql };

    // --- 5) Transação + desempenho
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

      // --- TAGS no formato NOVO [{id,name,color,auto_assign}]
      const compKey = String(row.tenantId);
      const infoMap = tagInfoByCompany.get(compKey) || new Map();
      const tagsOld = Array.isArray(tagsIdMap[contactId]) ? tagsIdMap[contactId] : [];

      const tags = tagsOld.map((tagIdNum) => {
        const tagId = Number(tagIdNum);
        const info = infoMap.get(tagId);
        return {
          id: tagId,
          name: info?.name ?? `Tag ${tagId}`,
          color: info?.color ?? '#999999',
          auto_assign: false
        };
      });

      await dest.query({
        ...upsertNamed,
        values: [
          contactId,
          row.name || `Contato ${contactId}`,
          phoneNumber,
          jId,
          whatsapp,
          row.instagramPK?.toString() ?? null,
          row.telegramId?.toString() ?? null,
          row.messengerId || null,
          row.email || null,
          row.profilePicUrl || null,
          row.pushname || null,
          isWaContact,
          isGroup,
          type,
          row.cpf || null,
          row.cnpj || null,
          row.dataNascimento || null, // se quiser, troque por parseDate seguro
          address || '',
          '',
          row.tenantId,
          row.createdAt,
          row.updatedAt,
          JSON.stringify(tags),
          JSON.stringify(channelAssignments)
        ]
      });

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
