// migrations/migratePermissions.batched.js
'use strict';

require('dotenv').config();
const { Client } = require('pg');
const Cursor = require('pg-cursor');
const cliProgress = require('cli-progress');

// === Default da plataforma (espelhando seu Go) ===
const DefaultPermissions = {
  Dashboard: { access: true, geral: true, atendente: true },

  Tickets: {
    access: true,
    sendMessage: true,
    openTicket: true,
    filterSeeAll: true,
    manage: true,
    viewTicketContact: true
  },

  InternalChat: { access: true },

  Users: { access: true, edit: true, create: true },
  Departments: { access: true, edit: true, create: true },
  Permissions: { access: true, edit: true, create: true },
  Channels: { access: true, edit: true, create: true },

  Tags: { access: true, edit: true, create: true, delete: true },

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
  ReportsContact: { access: true },

  ApiDocs: { access: true },

  Timeline: { access: true, seeAll: true },

  TicketsPanel: { access: true },

  Kanban: { access: true, edit: true, create: true, delete: true },
  KanbanBoard: { access: true, edit: true, create: true, delete: true }
};

module.exports = async function migratePermissions(ctx = {}) {
  console.log('🔐 Migrando "Permissions" → "permissions"...');

  // 6 params/linha → 5000 linhas ≈ 30k params (< 65535)
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
    application_name: 'migratePermissions:source'
  });

  const dest = new Client({
    host: process.env.DST_HOST,
    port: process.env.DST_PORT,
    user: process.env.DST_USER,
    password: process.env.DST_PASS,
    database: process.env.DST_DB,
    application_name: 'migratePermissions:dest'
  });

  await source.connect();
  await dest.connect();

  try {
    // —— COUNT para barra/ETA
    const countSql = `
      SELECT COUNT(*)::bigint AS total
      FROM "public"."Permissions"
      WHERE id != 0
      ${tenantId ? 'AND "tenantId" = $1' : ''}
    `;
    const countRes = await source.query(countSql, tenantId ? [tenantId] : []);
    const total = Number(countRes.rows[0]?.total || 0);
    if (!total) {
      console.log(
        tenantId
          ? `⚠️  Nenhuma permissão encontrada para TENANT_ID=${tenantId} (id != 0).`
          : '⚠️  Nenhuma permissão encontrada na origem (id != 0).'
      );
      return;
    }

    // —— Cursor server-side
    const selectSql = `
      SELECT id, name, permissions, "tenantId" AS company_id, "createdAt", "updatedAt"
      FROM "public"."Permissions"
      WHERE id != 0
      ${tenantId ? 'AND "tenantId" = $1' : ''}
      ORDER BY id
    `;
    const cursor = source.query(new Cursor(selectSql, tenantId ? [tenantId] : []));

    // —— Barra de progresso
    const bar = new cliProgress.SingleBar(
      { format: 'Progresso |{bar}| {percentage}% | {value}/{total} | ETA: {eta_formatted} | {rate} r/s', hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    bar.start(total, 0, { rate: '0.0' });

    const upsertSqlSingle = `
      INSERT INTO permissions (id, name, permissions, company_id, created_at, updated_at)
      VALUES ($1, $2, $3::jsonb, $4, $5, $6)
      ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        permissions = EXCLUDED.permissions,
        company_id = EXCLUDED.company_id,
        updated_at = EXCLUDED.updated_at
    `;

    const startedAt = Date.now();
    let processed = 0;
    let ok = 0;
    let erros = 0;

    while (true) {
      const batch = await new Promise((resolve, reject) => {
        cursor.read(BATCH_SIZE, (err, r) => (err ? reject(err) : resolve(r)));
      });
      if (!batch || batch.length === 0) break;

      const placeholders = [];
      const values = [];
      const perRowParams = [];

      batch.forEach((row, i) => {
        try {
          const oldPerms = parseJson(row.permissions);
          if (!oldPerms || typeof oldPerms !== 'object') throw new Error('permissions inválido/indefinido');

          const converted = convertPermissions(oldPerms);
          const jsonFinal = JSON.stringify(converted);

          const v = [
            row.id,
            safeName(row.name, row.id),
            jsonFinal,
            row.company_id,
            row.createdAt,
            row.updatedAt
          ];
          perRowParams.push(v);

          const base = i * 6;
          placeholders.push(`($${base+1}, $${base+2}, $${base+3}::jsonb, $${base+4}, $${base+5}, $${base+6})`);
          values.push(...v);
        } catch (e) {
          // se falhar parse/conversão, deixa para fallback row-a-row com try/catch
          const converted = JSON.stringify(DefaultPermissions);
          const v = [
            row.id,
            safeName(row.name, row.id),
            converted,
            row.company_id,
            row.createdAt,
            row.updatedAt
          ];
          perRowParams.push(v);

          const base = i * 6;
          placeholders.push(`($${base+1}, $${base+2}, $${base+3}::jsonb, $${base+4}, $${base+5}, $${base+6})`);
          values.push(...v);
          console.debug(`🧾 JSON da permissão com possível problema em '${row.name}':`, row.permissions);
        }
      });

      try {
        await dest.query('BEGIN');
        await dest.query('SET LOCAL synchronous_commit TO OFF');
        await dest.query(
          `
          INSERT INTO permissions (id, name, permissions, company_id, created_at, updated_at)
          VALUES ${placeholders.join(',')}
          ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            permissions = EXCLUDED.permissions,
            company_id = EXCLUDED.company_id,
            updated_at = EXCLUDED.updated_at
          `,
          values
        );
        await dest.query('COMMIT');
        ok += batch.length;
      } catch (batchErr) {
        await dest.query('ROLLBACK');
        // fallback: registro a registro (pra não perder o lote inteiro)
        for (const v of perRowParams) {
          try {
            await dest.query('BEGIN');
            await dest.query('SET LOCAL synchronous_commit TO OFF');
            await dest.query(upsertSqlSingle, v);
            await dest.query('COMMIT');
            ok += 1;
          } catch (rowErr) {
            await dest.query('ROLLBACK');
            erros += 1;
            console.error(`❌ Falha ao migrar permission id=${v[0]}: ${rowErr.message}`);
          }
        }
      }

      processed += batch.length;
      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = (processed / Math.max(1, elapsed)).toFixed(1);
      bar.update(processed, { rate });
    }

    bar.stop();
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`✅ Migração de permissões concluída. (${ok}/${total}) em ${secs}s${erros ? `, ${erros} com erro` : ''}.`);
  } finally {
    await source.end();
    await dest.end();
  }
};

// ---------- Helpers ----------

function parseJson(v) {
  try {
    if (v == null) return null;
    if (typeof v === 'string') return JSON.parse(v);
    return v;
  } catch {
    return null;
  }
}

function safeName(name, id) {
  const n = (name || '').toString().trim();
  return n.length ? n : `Perfil ${id}`;
}

// Converte o JSON legado para o novo formato + completa com DefaultPermissions
function convertPermissions(oldPerms) {
  const converted = {};

  for (const [modAntigo, acoesAntigas] of Object.entries(oldPerms || {})) {
    const moduloNovo = mapModulo(modAntigo);
    if (!moduloNovo || !DefaultPermissions[moduloNovo]) continue;

    const acoesObj = acoesAntigas && typeof acoesAntigas === 'object' ? acoesAntigas : {};
    converted[moduloNovo] = {};

    for (const [acaoAntiga, valor] of Object.entries(acoesObj)) {
      const acaoNova = mapAcao(acaoAntiga, moduloNovo);
      if (acaoNova && Object.prototype.hasOwnProperty.call(DefaultPermissions[moduloNovo], acaoNova)) {
        converted[moduloNovo][acaoNova] = !!valor;
      }
    }

    // Completa com padrão do módulo
    for (const [acaoNova, valorPadrao] of Object.entries(DefaultPermissions[moduloNovo])) {
      if (!(acaoNova in converted[moduloNovo])) {
        converted[moduloNovo][acaoNova] = valorPadrao;
      }
    }
  }

  // Garante todos módulos do default com suas ações
  for (const [modNovo, acoesPadrao] of Object.entries(DefaultPermissions)) {
    if (!converted[modNovo]) {
      converted[modNovo] = { ...acoesPadrao };
    } else {
      for (const [acaoNova, valorPadrao] of Object.entries(acoesPadrao)) {
        if (!(acaoNova in converted[modNovo])) {
          converted[modNovo][acaoNova] = valorPadrao;
        }
      }
    }
  }

  return converted;
}

// Nomes antigos → novos módulos
function mapModulo(modAntigo) {
  const mapa = {
    Dashboard: 'Dashboard',
    Atendimentos: 'Tickets',
    'Chat Interno': 'InternalChat',
    Usuários: 'Users',
    'Filas | Grupos': 'Departments',
    'Permissões do sistema': 'Permissions',
    Canais: 'Channels',
    Etiquetas: 'Tags',
    Telefonia: 'Providers',
    Contatos: 'Contacts',
    'Mensagens Rápidas': 'QuickMessages',
    Chatbot: 'Flows',
    Configurações: 'Settings',
    Campanha: 'Campaign',
    Tarefas: 'Task',
    'Tipos de Tarefa': 'TaskType',
    'Minhas Tarefas': 'MyTask',
    Automação: 'TaskAutomation',
    'Horário de Atendimento': 'AttendancePeriods',
    Feriados: 'Holidays',
    'Relatórios de Mensagem': 'ReportsMessage',
    'Relatórios de Atendimento': 'ReportsTicket',
    'Relatórios de Contatos': 'ReportsContact',

    // novos nomes prováveis no legado:
    'Motivos de Atendimento': 'TicketReasons',
    'Relatórios de Avaliação': 'ReportsRating',
    'Documentação da API': 'ApiDocs',
    'Linha do Tempo': 'Timeline',
    'Painel de Atendimentos': 'TicketsPanel',
    Kanban: 'Kanban',
    'Quadro Kanban': 'KanbanBoard',
    'Quadro': 'KanbanBoard'
  };
  return mapa[modAntigo] || null;
}

// Ações antigas → novas ações (com exceções por módulo)
function mapAcao(acaoAntiga, moduloNovo) {
  const base = {
    visualizar: 'access',
    acessar: 'access',
    editar: 'edit',
    criar: 'create',
    deletar: 'delete',
    download: 'download',
    configurar: 'edit',
    gerenciar: 'manage',
    alterar: 'edit',

    verDados: 'geral',
    contatos: 'access',

    enviarMensagem: 'sendMessage',
    verTodos: 'filterSeeAll',   // (Tickets)
    abrir: 'openTicket',
    abrirAtendimento: 'openTicket',

    timeline: 'access',         // às vezes vinha "timeline": true
    espiar: 'viewTicketContact' // compat
  };

  if (moduloNovo === 'Timeline') {
    if (acaoAntiga === 'verTodosTimeline' || acaoAntiga === 'verTodos') return 'seeAll';
    if (['timeline', 'visualizar', 'acessar'].includes(acaoAntiga)) return 'access';
  }

  if (moduloNovo === 'Tickets') {
    if (acaoAntiga === 'verTodosTimeline') return 'filterSeeAll';
  }

  if (moduloNovo === 'ApiDocs') {
    if (['docs', 'documentacao', 'documentação'].includes(acaoAntiga)) return 'access';
  }

  if (moduloNovo === 'TicketsPanel') {
    if (['painel', 'verPainel'].includes(acaoAntiga)) return 'access';
  }

  if (moduloNovo === 'Kanban' || moduloNovo === 'KanbanBoard') {
    if (['quadro', 'verQuadro', 'kanban'].includes(acaoAntiga)) return 'access';
  }

  return base[acaoAntiga] || null;
}
