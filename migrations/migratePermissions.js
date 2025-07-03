require('dotenv').config();
const { Client } = require('pg');

const DefaultPermissions = {
    Dashboard: { access: true, geral: true, atendente: true },
    Tickets: {
        access: true,
        sendMessage: true,
        openTicket: true,
        filterSeeAll: true,
        manage: true,
        viewTicketContact: true,
    },
    InternalChat: { access: true },
    Users: { access: true, edit: true, create: true },
    Departments: { access: true, edit: true, create: true },
    Permissions: { access: true, edit: true, create: true },
    Channels: { access: true, edit: true, create: true },
    Tags: { access: true, edit: true, create: true, delete: true },
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
    ReportsContact: { access: true },
};

module.exports = async function migratePermissions() {
    console.log('🔐 Migrando "permissions"...');

    const source = new Client({
        host: process.env.SRC_HOST,
        port: process.env.SRC_PORT,
        user: process.env.SRC_USER,
        password: process.env.SRC_PASS,
        database: process.env.SRC_DB,
    });

    const dest = new Client({
        host: process.env.DST_HOST,
        port: process.env.DST_PORT,
        user: process.env.DST_USER,
        password: process.env.DST_PASS,
        database: process.env.DST_DB,
    });

    await source.connect();
    await dest.connect();

    const result = await source.query(`
    SELECT id, name, permissions, "tenantId" AS company_id, "createdAt", "updatedAt"
    FROM "public"."Permissions"
    WHERE id != 0
  `);

    for (const row of result.rows) {
        let converted = {};
        try {
            let oldPerms;
            if (typeof row.permissions === 'string') {
                oldPerms = JSON.parse(row.permissions);
            } else {
                oldPerms = row.permissions;
            }

            for (const [moduloAntigo, acoesAntigas] of Object.entries(oldPerms)) {
                const moduloNovo = mapModulo(moduloAntigo);
                if (!moduloNovo || !DefaultPermissions[moduloNovo]) {
                    continue;
                }

                converted[moduloNovo] = {};
                for (const [acaoAntiga, valor] of Object.entries(acoesAntigas)) {
                    const acaoNova = mapAcao(acaoAntiga);
                    if (acaoNova && DefaultPermissions[moduloNovo][acaoNova] !== undefined) {
                        converted[moduloNovo][acaoNova] = valor;
                    }
                }

                // Preenche ações faltantes com padrão
                for (const [acaoNova, valorPadrao] of Object.entries(DefaultPermissions[moduloNovo])) {
                    if (!(acaoNova in converted[moduloNovo])) {
                        converted[moduloNovo][acaoNova] = valorPadrao;
                    }
                }
            }

            const jsonFinal = JSON.stringify(converted);

            await dest.query(
                `INSERT INTO permissions (id, name, permissions, company_id, created_at, updated_at)
       VALUES ($1, $2, $3::jsonb, $4, $5, $6)`,
                [
                    row.id,
                    row.name,
                    jsonFinal,
                    row.company_id,
                    row.createdAt,
                    row.updatedAt,
                ]
            );
        } catch (err) {
            console.debug(`🧾 JSON da permissão com erro em '${row.name}':`, row.permissions);
        }
    }

    console.log(`✅ Migração de permissões concluída.`);

    await source.end();
    await dest.end();
};

// Mapeamento de nomes antigos para novos módulos
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
    };
    return mapa[modAntigo] || null;
}

// Mapeamento de ações antigas para novas
function mapAcao(acao) {
    const mapa = {
        visualizar: 'access',
        editar: 'edit',
        criar: 'create',
        deletar: 'delete',
        download: 'download',
        configurar: 'edit',
        verTodos: 'filterSeeAll',
        enviarMensagem: 'sendMessage',
        timeline: 'viewTicketContact',
        espiar: 'viewTicketContact',
        verTodosTimeline: 'viewTicketContact',
        verDados: 'geral',
        contatos: 'access',
        gerenciar: 'manage',
        alterar: 'edit',
    };
    return mapa[acao] || null;
}
