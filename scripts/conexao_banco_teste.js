const { Client } = require('pg');

(async () => {
  const client = new Client({
    host: '170.81.42.168',
    port: 5432,
    user: 'postgres',
    password: '@Th0st3c',
    database: 'omniathostec',
    ssl: false // troque para { rejectUnauthorized: false } se precisar SSL
  });

  try {
    await client.connect();
    const { rows } = await client.query('select version(), current_user, current_database()');
    console.log(rows[0]);
  } catch (e) {
    console.error('Falha na conexão/query:', e.message);
  } finally {
    await client.end();
  }
})();
