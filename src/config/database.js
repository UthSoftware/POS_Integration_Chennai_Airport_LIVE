const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  host: 'localhost',
  port: 5432,//process.env.DB_PORT,
  database: 'VIDVEDAA_INTEG',//process.env.DB_NAME,
  user: 'postgres',//process.env.DB_USER,
  password: 'masterkey',//process.env.DB_PASSWORD,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

pool.on('error', (err) => {
  console.error('Unexpected error on idle client', err);
  process.exit(-1);
});

module.exports = pool;