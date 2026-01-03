// engine/services/db.js  — UNIVERSAL
import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) throw new Error('DATABASE_URL não definido');

// Mono-tenant por processo (opcional). Ex.: PG_SCHEMA=hmg
const PG_SCHEMA = (process.env.PG_SCHEMA || 'public').trim();

function pgIdent(id) {
  return /^[a-z0-9_]+$/.test(id) ? id : `"${String(id).replace(/"/g, '""')}"`;
}

export let dbPool;

export async function initDB() {
  if (dbPool) return dbPool;

  dbPool = new Pool({
    connectionString: DATABASE_URL,
    ssl: DATABASE_URL.includes('supabase') ? { rejectUnauthorized: false } : false,
    max: Number(process.env.PG_MAX || 20),
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 5_000,
  });

  // Cada conexão nasce com search_path default do processo (mono-tenant opcional)
  dbPool.on('connect', (client) => {
    client.query(`SET search_path TO ${pgIdent(PG_SCHEMA)}, public`).catch((e) => {
      console.error('[db] falha ao SET search_path:', e?.message);
    });
  });

  // Sanidade + reforça na 1ª vez
  const c = await dbPool.connect();
  try {
    await c.query('SELECT 1');
    await c.query(`SET search_path TO ${pgIdent(PG_SCHEMA)}, public`);
  } finally {
    c.release();
  }

  console.log(`[db] conectado. search_path=${PG_SCHEMA},public`);
  return dbPool;
}

// API básica
export const query = (text, params) => dbPool.query(text, params);

export async function tx(fn) {
  const client = await dbPool.connect();
  try {
    await client.query('BEGIN');
    // dentro da tx, reforça o search_path default
    await client.query(`SET LOCAL search_path TO ${pgIdent(PG_SCHEMA)}, public`);
    const out = await fn(client);
    await client.query('COMMIT');
    return out;
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    throw e;
  } finally {
    client.release();
  }
}

// --------- Helpers multi-tenant (copiados/adaptados do services/db.js) ---------

// resolve schema por subdomínio (usa tabela public.tenants)
export async function lookupSchemaBySubdomain(subdomain) {
  if (!subdomain) return null;
  const q = 'SELECT schema_name FROM public.tenants WHERE subdomain = $1';
  const { rows } = await dbPool.query(q, [subdomain]);
  return rows[0]?.schema_name || null;
}

// executa callback dentro de uma tx com search_path=<schema>,public
export async function withTenant(schema, fn) {
  if (!schema) throw new Error('schema do tenant ausente');
  const client = await dbPool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL search_path TO ${pgIdent(schema)}, public`);
    const out = await fn(client);
    await client.query('COMMIT');
    return out;
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    throw e;
  } finally {
    client.release();
  }
}
