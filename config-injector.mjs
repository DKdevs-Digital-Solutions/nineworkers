import { Client } from 'pg';
import { pathToFileURL } from 'node:url';

async function main(){
  const [ , , workerKey, workerScript, subdomain, schemaName ] = process.argv;
  if (!workerKey || !workerScript || !subdomain || !schemaName){
    console.error("usage: node config-injector.mjs <workerKey> <scriptPath> <subdomain> <schema>");
    process.exit(2);
  }
  const DATABASE_URL = process.env.DATABASE_URL;
  const client = new Client({ connectionString: DATABASE_URL });
  await client.connect();

  try {
    // junta defaults + overrides do tenant
    const { rows } = await client.query(
      "SELECT public.get_worker_config($1::text,$2::text) AS cfg",
      [subdomain, workerKey]
    );
    const cfg = rows[0]?.cfg || {};
    for (const [k,v] of Object.entries(cfg)) {
      if (v === null || v === undefined) continue;
      process.env[k] = typeof v === "string" ? v : JSON.stringify(v);
    }
    // search_path do tenant
    process.env.PGOPTIONS = `-c search_path=${schemaName},public`;
    process.env.TENANT_ID = subdomain;
  } finally {
    try { await client.end(); } catch {}
  }

  const url = workerScript.startsWith("file://")
    ? workerScript
    : pathToFileURL(workerScript).href;

  await import(url);
}

main().catch((e)=>{ console.error("config-injector fatal:", e); process.exit(1); });
