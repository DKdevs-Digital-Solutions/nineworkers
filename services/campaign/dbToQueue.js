// worker-campaign-scheduler.js
import 'dotenv/config';
import { initDB, dbPool } from '../../engine/services/db.js';
import { processCampaignDirect } from './services/campaign/processDirect.js';

const TICK_MS = parseInt(process.env.CAMPAIGN_SCHED_DB_TICK_MS || '5000', 10);
const TENANT  = (process.env.TENANT || 'default').toLowerCase();
const SCHEMA  = (process.env.PG_SCHEMA || 'public').trim();

function pgIdent(id) {
  return /^[a-z0-9_]+$/i.test(id) ? id : `"${String(id).replace(/"/g, '""')}"`;
}

async function tick() {
  const client = await dbPool.connect();
  try {
    await client.query(`SET search_path TO ${pgIdent(SCHEMA)}, public;`);

    const { rows } = await client.query(
      `SELECT id, name, start_at, status
         FROM ${pgIdent(SCHEMA)}.campaigns
        WHERE (status = 'queued')  -- imediatas
           OR (status = 'scheduled' AND start_at <= NOW()) -- agendadas no horário
        ORDER BY
          CASE WHEN status = 'queued' THEN 0 ELSE 1 END,
          start_at NULLS FIRST
        LIMIT 10`
    );

    if (!rows.length) {
      console.log(`[scheduler] nenhuma campanha pronta (tenant=${TENANT})`);
      return;
    }

    for (const row of rows) {
      try {
        console.log(`[scheduler] processando campanha ${row.id} (${row.name}) — status=${row.status} start_at=${row.start_at}`);

        const res = await processCampaignDirect(row.id, {
          tenant: TENANT,
          schema: SCHEMA,
          batchSize: parseInt(process.env.CAMPAIGN_BATCH_SIZE || '200', 10),
          maxAttempts: parseInt(process.env.CAMPAIGN_MAX_ATTEMPTS || '5', 10),
          sendDelayMs: parseInt(process.env.CAMPAIGN_SEND_DELAY_MS || '0', 10) // throttle opcional
        });

        console.log(`[scheduler] campanha ${row.id}: processed=${res.processed} sent=${res.sent} retry=${res.retry} failed=${res.failed}`);

        // Se zerou pendências, marca como finished
        if (res.remaining === 0) {
          await client.query(
            `UPDATE ${pgIdent(SCHEMA)}.campaigns
                SET status='finished', updated_at=NOW()
              WHERE id=$1`,
            [row.id]
          );
          console.log(`[scheduler] campanha ${row.id} marcada como finished`);
        } else {
          // Se ainda restam itens, mantém 'queued' (ou 'scheduled' se tiver start_at futuro — não é o caso)
          console.log(`[scheduler] campanha ${row.id} ainda tem ${res.remaining} itens pendentes (status não alterado)`);
        }
      } catch (e) {
        console.error(`[scheduler] erro ao processar campanha ${row.id}:`, e?.message || e);
        await client.query(
          `UPDATE ${pgIdent(SCHEMA)}.campaigns
              SET status='failed', updated_at=NOW()
            WHERE id=$1`,
          [row.id]
        );
      }
    }
  } catch (e) {
    console.error('[scheduler] falha no tick:', e?.message || e);
  } finally {
    client.release();
  }
}

async function start() {
  await initDB();
  console.log(`[scheduler] iniciado. tenant=${TENANT}, schema=${SCHEMA}, tick=${TICK_MS}ms`);
  await tick().catch(e => console.error('[scheduler] primeiro tick erro:', e?.message || e));
  setInterval(() => tick().catch(e => console.error('[scheduler] tick erro:', e?.message || e)), TICK_MS);
}

start().catch((e) => {
  console.error('[scheduler] erro fatal de bootstrap:', e?.message || e);
  process.exit(1);
});
