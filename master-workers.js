// master-workers.js
import 'dotenv/config'; // se quiser, mas db.js já faz dotenv.config()
import { initDB } from './engine/services/db.js';

import { startCampaignScheduler } from './worker-campaign-scheduler.js';
import { startIncomingWorker }   from './worker-incoming.js';
import { startOutgoingWorker }   from './worker-outgoing.js';
import { startStatusWorker }     from './worker-status.js';

async function main() {
  console.log('[master] bootando workers...');

  await initDB(); // garante pool criado (idempotente)

  const ENABLE_SCHEDULER = process.env.ENABLE_SCHEDULER === '1';
  const ENABLE_INCOMING  = process.env.ENABLE_INCOMING  !== '0'; // default ON
  const ENABLE_OUTGOING  = process.env.ENABLE_OUTGOING  !== '0'; // default ON
  const ENABLE_STATUS    = process.env.ENABLE_STATUS    !== '0'; // default ON

  const shutdownFns = [];

  if (ENABLE_SCHEDULER) {
    const stopScheduler = await startCampaignScheduler().catch((e) => {
      console.error('[master] scheduler fail:', e?.message || e);
      return null;
    });
    if (stopScheduler) shutdownFns.push(stopScheduler);
  }

  if (ENABLE_INCOMING) {
    const stopIncoming = await startIncomingWorker().catch((e) => {
      console.error('[master] incoming fail:', e?.message || e);
      return null;
    });
    if (stopIncoming) shutdownFns.push(stopIncoming);
  }

  if (ENABLE_OUTGOING) {
    const stopOutgoing = await startOutgoingWorker().catch((e) => {
      console.error('[master] outgoing fail:', e?.message || e);
      return null;
    });
    if (stopOutgoing) shutdownFns.push(stopOutgoing);
  }

  if (ENABLE_STATUS) {
    const stopStatus = await startStatusWorker().catch((e) => {
      console.error('[master] status fail:', e?.message || e);
      return null;
    });
    if (stopStatus) shutdownFns.push(stopStatus);
  }

  if (!shutdownFns.length) {
    console.error('[master] nenhuma feature habilitada (veja ENABLE_*)');
    process.exit(1);
  }

  console.log('[master] todos os workers iniciados.');

  async function gracefulExit(signal) {
    console.log(`[master] recebendo ${signal}, encerrando...`);
    for (const fn of shutdownFns) {
      try { await fn(signal); } catch (e) {
        console.error('[master] erro ao desligar worker:', e?.message || e);
      }
    }
    process.exit(0);
  }

  process.on('SIGINT',  () => gracefulExit('SIGINT'));
  process.on('SIGTERM', () => gracefulExit('SIGTERM'));
}

main().catch((e) => {
  console.error('[master] erro fatal:', e?.message || e);
  process.exit(1);
});
