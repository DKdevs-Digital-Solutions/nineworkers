// worker-outgoing.js — OUTGOING
import amqplib from 'amqplib';
import { dispatchOutgoing } from './services/outgoing/dispatcher.js';

const AMQP_URL  = process.env.AMQP_URL || 'amqp://guest:guest@rabbitmq:5672/';
const QUEUE     = process.env.OUTGOING_QUEUE || 'hmg.outgoing';
const PREFETCH  = Number(process.env.OUT_PREFETCH || process.env.PREFETCH || 50);

let conn, ch;

export async function startOutgoingWorker() {
  console.log(`[workerOut] boot → queue=${QUEUE} prefetch=${PREFETCH} AMQP_URL=${AMQP_URL}`);

  conn = await amqplib.connect(AMQP_URL, { heartbeat: 15 });
  ch   = await conn.createChannel();

  await ch.assertQueue(QUEUE, { durable: true });
  await ch.prefetch(PREFETCH);

  ch.consume(
    QUEUE,
    async (msg) => {
      if (!msg) return;

      const redelivered = !!msg.fields?.redelivered;
      const deliveryTag = msg.fields?.deliveryTag;

      let data;
      try {
        data = JSON.parse(msg.content.toString());
      } catch (e) {
        console.error('[workerOut] JSON inválido:', e?.message);
        console.error('[workerOut] DECISION = NACK(DROP) (json parse fail)');
        ch.nack(msg, false, false);
        return;
      }

      console.log('[workerOut] ➜', {
        channel: data.channel,
        to: data.to,
        type: data.type,
        redelivered,
        deliveryTag,
      });

      try {
        const res = await dispatchOutgoing(data);

        if (res?.ok) {
          console.log('[workerOut] DECISION = ACK');
          ch.ack(msg);
          return;
        }

        if (res?.retry) {
          console.error('[workerOut] DECISION = NACK(REQUEUE)', {
            reason: res?.reason || null,
          });
          ch.nack(msg, false, true); // requeue
          return;
        }

        console.error('[workerOut] DECISION = NACK(DROP)', {
          reason: res?.reason || null,
        });
        ch.nack(msg, false, false); // drop
      } catch (e) {
        console.error('[workerOut] erro inesperado no dispatcher:', e?.message || e);
        console.error('[workerOut] DECISION = NACK(REQUEUE) (catch-all)');
        ch.nack(msg, false, true);
      }
    },
    { noAck: false }
  );

  console.log(`[workerOut] consumindo ${QUEUE}`);

  // devolve shutdown pro master
  return async (reason = 'shutdown') => {
    console.log(`[workerOut] shutdown: ${reason}`);
    try { await ch?.close(); } catch {}
    try { await conn?.close(); } catch {}
  };
}

startOutgoingWorker().catch((e) => {
  console.error('[workerOut] erro fatal:', e?.message || e);
  process.exit(1);
});
