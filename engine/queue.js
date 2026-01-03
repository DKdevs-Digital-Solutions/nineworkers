// engine/queue.js
import amqplib from 'amqplib';

const AMQP_URL      = process.env.AMQP_URL      || 'amqp://guest:guest@rabbitmq:5672/';
const OUTGOING_Q    = process.env.OUTGOING_QUEUE|| 'hmg.outgoing';
const FLOW_Q        = process.env.FLOW_QUEUE    || 'hmg.flow';

let ch;
async function getCh() {
  if (ch) return ch;
  const conn = await amqplib.connect(AMQP_URL, { heartbeat: 15 });
  ch = await conn.createChannel();
  await ch.assertQueue(OUTGOING_Q, { durable: true });
  await ch.assertQueue(FLOW_Q,     { durable: true });
  return ch;
}

export async function enqueueOutgoing(msg) {
  const c = await getCh();
  c.sendToQueue(OUTGOING_Q, Buffer.from(JSON.stringify(msg)), {
    persistent: true,
    contentType: 'application/json',
    headers: { 'x-attempts': 0 },
  });
}

export async function enqueueFlow(step, { delayMs = 0 } = {}) {
  const c = await getCh();
  const opts = { persistent: true, contentType: 'application/json' };
  if (delayMs > 0) opts.headers = { 'x-delay': delayMs }; // se usar delayed-exchange
  c.sendToQueue(FLOW_Q, Buffer.from(JSON.stringify(step)), opts);
}
