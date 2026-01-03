// worker.js
import "dotenv/config";
import amqplib from "amqplib";
import { initDB } from "./engine/services/db.js";
import { processEvent } from "./services/high/processEvent.js"; // ajuste lá pra usar emitToRoom
// import { getIO } from './services/realtime/socketClient.js';  // ❌ não usamos mais

const AMQP_URL  = process.env.AMQP_URL || "amqp://guest:guest@localhost:5672/";
const QUEUE     = process.env.INCOMING_QUEUE || "hmg.incoming";
const PREFETCH  = Number(process.env.PREFETCH || 50);
const MAX_RETRY = Number(process.env.MAX_RETRIES || 5);

let conn, ch, closing = false;

const now = () => new Date().toISOString();
const redact = (u) => String(u).replace(/(\/\/[^:]+:)([^@]+)(@)/, "$1***$3");
const getAttempts = (msg) => Number((msg.properties.headers || {})["x-attempts"] || 0);

function requeue(msg) {
  const h = { ...(msg.properties.headers || {}) };
  h["x-attempts"] = getAttempts(msg) + 1;
  ch.nack(msg, false, true);
  console.log(`🔁 retry #${h["x-attempts"]}`);
}

async function onMessage(msg) {
  if (!msg) return;
  let evt;
  try { evt = JSON.parse(msg.content.toString()); }
  catch (e) { console.error("❌ JSON inválido, descarta:", e?.message); ch.nack(msg, false, false); return; }

  const attempts = getAttempts(msg);
  console.log(`📦 evento (${now()}) attempts=${attempts} ->`, {
    channel: evt?.channel, tenant: evt?.tenant_id, agg: evt?.aggregate_id, ext: evt?.external_id
  });

  try {
    // ✅ processEvent agora DEVE emitir via emitToRoom internamente
    //    ou receber um "publish" que chama emitToRoom
    const status = await processEvent(evt);
    if (status === "duplicate") { console.log("♻️ duplicate — ACK"); ch.ack(msg); return; }
    ch.ack(msg);
    console.log("✅ processado");
  } catch (e) {
    console.error("💥 erro no processamento:", e?.message || e);
    if (attempts + 1 >= MAX_RETRY) { console.warn(`⛔️ estourou ${MAX_RETRY} — NACK drop`); ch.nack(msg, false, false); }
    else requeue(msg);
  }
}

async function start() {
  console.log(`🚀 Worker @ ${now()} | AMQP=${redact(AMQP_URL)} | QUEUE=${QUEUE} | PREFETCH=${PREFETCH}`);
  await initDB();
  console.log("🗄️ Postgres conectado");

  conn = await amqplib.connect(AMQP_URL, { heartbeat: 15 });
  conn.on("error", e => console.error("[amqp conn error]", e));
  conn.on("close", () => { console.warn("[amqp conn closed]"); if (!closing) process.exit(1); });

  ch = await conn.createChannel();
  ch.on("error", e => console.error("[amqp ch error]", e));
  ch.on("close",  () => console.warn("[amqp ch closed]"));
  await ch.assertQueue(QUEUE, { durable: true });
  await ch.prefetch(PREFETCH);
  await ch.consume(QUEUE, onMessage, { noAck: false });

  console.log(`👂 Consumindo ${QUEUE}`);
}

async function shutdown(reason) {
  console.log(`🛑 shutdown: ${reason} @ ${now()}`);
  closing = true;
  try { await ch?.close(); } catch {}
  try { await conn?.close(); } catch {}
  console.log("✅ encerrado");
  process.exit(0);
}

process.on("SIGINT",  () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("unhandledRejection", e => console.error("unhandledRejection", e));
process.on("uncaughtException",  e => console.error("uncaughtException", e));

start().catch(e => { console.error("❌ start fail:", e); process.exit(1); });
