// worker-status.js
import amqplib from 'amqplib';
import { initDB, dbPool } from './engine/services/db.js';
import { emitConversationEvent } from './services/realtime/emitToRoom.js';

const AMQP_URL = process.env.AMQP_URL || 'amqp://guest:guest@rabbit:5672/';
const SCHEMA   = (process.env.PG_SCHEMA || 'public').trim();
const QUEUE    = process.env.STATUS_QUEUE || `${SCHEMA}.status`;
const PREFETCH = parseInt(process.env.STATUS_PREFETCH || '100', 10);

const LOG_PAYLOAD = String(process.env.STATUS_LOG_PAYLOAD || '0') === '1';
const LOG_DEBUG   = String(process.env.STATUS_LOG_DEBUG   || '0') === '1';

const ident = (s) =>
  (/^[a-z0-9_]+$/i.test(s) ? s : `"${String(s).replace(/"/g, '""')}"`);

const MATCH_PREDICATE = `
  (
    message_id = $1
    OR message_id ILIKE '%' || $1 || '%'
    OR (
      message_id ~ '^[{[].*[}\\]]$'
      AND (
        (message_id::jsonb ? 'wamid' AND (message_id::jsonb ->> 'wamid') = $1)
        OR
        (message_id::jsonb ? 'id'    AND (message_id::jsonb ->> 'id')    = $1)
      )
    )
  )
`;

const mapStatus = (s = '') => {
  const v = String(s).toLowerCase();
  return ['sent', 'delivered', 'read', 'failed'].includes(v) ? v : 'unknown';
};

async function updateServiceMessageByWamid(client, schema, wamid, status, eventEpochSec) {
  const sql = `
    UPDATE ${ident(schema)}.messages
       SET status          = CASE
                               WHEN $2 IN ('sent','delivered','read','failed') THEN $2
                               ELSE status
                             END,
           last_status     = $2,
           last_status_at  = to_timestamp($3),
           delivered_at    = CASE
                               WHEN $2 = 'delivered'
                                AND (delivered_at IS NULL OR to_timestamp($3) > delivered_at)
                               THEN to_timestamp($3)
                               ELSE delivered_at
                             END,
           read_at         = CASE
                               WHEN $2 = 'read'
                                AND (read_at IS NULL OR to_timestamp($3) > read_at)
                               THEN to_timestamp($3)
                               ELSE read_at
                             END,
           updated_at      = NOW()
     WHERE ${MATCH_PREDICATE}
  `;
  const q = await client.query(sql, [wamid, status, eventEpochSec]);
  return q.rowCount || 0;
}

async function updateCampaignItemByWamid(client, schema, wamid, status, eventEpochSec) {
  const sql = `
    UPDATE ${ident(schema)}.campaign_items
       SET delivery_status = CASE
                               WHEN $2 IN ('sent','delivered','read','failed') THEN $2
                               ELSE delivery_status
                             END,
           last_status     = $2,
           last_status_at  = to_timestamp($3),
           delivered_at    = CASE
                               WHEN $2 = 'delivered'
                                AND (delivered_at IS NULL OR to_timestamp($3) > delivered_at)
                               THEN to_timestamp($3)
                               ELSE delivered_at
                             END,
           read_at         = CASE
                               WHEN $2 = 'read'
                                AND (read_at IS NULL OR to_timestamp($3) > read_at)
                               THEN to_timestamp($3)
                               ELSE read_at
                             END,
           updated_at      = NOW()
     WHERE ${MATCH_PREDICATE}
  `;
  const q = await client.query(sql, [wamid, status, eventEpochSec]);
  return q.rowCount || 0;
}

async function getServiceMessageLite(client, schema, wamid) {
  const sql = `
    SELECT id, message_id, user_id, channel, direction, type, content, timestamp, status
      FROM ${ident(schema)}.messages
     WHERE ${MATCH_PREDICATE}
     ORDER BY id DESC
     LIMIT 1
  `;
  const q = await client.query(sql, [wamid]);
  return q.rows[0] || null;
}

export async function startStatusWorker() {
  console.log(
    `[status-worker] boot → queue=${QUEUE} prefetch=${PREFETCH} PG_SCHEMA=${SCHEMA}`
  );
  await initDB();

  try {
    const c = await dbPool.connect();
    try {
      await c.query(`SET search_path TO ${ident(SCHEMA)}, public;`);
      const ctx = await c.query(
        `select current_user as u, current_schema() as s, current_setting('search_path', true) as p`
      );
      console.log(
        `[status-worker] db ctx user=${ctx.rows[0].u} schema=${ctx.rows[0].s} search_path=${ctx.rows[0].p}`
      );
    } finally {
      c.release();
    }
  } catch (e) {
    console.warn('[status-worker] aviso ctx DB:', e?.message);
  }

  const conn = await amqplib.connect(AMQP_URL, { heartbeat: 15 });
  const ch = await conn.createChannel();
  await ch.assertQueue(QUEUE, { durable: true });
  ch.prefetch(PREFETCH);
  console.log(`[status-worker] consumindo ${QUEUE} ...`);

  ch.consume(
    QUEUE,
    async (msg) => {
      if (!msg) return;

      let ev;
      try {
        ev = JSON.parse(msg.content.toString('utf8'));
      } catch (e) {
        console.error('[status-worker] JSON inválido — descartando:', e?.message || e);
        ch.nack(msg, false, false);
        return;
      }

      if (ev?.kind !== 'waba_status' || !ev?.status?.id) {
        if (LOG_DEBUG)
          console.log(
            '[status-worker] ignorando: kind/id ausentes',
            ev?.kind
          );
        ch.ack(msg);
        return;
      }

      const wamid  = ev.status.id;
      const status = mapStatus(ev.status.status);
      const tsSec  = Number(ev.status.timestamp || 0);

      if (LOG_PAYLOAD) {
        try {
          console.log(
            '[status-worker] payload:',
            JSON.stringify(ev, null, 2)
          );
        } catch {}
      }
      console.log(`[status-worker] ⇢ wamid=${wamid} → ${status} @ ${tsSec}`);

      const client = await dbPool.connect();
      try {
        await client.query(`SET search_path TO ${ident(SCHEMA)}, public;`);

        const updatedService = await updateServiceMessageByWamid(
          client,
          SCHEMA,
          wamid,
          status,
          tsSec
        );
        let updatedCampaign = 0;
        try {
          updatedCampaign = await updateCampaignItemByWamid(
            client,
            SCHEMA,
            wamid,
            status,
            tsSec
          );
        } catch (e) {
          if (LOG_DEBUG)
            console.warn(
              '[status-worker] campaign_items ausente/erro:',
              e?.message
            );
        }

        const row = await getServiceMessageLite(client, SCHEMA, wamid);

        ch.ack(msg);
        console.log(
          `[status-worker] ✔ service(messages)~${updatedService} campaign_items~${updatedCampaign}`
        );

        if (row?.user_id) {
          const payload = {
            id: row.id,
            message_id: row.message_id,
            user_id: row.user_id,
            channel: row.channel || 'whatsapp',
            direction: row.direction || 'outgoing',
            type: (row.type || 'text').toLowerCase(),
            status,
            timestamp: tsSec
              ? new Date(tsSec * 1000).toISOString()
              : row.timestamp,
            content: row.content ?? undefined
          };
          await emitConversationEvent(
            SCHEMA,
            row.user_id,
            'update_message',
            payload
          );
        } else {
          console.log(
            '[status-worker] ⚠️ não achou message.user_id pra emitir'
          );
        }

        if (updatedService === 0 && updatedCampaign === 0) {
          console.log(
            '[status-worker] ⚠️ nada correlacionado. Verifique como o message_id está sendo salvo (substring/JSON).'
          );
        }
      } catch (e) {
        console.error(
          '[status-worker] falha ao gravar/emitir (nack/requeue):',
          e?.message || e
        );
        ch.nack(msg, false, true);
      } finally {
        client.release();
      }
    },
    { noAck: false }
  );

  return async (reason = 'shutdown') => {
    console.log(`[status-worker] shutdown: ${reason}`);
    try { await ch.close(); } catch {}
    try { await conn.close(); } catch {}
  };
}

startStatusWorker().catch((e) => {
  console.error('[status-worker] erro fatal:', e?.message || e);
  process.exit(1);
});
