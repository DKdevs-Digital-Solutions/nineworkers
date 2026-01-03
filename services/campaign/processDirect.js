// services/campaign/processDirect.js
import { v4 as uuidv4 } from 'uuid';
import { initDB, dbPool } from '../../engine/services/db.js';
import { dispatchOutgoing } from '../outgoing/dispatcher.js';

const pgIdent = (id) => (/^[a-z0-9_]+$/i.test(id) ? id : `"${String(id).replace(/"/g, '""')}"`);

function formatUserId(to, channel = 'whatsapp') {
  const msisdn = String(to || '').replace(/\D/g, '').replace(/^0+/, '');
  return channel === 'telegram' ? `${msisdn}@t.msgcli.net` : `${msisdn}@w.msgcli.net`;
}

function hydrateComponents(components, vars) {
  if (!components) return undefined;
  try {
    const s = JSON.stringify(components);
    const out = s.replace(/\{(\w+)\}/g, (_, k) => (vars?.[k] ?? ''));
    return JSON.parse(out);
  } catch {
    return components;
  }
}

async function upsertPendingActive(client, schema, { userId, tempId, origin, onReply }) {
  if (!onReply || !onReply.action) return;
  const action  = String(onReply.action).toLowerCase();
  const payload = onReply.payload ? JSON.stringify(onReply.payload) : JSON.stringify({});
  const ttlDays = Number(onReply.ttl_days || 30);

  await client.query(
    `
    INSERT INTO ${pgIdent(schema)}.sessions (user_id, current_block, last_flow_id, vars, updated_at)
    VALUES ($1, NULL, NULL,
      jsonb_build_object(
        'pending_active', jsonb_build_object(
          'message_id', $2,
          'origin',     $3,
          'action',     $4,
          'payload',    $5::jsonb,
          'expires_at', NOW() + ($6 || ' days')::interval
        )
      ),
      NOW()
    )
    ON CONFLICT (user_id) DO UPDATE
    SET vars = jsonb_set(
          COALESCE(${pgIdent(schema)}.sessions.vars, '{}'::jsonb),
          '{pending_active}',
          jsonb_build_object(
            'message_id', $2,
            'origin',     $3,
            'action',     $4,
            'payload',    $5::jsonb,
            'expires_at', NOW() + ($6 || ' days')::interval
          )
        ),
        updated_at = NOW()
    `,
    [userId, tempId, origin, action, payload, ttlDays]
  );
}

export async function processCampaignDirect(campaignId, opts = {}) {
  await initDB();

  const schema      = (opts.schema || process.env.PG_SCHEMA || 'public').trim();
  const batchSize   = Number(opts.batchSize || 200);
  const sendDelayMs = Number(opts.sendDelayMs || 0);

  const client = await dbPool.connect();
  try {
    await client.query(`SET search_path TO ${pgIdent(schema)}, public;`);

    const { rows: camps } = await client.query(
      `SELECT id, name, template_name, language_code, components, on_reply
         FROM ${pgIdent(schema)}.campaigns
        WHERE id = $1
        LIMIT 1`,
      [campaignId]
    );
    if (!camps.length) throw new Error('Campanha não encontrada');
    const camp = camps[0];

    const { rows: items } = await client.query(
      `SELECT id, to_msisdn, variables
         FROM ${pgIdent(schema)}.campaign_items
        WHERE campaign_id = $1
          AND (message_id IS NULL OR message_id = '')
        ORDER BY created_at ASC
        LIMIT $2`,
      [campaignId, batchSize]
    );

    if (!items.length) {
      const { rows: r } = await client.query(
        `SELECT COUNT(*)::int AS c
           FROM ${pgIdent(schema)}.campaign_items
          WHERE campaign_id=$1 AND (message_id IS NULL OR message_id='')`,
        [campaignId]
      );
      return { processed: 0, sent: 0, retry: 0, failed: 0, remaining: r[0].c };
    }

    let processed = 0, sent = 0, retry = 0, failed = 0;

    for (const it of items) {
      processed += 1;

      const to = String(it.to_msisdn || '').replace(/\D/g, '');
      if (!to) { failed += 1; continue; }

      const hydratedComponents = hydrateComponents(camp.components, it.variables);
      const tempId = uuidv4();
      const userId = formatUserId(to, 'whatsapp');

      try {
        await client.query(
          `INSERT INTO ${pgIdent(schema)}.messages (
             user_id, message_id, direction, "type", "content",
             "timestamp", flow_id, reply_to, status, metadata,
             created_at, updated_at, channel
           ) VALUES ($1,$2,'outgoing','template',$3,
                    NOW(), NULL, NULL, 'pending',
                    jsonb_build_object(
                      'languageCode', $4::text,
                      'components',   $5::jsonb,
                      'campaign_id',  $6::text,
                      'campaign_item_id', $7::text
                    ),
                    NOW(), NOW(), 'whatsapp')`,
          [
            userId,
            tempId,
            camp.template_name,
            camp.language_code,
            hydratedComponents ? JSON.stringify(hydratedComponents) : null,
            String(camp.id),
            String(it.id)
          ]
        );

        if (camp.on_reply) {
          try {
            await upsertPendingActive(client, schema, {
              userId,
              tempId,
              origin: 'campaign',
              onReply: camp.on_reply
            });
          } catch (e) {
            console.warn('[campaign] upsert pending_active falhou:', e?.message || e);
          }
        }
      } catch (e) {
        console.error('[campaign] falha ao inserir em messages:', e?.message || e);
        failed += 1;
        continue;
      }

      const job = {
        channel: 'whatsapp',
        to,
        type: 'template',
        template: {
          name: camp.template_name,
          language: { code: camp.language_code },
          ...(hydratedComponents ? { components: hydratedComponents } : {})
        },
        tempId
      };

      try {
        const res = await dispatchOutgoing(job);
        if (res?.ok) {
          sent += 1;
          const wamid = res.providerId || null;

          if (wamid) {
            await client.query(
              `UPDATE ${pgIdent(schema)}.campaign_items
                  SET message_id = $1,
                      delivery_status = COALESCE(delivery_status, 'sent'),
                      last_status = 'sent',
                      last_status_at = NOW(),
                      updated_at = NOW()
                WHERE id = $2`,
              [wamid, it.id]
            );
          }
        } else {
          retry += 1;
          await client.query(
            `UPDATE ${pgIdent(schema)}.campaign_items
                SET last_status = 'failed',
                    last_status_at = NOW(),
                    updated_at = NOW()
              WHERE id = $1`,
            [it.id]
          );
        }
      } catch (e) {
        failed += 1;
        await client.query(
          `UPDATE ${pgIdent(schema)}.campaign_items
              SET last_status = 'failed',
                  last_status_at = NOW(),
                  updated_at = NOW()
            WHERE id = $1`,
          [it.id]
        );
      }

      if (sendDelayMs > 0) {
        await new Promise(r => setTimeout(r, sendDelayMs));
      }
    }

    const { rows: r2 } = await client.query(
      `SELECT COUNT(*)::int AS c
         FROM ${pgIdent(schema)}.campaign_items
        WHERE campaign_id=$1 AND (message_id IS NULL OR message_id='')`,
      [campaignId]
    );

    return { processed, sent, retry, failed, remaining: r2[0].c };
  } finally {
    client.release();
  }
}
