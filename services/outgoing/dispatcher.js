import { sendViaWhatsApp } from './senders/whatsappSender.js';
import { sendViaTelegram } from './senders/telegramSender.js';
import { sendViaFacebook } from './senders/facebookSender.js';
import { sendViaInstagram } from './senders/instagramSender.js';

export async function dispatchOutgoing(msg) {
  try {
    const ch = (msg?.channel || '').toLowerCase();
    const type = (msg?.type || '').toLowerCase();

    console.log('[dispatcher] IN  ->', { channel: ch, type });

    let out;
    switch (ch) {
      case 'whatsapp':
        out = await sendViaWhatsApp(msg);
        break;
      case 'telegram':
        out = await sendViaTelegram(msg);
        break;
      case 'facebook':
        out = await sendViaFacebook(msg);
        break;
      case 'instagram':
        out = await sendViaInstagram(msg);
        break;
      default:
        out = { ok: false, retry: false, reason: `Canal não suportado: ${msg?.channel}` };
    }

    console.log('[dispatcher] OUT <-', {
      ok: out?.ok === true,
      retry: out?.retry === true,
      reason: out?.reason || null,
    });

    return out;
  } catch (e) {
    console.error('[dispatcher] ERROR <-', e?.message || e);
    return { ok: false, retry: true, reason: e?.message || String(e) };
  }
}
