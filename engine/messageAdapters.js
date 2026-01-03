// engine/messageAdapters.js
import { MESSAGE_TYPES, CHANNELS } from './messageTypes.js';

function toMapsText({ latitude, longitude, name, address }) {
  const url = `https://www.google.com/maps?q=${latitude},${longitude}`;
  const header = name ? `📍 ${name}\n` : '';
  const addr = address ? `${address}\n` : '';
  return `${header}${addr}${url}`;
}

function toQuickReplies(buttons = []) {
  // Messenger/Instagram: quick_replies: [{content_type:'text', title, payload}]
  return buttons.slice(0, 11).map(b => ({
    content_type: 'text',
    title: b?.title?.toString()?.slice(0, 20) || 'Opção',
    payload: b?.id?.toString() || b?.title?.toString() || 'opt'
  }));
}

function toButtonsTemplate(text, buttons = []) {
  // Messenger: até 3 botões
  const btns = buttons.slice(0, 3).map(b => ({
    type: 'postback',
    title: b?.title?.toString()?.slice(0, 20) || 'Opção',
    payload: b?.id?.toString() || b?.title?.toString() || 'opt'
  }));
  return {
    attachment: {
      type: 'template',
      payload: {
        template_type: 'button',
        text: text || 'Escolha uma opção',
        buttons: btns
      }
    }
  };
}

export class MessageAdapter {
  /* ============== WHATSAPP ============== */
  static toWhatsapp(unifiedMessage) {
    switch (unifiedMessage.type) {
      case MESSAGE_TYPES.TEXT:
        return { body: unifiedMessage.content.text };
      case MESSAGE_TYPES.IMAGE:
        return {
          link: unifiedMessage.content.url,
          caption: unifiedMessage.content.caption
        };
      case MESSAGE_TYPES.AUDIO:
        return {
          link: unifiedMessage.content.url,
          voice: unifiedMessage.content.isVoice || false
        };
      case MESSAGE_TYPES.VIDEO:
        return {
          link: unifiedMessage.content.url,
          caption: unifiedMessage.content.caption
        };
      case MESSAGE_TYPES.DOCUMENT:
        return {
          link: unifiedMessage.content.url,
          filename: unifiedMessage.content.filename
        };
      case MESSAGE_TYPES.LOCATION:
        return {
          latitude: unifiedMessage.content.latitude,
          longitude: unifiedMessage.content.longitude,
          name: unifiedMessage.content.name,
          address: unifiedMessage.content.address
        };
      case MESSAGE_TYPES.INTERACTIVE:
        // Pass-through no WhatsApp (já está no formato esperado)
        return unifiedMessage.content;
      default:
        throw new Error(`Tipo de mensagem não suportado: ${unifiedMessage.type}`);
    }
  }

  /* ============== TELEGRAM ============== */
  static toTelegram(unifiedMessage) {
    switch (unifiedMessage.type) {
      case MESSAGE_TYPES.TEXT:
        return { text: unifiedMessage.content.text };
      case MESSAGE_TYPES.IMAGE:
        return {
          photo: unifiedMessage.content.url,
          caption: unifiedMessage.content.caption
        };
      case MESSAGE_TYPES.AUDIO:
        return {
          audio: unifiedMessage.content.url,
          voice: unifiedMessage.content.isVoice || false
        };
      case MESSAGE_TYPES.VIDEO:
        return {
          video: unifiedMessage.content.url,
          caption: unifiedMessage.content.caption
        };
      case MESSAGE_TYPES.DOCUMENT:
        return {
          document: unifiedMessage.content.url,
          filename: unifiedMessage.content.filename
        };
      case MESSAGE_TYPES.LOCATION:
        return {
          latitude: unifiedMessage.content.latitude,
          longitude: unifiedMessage.content.longitude
        };
      case MESSAGE_TYPES.INTERACTIVE:
        return this._adaptInteractiveToTelegram(unifiedMessage.content);
      default:
        throw new Error(`Tipo de mensagem não suportado: ${unifiedMessage.type}`);
    }
  }

  static _adaptInteractiveToTelegram(content) {
    if (content.buttons) {
      return {
        reply_markup: {
          inline_keyboard: content.buttons.map(btn => [{ text: btn.title, callback_data: btn.id }])
        }
      };
    }
    if (content.list) {
      return {
        reply_markup: {
          keyboard: content.list.sections.map(section =>
            section.rows.map(row => ({ text: row.title }))
          ),
          one_time_keyboard: true
        }
      };
    }
    return content;
  }

  /* ============== FACEBOOK (Messenger) ============== */
  static toFacebook(unifiedMessage) {
    const c = unifiedMessage.content || {};
    switch (unifiedMessage.type) {
      case MESSAGE_TYPES.TEXT:
        return { text: c.text };

      case MESSAGE_TYPES.IMAGE:
        return {
          message: {
            attachment: { type: 'image', payload: { url: c.url, is_reusable: false } },
          },
          // manter caption em metadata para envio separado, se desejado
          _meta: c.caption ? { caption: c.caption } : undefined
        };

      case MESSAGE_TYPES.AUDIO:
        return {
          message: {
            attachment: { type: 'audio', payload: { url: c.url, is_reusable: false } },
          }
        };

      case MESSAGE_TYPES.VIDEO:
        return {
          message: {
            attachment: { type: 'video', payload: { url: c.url, is_reusable: false } },
          },
          _meta: c.caption ? { caption: c.caption } : undefined
        };

      case MESSAGE_TYPES.DOCUMENT:
        return {
          message: {
            attachment: { type: 'file', payload: { url: c.url, is_reusable: false } },
          }
        };

      case MESSAGE_TYPES.LOCATION: {
        // Messenger não envia "location" ativa; convertemos para texto com link.
        return { text: toMapsText(c) };
      }

      case MESSAGE_TYPES.INTERACTIVE: {
        // ===== Suporte aos formatos WhatsApp =====
        // 1) BUTTON (type: 'button' com action.buttons[].reply.{id,title})
        if (c?.type === 'button' && Array.isArray(c?.action?.buttons)) {
          const btns = c.action.buttons.map(b => ({
            id: b?.reply?.id || b?.reply?.title,
            title: b?.reply?.title || 'Opção'
          }));

          if (btns.length <= 3) {
            return { message: toButtonsTemplate(c.body?.text || c.text || c.caption, btns) };
          }
          return {
            message: {
              text: c.body?.text || c.text || 'Escolha uma opção',
              quick_replies: toQuickReplies(btns)
            }
          };
        }

        // 2) LIST (type: 'list' com action.sections[].rows[])
        if (c?.type === 'list' && Array.isArray(c?.action?.sections)) {
          const rows = c.action.sections.flatMap(s => s.rows || []);
          const mapped = rows.map(r => ({ id: r.id || r.title, title: r.title }));
          return {
            message: {
              text: c.body?.text || c.list?.title || 'Escolha uma opção',
              quick_replies: toQuickReplies(mapped)
            }
          };
        }

        // ===== Formatos já "normalizados" (genéricos) =====
        if (c?.buttons?.length) {
          if (c.buttons.length <= 3) {
            return { message: toButtonsTemplate(c.text || c.caption, c.buttons) };
          }
          return {
            message: {
              text: c.text || 'Escolha uma opção',
              quick_replies: toQuickReplies(c.buttons)
            }
          };
        }

        if (c?.list?.sections?.length) {
          const rows = c.list.sections.flatMap(s => s.rows || []);
          return {
            message: {
              text: c.list.title || 'Escolha uma opção',
              quick_replies: toQuickReplies(
                rows.map(r => ({ id: r.id || r.title, title: r.title }))
              )
            }
          };
        }

        // 4) Último recurso: devolve como está, para o sender decidir
        return c?.message ? { message: c.message } : c;
      }

      default:
        throw new Error(`Tipo de mensagem não suportado (facebook): ${unifiedMessage.type}`);
    }
  }

  /* ============== INSTAGRAM ============== */
  static toInstagram(unifiedMessage) {
    const c = unifiedMessage.content || {};
    switch (unifiedMessage.type) {
      case MESSAGE_TYPES.TEXT:
        return { text: c.text };

      case MESSAGE_TYPES.IMAGE:
        return {
          message: {
            attachment: { type: 'image', payload: { url: c.url, is_reusable: false } },
          },
          _meta: c.caption ? { caption: c.caption } : undefined
        };

      case MESSAGE_TYPES.AUDIO:
        return {
          message: {
            attachment: { type: 'audio', payload: { url: c.url, is_reusable: false } },
          }
        };

      case MESSAGE_TYPES.VIDEO:
        return {
          message: {
            attachment: { type: 'video', payload: { url: c.url, is_reusable: false } },
          },
          _meta: c.caption ? { caption: c.caption } : undefined
        };

      case MESSAGE_TYPES.DOCUMENT:
        // IG DM não aceita "file" genérico — fallback para link em texto
        return { text: c.filename ? `${c.filename}\n${c.url}` : c.url };

      case MESSAGE_TYPES.LOCATION:
        return { text: toMapsText(c) };

      case MESSAGE_TYPES.INTERACTIVE: {
        // ===== Suporte aos formatos WhatsApp =====
        // 1) BUTTON
        if (c?.type === 'button' && Array.isArray(c?.action?.buttons)) {
          const btns = c.action.buttons.map(b => ({
            id: b?.reply?.id || b?.reply?.title,
            title: b?.reply?.title || 'Opção'
          }));
          const qrs = toQuickReplies(btns);
          return { message: { text: c.body?.text || c.text || 'Escolha uma opção', quick_replies: qrs } };
        }

        // 2) LIST
        if (c?.type === 'list' && Array.isArray(c?.action?.sections)) {
          const rows = c.action.sections.flatMap(s => s.rows || []);
          const mapped = rows.map(r => ({ id: r.id || r.title, title: r.title }));
          const qrs = toQuickReplies(mapped);
          return { message: { text: c.body?.text || 'Escolha uma opção', quick_replies: qrs } };
        }

        // ===== Formatos já "normalizados" (genéricos) =====
        if (c?.buttons?.length) {
          return { message: { text: c.text || 'Escolha uma opção', quick_replies: toQuickReplies(c.buttons) } };
        }
        if (c?.list?.sections?.length) {
          const rows = c.list.sections.flatMap(s => s.rows || []);
          return {
            message: {
              text: c.list?.title || 'Escolha uma opção',
              quick_replies: toQuickReplies(rows.map(r => ({ id: r.id || r.title, title: r.title })))
            }
          };
        }

        // 4) Fallback: passa como está (sender consegue montar)
        return c?.message ? { message: c.message } : c;
      }

      default:
        throw new Error(`Tipo de mensagem não suportado (instagram): ${unifiedMessage.type}`);
    }
  }
}
