import axios from 'axios';
import dotenv from 'dotenv';
dotenv.config();

const {
  API_VERSION,
  PHONE_NUMBER_ID,
  WHATSAPP_TOKEN: ACCESS_TOKEN
} = process.env;

/**
 * Marca a mensagem como lida no WhatsApp Cloud API.
 */
export async function markMessageAsRead(messageId) {
  if (!messageId) return;

  try {
    await axios.post(
      `https://graph.facebook.com/${API_VERSION}/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: 'whatsapp',
        status: 'read',
        message_id: messageId
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          'Content-Type': 'application/json'
        }
      }
    );
    console.log('[✅ Mensagem marcada como lida]');
  } catch (err) {
    console.warn('[⚠️ Erro ao marcar como lida]', err.response?.data || err.message);
  }
}
