import axios from 'axios';
import dotenv from 'dotenv';
dotenv.config();

const {
  API_VERSION,
  PHONE_NUMBER_ID,
  WHATSAPP_TOKEN: ACCESS_TOKEN
} = process.env;

/**
 * Envia indicador de digitação ("typing...") no WhatsApp Cloud API.
 */
export async function sendTypingIndicator(to) {
  if (!to) return;

  try {
    await axios.post(
  `https://graph.facebook.com/${API_VERSION}/${PHONE_NUMBER_ID}/messages`,
  {
    "messaging_product": "whatsapp",
    "status": "read",
    "message_id": to,
    "typing_indicator": {
      "type": "text"
    }
  },
  {
    headers: {
      Authorization: `Bearer ${ACCESS_TOKEN}`,
      'Content-Type': 'application/json'
    }
  }
);

    console.log('[✅ Enviado typing indicator]');
  } catch (err) {
    console.warn('[⚠️ Erro ao enviar typing]', err.response?.data || err.message);
  }
}
