import axios from 'axios';
import FormData from 'form-data';
import path from 'path';
import dotenv from 'dotenv';
import fs from 'fs';
import tmp from 'tmp';
import ffmpeg from 'fluent-ffmpeg';
import ffmpegPath from 'ffmpeg-static';

dotenv.config();
ffmpeg.setFfmpegPath(ffmpegPath);

const VALID_WHATSAPP_MEDIA_TYPES = ['image', 'audio', 'document'];

export async function uploadMediaToWhatsapp(fileUrl, type = 'image') {
  if (!VALID_WHATSAPP_MEDIA_TYPES.includes(type)) {
    throw new Error(`[uploadMediaToWhatsapp] Tipo de mídia inválido: "${type}". Use: ${VALID_WHATSAPP_MEDIA_TYPES.join(', ')}.`);
  }

  console.log('📥 Baixando arquivo da URL:', fileUrl);

  const response = await axios.get(fileUrl, { responseType: 'stream' });
  const ext = path.extname(fileUrl);
  const originalFile = tmp.fileSync({ postfix: ext });
  const writeStream = fs.createWriteStream(originalFile.name);

  await new Promise((resolve, reject) => {
    response.data.pipe(writeStream);
    writeStream.on('finish', resolve);
    writeStream.on('error', reject);
  });

  let finalFilePath = originalFile.name;
  let finalMime = response.headers['content-type'];

  try {
    if (type === 'audio') {
      console.log('🎙️ Convertendo áudio para OGG (opus, mono)...');
      const oggFile = tmp.fileSync({ postfix: '.ogg' });

      await new Promise((resolve, reject) => {
        ffmpeg(originalFile.name)
          .audioCodec('libopus')      // Codec opus
          .audioChannels(1)           // Canal único (mono)
          .format('ogg')              // Formato OGG
          .on('end', resolve)
          .on('error', (err) => {
            console.error('❌ Erro na conversão FFmpeg:', err.message);
            reject(err);
          })
          .save(oggFile.name);
      });

      finalFilePath = oggFile.name;
      finalMime = 'audio/ogg';

      originalFile.removeCallback();
    }

    const form = new FormData();
    form.append('messaging_product', 'whatsapp');
    form.append('type', type);
    form.append('file', fs.createReadStream(finalFilePath), {
      filename: path.basename(finalFilePath),
      contentType: finalMime
    });

    console.log('📤 Enviando para WhatsApp API...');

    const res = await axios.post(
      `https://graph.facebook.com/v17.0/${process.env.PHONE_NUMBER_ID}/media`,
      form,
      {
        headers: {
          ...form.getHeaders(),
          Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`
        }
      }
    );

    console.log('[✅ uploadMediaToWhatsapp] Upload bem-sucedido:', res.data);
    return res.data.id;
  } catch (err) {
    console.error('[❌ uploadMediaToWhatsapp] Erro:', err.response?.data || err.message);
    throw new Error('Erro ao subir mídia para o WhatsApp');
  } finally {
    if (fs.existsSync(originalFile.name)) originalFile.removeCallback();
  }
}
