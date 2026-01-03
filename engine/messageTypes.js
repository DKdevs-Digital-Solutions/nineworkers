// engine/messageTypes.js

export const MESSAGE_TYPES = {
  TEXT: 'text',
  IMAGE: 'image',
  AUDIO: 'audio',
  VIDEO: 'video',
  DOCUMENT: 'document',
  LOCATION: 'location',
  INTERACTIVE: 'interactive',
  TEMPLATE: 'template'
};

export const CHANNELS = {
  WHATSAPP: 'whatsapp',
  TELEGRAM: 'telegram',
  FACEBOOK: 'facebook',
  INSTAGRAM: 'instagram',

};

export const SYSTEM_EVENT = 'system_event';
export const SYSTEM_EVT_TICKET_STATUS = 'ticket_status';

export const TICKET_STATUS = {
  OPEN: 'open',
  CLOSED: 'closed',
  TRANSFER: 'transfer',
};

export const UNIFIED_PAYLOAD_SCHEMA = {
  // Campos comuns a todos os canais
  common: {
    type: { required: true, type: 'string' },
    content: { required: true, type: ['string', 'object'] },
    metadata: { required: false, type: 'object' }
  },
  
  // Campos específicos por tipo de mensagem
  byType: {
    [MESSAGE_TYPES.TEXT]: {
      text: { required: true, type: 'string' }
    },
    [MESSAGE_TYPES.IMAGE]: {
      url: { required: true, type: 'string' },
      caption: { required: false, type: 'string' }
    },
    [MESSAGE_TYPES.AUDIO]: {
      url: { required: true, type: 'string' },
      isVoice: { required: false, type: 'boolean' }
    },
    [MESSAGE_TYPES.VIDEO]: {
      url: { required: true, type: 'string' },
      caption: { required: false, type: 'string' }
    },
    [MESSAGE_TYPES.DOCUMENT]: {
      url: { required: true, type: 'string' },
      filename: { required: false, type: 'string' }
    },
    [MESSAGE_TYPES.LOCATION]: {
      latitude: { required: true, type: 'number' },
      longitude: { required: true, type: 'number' },
      name: { required: false, type: 'string' },
      address: { required: false, type: 'string' }
    },
    [MESSAGE_TYPES.INTERACTIVE]: {
      buttons: { required: false, type: 'array' },
      list: { required: false, type: 'object' }
    }
  }
};
