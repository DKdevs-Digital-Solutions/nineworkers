// utils/varScopes.js

/** MIGRA os campos legados para contact.{...} e garante defaults */
export function ensureContact(sessionVars = {}, rawUserId = null) {
  // preserva caso já exista
  const c0 = sessionVars.contact && typeof sessionVars.contact === 'object'
    ? sessionVars.contact
    : {};

  const plain = sessionVars || {};

  // telefones: tenta rawUserId como fallback (apenas dígitos)
  const phoneFromRaw = rawUserId ? String(rawUserId).replace(/\D+/g, '') : undefined;

  const contact = {
    ...c0,

    // nomes (novo → velho → outro velho → vazio)
    name:      c0.name      ?? plain.name      ?? plain.userName ?? null,
    phone:     c0.phone     ?? plain.phone     ?? plain.userPhone ?? phoneFromRaw ?? null,
    id:        c0.id        ?? plain.id        ?? plain.userId    ?? null,

    // espelho útil
    channel:   c0.channel   ?? plain.channel   ?? null,

    // documentos
    document:  c0.document  ?? plain.document  ?? null,
    email:     c0.email     ?? plain.email     ?? null,
  };

  // garante que `id` tenha um valor final (se possível)
  if (!contact.id && plain.userId) contact.id = plain.userId;

  // aplica no sessionVars
  sessionVars.contact = contact;

  // opcional: limpeza suave dos legados (não apaga nada do DB; só evita reaparecer em memória)
  delete sessionVars.userName;
  delete sessionVars.userPhone;
  delete sessionVars.id;       // vamos preferir contact.id
  // NÃO apagamos sessionVars.userId porque o motor usa em outros pontos
  // mas você pode escolher remover se já estiver tudo migrado.
  // delete sessionVars.userId;

  return sessionVars;
}

/**
 * Escopos usados na resolução de variáveis do fluxo.
 * - context: variáveis "puras" (implícitas), persistidas em sessionVars.context
 * - contact: dados do contato (com migração automática acima)
 * - session: o sessionVars inteiro (explícito via "session." ou "vars.")
 * - system: utilitárias (explícito via "system.")
 * - local: overlay temporário por bloco (não persistido)
 */
export function buildScopes(sessionVars = {}, local = {}) {
  // migração suave: se ainda vier "ctx", espelhe em "context"
  if (sessionVars.ctx && !sessionVars.context) {
    sessionVars.context = sessionVars.ctx;
  }

  // ⚠️ garante que contact foi normalizado
  if (!sessionVars.contact) ensureContact(sessionVars);

  const context  = sessionVars.context || {};
  const contact  = sessionVars.contact || {};
  const session  = sessionVars;
  const system   = {
    now: new Date(),
    nowISO: new Date().toISOString(),
    today: new Date().toISOString().slice(0, 10),
    channel: sessionVars.channel,
    userId: sessionVars.userId,       // legado p/ quem ainda usa
    protocol: sessionVars.protocol,
  };

  return { local, context, contact, session, system, vars: session };
}

/** Lê caminho "a.b.c" dentro de um objeto */
function getPath(obj, path) {
  if (!obj) return undefined;
  if (!path) return obj;
  let cur = obj;
  for (const part of String(path).split('.')) {
    if (cur == null) return undefined;
    cur = cur[part];
  }
  return cur;
}

/**
 * Resolve expressão:
 * - prefixos explícitos (local|context|contact|session|system|vars)
 * - sem prefixo: procura apenas em local → context (implícito)
 */
export function resolveVar(expr, scopes) {
  if (!expr) return undefined;
  const s = String(expr).trim();
  const [head, ...rest] = s.split('.');
  const tail = rest.join('.');

  if (['local','context','contact','session','system','vars'].includes(head)) {
    return getPath(scopes[head], tail);
  }

  // implícito: local > context
  const order = ['local', 'context'];
  for (const k of order) {
    const v = getPath(scopes[k], s);
    if (v !== undefined) return v;
  }

  // ✅ fallback compat (nível da sessão)
  const legacy = getPath(scopes.session, s);
  if (legacy !== undefined) return legacy;

  return undefined;
}


/** Expansão ${...}/{{...}} em strings/objetos/arrays */
export function expandTemplate(input, scopes, { keepUnknown = false } = {}) {
  if (input == null) return input;

  if (typeof input === 'string') {
    const re = /\$\{([^}]+)\}|\{\{([^}]+)\}\}/g;
    return input.replace(re, (_, a, b) => {
      const expr = (a ?? b ?? '').trim();
      const val = resolveVar(expr, scopes);
      if (val === undefined || val === null) return keepUnknown ? _ : '';
      if (typeof val === 'object') return JSON.stringify(val);
      return String(val);
    });
  }

  if (Array.isArray(input)) {
    return input.map((v) => expandTemplate(v, scopes, { keepUnknown }));
  }
  if (typeof input === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(input)) {
      out[k] = expandTemplate(v, scopes, { keepUnknown });
    }
    return out;
  }

  return input;
}

/** Proxy para `with (S) { ... }` (mesmo de antes) */
export function makeScopeProxy(scopes) {
  const handler = {
    has() { return true; },
    get(_, prop) {
      if (prop === Symbol.unscopables) return undefined;
      const key = String(prop);
      const prefix = key.split('.')[0];
      if (['local','context','contact','session','system','vars'].includes(prefix)) {
        return resolveVar(key, scopes);
      }
      const v = resolveVar(key, scopes); // implícito: local/context
      return v;
    },
    set(_, prop, value) {
      const key = String(prop);
      if (key.startsWith('context.')) {
        setPath(scopes.context, key.slice('context.'.length), value);
        return true;
      }
      if (key.startsWith('local.')) {
        setPath(scopes.local, key.slice('local.'.length), value);
        return true;
      }
      if (!scopes.context || typeof scopes.context !== 'object') scopes.context = {};
      scopes.context[key] = value;
      return true;
    }
  };
  return new Proxy({}, handler);
}

function setPath(obj, path, value) {
  const parts = String(path).split('.');
  let cur = obj;
  while (parts.length > 1) {
    const p = parts.shift();
    if (!cur[p] || typeof cur[p] !== 'object') cur[p] = {};
    cur = cur[p];
  }
  cur[parts[0]] = value;
}
