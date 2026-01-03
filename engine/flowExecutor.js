// engine/flowExecutor.js
import axios from "axios";
import vm from "vm";

import {
  expandTemplate,
  buildScopes,
  makeScopeProxy,
  resolveVar,
} from "../utils/varScopes.js";
import { loadSession, saveSession } from "./sessionManager.js";
import { distribuirTicket } from "./ticketManager.js";
import { CHANNELS } from "./messageTypes.js";
import { isOpenNow } from "./businessHours.js";
import { loadQueueBH } from "./queueHoursService.js";
import { sendMessageByChannel } from "./messenger.js";
import { logBlockTransition } from "./transitionLogger.js";
import { pickQueueByDBRules } from "./services/queueRulesStore.js";

import {
  resolveOnErrorId,
  parseInboundMessage,
  buildInteractiveAliases,
  determineNextSmart,
  sendConfiguredMessage,
  resolveByIdOrLabel,
  buildProtocol,
  normalizeStr,
  splitMulti,
  anyMatch,
  allMismatch,
  checkCondition,
} from "./helpers.js";

// 🔗 Contact store (DB clientes)
import {
  upsertContactFromSession,
  loadContactByUserId,
  updateContactFields,
  diffContact,
} from "./services/contactStore.js";

import { emitToRoom } from "../services/realtime/emitToRoom.js";

// cache TTL (ms)
const CONTACT_CACHE_TTL_MS = Number(
  process.env.CONTACT_CACHE_TTL_MS || 5 * 60 * 1000
);

// decide se cache expirou
function shouldRefreshContact(sessionVars) {
  try {
    const last = Number(sessionVars?._contact_synced_at || 0);
    const expired = !last || Date.now() - last > CONTACT_CACHE_TTL_MS;
    const hasContact = !!sessionVars?.contact?.id;
    const channelChanged = !!(
      sessionVars?.contact?.channel &&
      sessionVars?.channel &&
      sessionVars.contact.channel !== sessionVars.channel
    );
    return !hasContact || expired || channelChanged;
  } catch {
    return true;
  }
}

// permitir a resolução no Graph somente 1x por sessão
function canResolveMetaThisSession(sessionVars) {
  return !sessionVars?._contact_meta_resolved; // flag persistida na sessão
}

const defaultPublish = (room, event, payload) =>
  emitToRoom({ room, event, payload });

// Sufixos de armazenamento por canal
const STORAGE_SUFFIX = {
  whatsapp: "w.msgcli.net",
  telegram: "t.msgcli.net",
  facebook: "f.msgcli.net",
  instagram: "i.msgcli.net",
};
function makeStorageUserId(rawUserId, channel) {
  const ch = String(channel || "").toLowerCase();
  const domain = STORAGE_SUFFIX[ch] || STORAGE_SUFFIX.whatsapp;
  return `${rawUserId}@${domain}`;
}

/* --------------------------- helpers p/ tracker --------------------------- */

function trimStr(s, max = 2000) {
  if (s == null) return s;
  s = String(s);
  return s.length > max ? s.slice(0, max) + "…" : s;
}

function safeJson(v, max = 2000) {
  try {
    const s = typeof v === "string" ? v : JSON.stringify(v);
    return trimStr(s, max);
  } catch {
    return String(v);
  }
}

function buildHttpMeta({ url, method, status, durationMs, reqBody, resBody }) {
  return {
    url,
    method,
    status,
    durationMs,
    reqPreview: reqBody == null ? undefined : safeJson(reqBody, 1200),
    resPreview: resBody == null ? undefined : safeJson(resBody, 1200),
  };
}

function buildErrorMeta(err, kind = "script", extra = {}) {
  const isHttp = Boolean(err?.isAxiosError || extra?.kind === "http");
  return {
    kind: isHttp ? "http" : kind,
    message: String(err?.message || err),
    code: err?.code ?? err?.response?.status ?? null,
    stack: trimStr(err?.stack, 2000),
    ...extra,
    http: isHttp
      ? buildHttpMeta({
          url: extra?.url || err?.config?.url,
          method: (extra?.method || err?.config?.method || "GET").toUpperCase(),
          status: err?.response?.status,
          durationMs: extra?.durationMs,
          reqBody: err?.config?.data,
          resBody: err?.response?.data,
        })
      : undefined,
  };
}

// Buffer de metadados por etapa
function mergeStageMeta(sessionVars, patch) {
  if (!sessionVars._stage_meta) sessionVars._stage_meta = {};
  Object.assign(sessionVars._stage_meta, patch || {});
}
function flushStageMetaInto(varsObj) {
  if (!varsObj) return varsObj;
  if (varsObj._stage_meta && Object.keys(varsObj._stage_meta).length > 0) {
    varsObj.stage_meta = {
      ...(varsObj.stage_meta || {}),
      ...varsObj._stage_meta,
    };
    varsObj._stage_meta = {};
  }
  return varsObj;
}

// extrai nomes de parâmetros de uma função (string)
function getParamNames(fnSrc = "") {
  try {
    const s = String(fnSrc);
    const mFn =
      s.match(/function\s+[^(]*\(([^)]*)\)/) ||
      s.match(/^[\s\(]*function\s*\(([^)]*)\)/) ||
      s.match(/^[\s\(]*([^\)]*)\)\s*=>/);
    if (!mFn) return [];
    const inside = mFn[1] || "";
    return inside
      .split(",")
      .map((p) => p.trim())
      .filter(Boolean)
      .map((p) =>
        p
          .replace(/=[^,]+$/, "")
          .replace(/^\.\.\./, "")
          .trim()
      );
  } catch {
    return [];
  }
}

/* --------------------------- set/assign & ações especiais --------------------------- */

function setPath(obj, path, value) {
  if (!obj || !path) return;
  const parts = String(path).split(".").filter(Boolean);
  let cur = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    const k = parts[i];
    if (typeof cur[k] !== "object" || cur[k] === null) cur[k] = {};
    cur = cur[k];
  }
  cur[parts[parts.length - 1]] = value;
}

function assignScoped({ scope, key, value }, { sessionVars, scopes }) {
  if (!key) return;
  const v = typeof value === "string" ? expandTemplate(value, scopes) : value;
  if (scope === "contact") {
    if (!sessionVars.contact) sessionVars.contact = {};
    setPath(sessionVars.contact, key, v);
  } else if (scope === "contact.extra") {
    if (!sessionVars.contact) sessionVars.contact = {};
    if (
      !sessionVars.contact.extra ||
      typeof sessionVars.contact.extra !== "object"
    ) {
      sessionVars.contact.extra = {};
    }
    setPath(sessionVars.contact.extra, key, v);
  } else {
    if (!sessionVars.context || typeof sessionVars.context !== "object") {
      sessionVars.context = {};
    }
    setPath(sessionVars.context, key, v);
  }
}

function saveInboundTextToVarPath(varPath, inbound, { sessionVars }) {
  if (!varPath) return;
  const text = inbound?.title ?? inbound?.text ?? "";
  const payload = {
    type: inbound?.type ?? null,
    text: text || null,
    id: inbound?.id ?? null,
    title: inbound?.title ?? null,
    raw: inbound ?? null,
  };

  sessionVars.lastcontentmessage = payload;

  const norm = String(varPath).trim();
  if (!norm) return;

  if (norm.startsWith("contact.extra.")) {
    if (!sessionVars.contact) sessionVars.contact = {};
    if (
      !sessionVars.contact.extra ||
      typeof sessionVars.contact.extra !== "object"
    ) {
      sessionVars.contact.extra = {};
    }
    setPath(
      sessionVars.contact.extra,
      norm.replace(/^contact\.extra\./, ""),
      text
    );
  } else if (norm.startsWith("contact.")) {
    if (!sessionVars.contact) sessionVars.contact = {};
    setPath(sessionVars.contact, norm.replace(/^contact\./, ""), text);
  } else if (norm.startsWith("context.")) {
    if (!sessionVars.context || typeof sessionVars.context !== "object") {
      sessionVars.context = {};
    }
    setPath(sessionVars.context, norm.replace(/^context\./, ""), text);
  } else {
    setPath(sessionVars, norm, text);
  }
}

/* --------------------------- condições / ações especiais --------------------------- */

async function runSpecialActions(list, { sessionVars, scopes, phase }) {
  try {
    const arr = Array.isArray(list) ? list : [];
    for (const a of arr) {
      const conds = Array.isArray(a?.conditions) ? a.conditions : [];
      const ok =
        conds.length === 0
          ? true
          : conds.every((c) => checkCondition(c, scopes));
      if (!ok) continue;

      const scope = a?.scope || "context";
      const key = a?.key || "";
      const value = a?.value ?? "";
      assignScoped({ scope, key, value }, { sessionVars, scopes });
    }
    return buildScopes(sessionVars);
  } catch (e) {
    console.error(
      `[flowExecutor] Erro em ações especiais (${phase}):`,
      e?.message
    );
    return scopes;
  }
}

/* --------------------------- executor --------------------------- */

export async function runFlow({
  message,
  flow,
  vars,
  rawUserId,
  publish = defaultPublish,

  // metadata de versão/deploy do fluxo
  flowVersionId = null,
  flowDeploymentId = null,
  environment = (process.env.FLOW_ENV || "prod").toLowerCase(),
}) {
  const chForStorage =
    (vars && vars.channel) ||
    (typeof vars === "object" ? vars?.channel : null) ||
    CHANNELS.WHATSAPP;

  const userId = makeStorageUserId(rawUserId, chForStorage);

  console.log("🔍 RAW MESSAGE STRUCTURE:");
  console.dir(message, { depth: 5 });
  console.log("🔍 FLOW START:", flow?.start);
  console.log("🔍 FLOW BLOCKS:", flow?.blocks ? Object.keys(flow.blocks) : []);

  if (!flow || !flow.blocks || !flow.start) {
    return flow?.onError?.content || "Erro interno no bot";
  }

  const onErrorId = resolveOnErrorId(flow);
  const baseFlowId = flow?.id ?? null;

  // 🔹 carrega sessão
  const session = await loadSession(userId);

  // Guard humano
  if (
    session?.current_block === "human" &&
    (session?.vars?.handover?.status || "open") !== "closed"
  ) {
    console.log("🙅 Human mode active — ignoring inbound for automation");
    await saveSession(
      userId,
      "human",
      flow.id,
      flushStageMetaInto({ ...(session?.vars || {}) })
    );
    return null;
  }

  // injeta version/deployment/env/flow no início
  let sessionVars = {
    ...(vars || {}),
    ...(session?.vars || {}),
    flowId: vars?.flowId ?? session?.vars?.flowId ?? baseFlowId,
    flowVersionId:
      vars?.flowVersionId ??
      session?.vars?.flowVersionId ??
      flowVersionId ??
      null,
    flowDeploymentId:
      vars?.flowDeploymentId ??
      session?.vars?.flowDeploymentId ??
      flowDeploymentId ??
      null,
    environment:
      vars?.environment ??
      session?.vars?.environment ??
      environment ??
      "prod",
  };

  if (!sessionVars.channel) sessionVars.channel = CHANNELS.WHATSAPP;

  // 🔹 normaliza ids
  if (!sessionVars.userId || !sessionVars.userId.includes("@")) {
    sessionVars.userId = userId;
  }
  if (!sessionVars.userIdRaw) sessionVars.userIdRaw = rawUserId;

  // 🔹 CONTACT: upsert/cache/meta
  let contact;
  const wantRefresh = shouldRefreshContact(sessionVars);
  const allowMetaThisSession = canResolveMetaThisSession(sessionVars);

  if (wantRefresh) {
    const leanMode = !allowMetaThisSession;
    contact = await upsertContactFromSession({
      userId: sessionVars.userId,
      rawUserId: sessionVars.userIdRaw,
      channel: sessionVars.channel,
      meta: {
        pageId: sessionVars.metaPageId || null,
        flow_id: sessionVars.flowId || baseFlowId || null,
      },
      contactPatch: {
        name: sessionVars.userName ?? sessionVars.name ?? undefined,
        phone: sessionVars.userPhone ?? sessionVars.phone ?? undefined,
        email: sessionVars.email ?? undefined,
        document: sessionVars.document ?? undefined,
        // 🔹 AQUI: garante que mandamos pro contato também
        flow_id: sessionVars.flowId ?? baseFlowId ?? null,
      },
      options: { lean: leanMode },
    });

    sessionVars._contact_meta_resolved = true;
    sessionVars._contact_synced_at = Date.now();
  } else {
    contact = sessionVars.contact;
  }

  if (sessionVars.userName && !contact?.name)
    contact =
      (await updateContactFields(sessionVars.userId, {
        name: sessionVars.userName,
      })) || contact;
  if (sessionVars.userPhone && !contact?.phone)
    contact =
      (await updateContactFields(sessionVars.userId, {
        phone: sessionVars.userPhone,
      })) || contact;
  if (!contact?.channel && sessionVars.channel)
    contact =
      (await updateContactFields(sessionVars.userId, {
        channel: sessionVars.channel,
      })) || contact;

  delete sessionVars.userName;
  delete sessionVars.userPhone;

  // espelho simples
  sessionVars.contact = contact;
  sessionVars.id = contact?.id || sessionVars.userId;
  sessionVars.name = contact?.name || sessionVars.name || null;
  sessionVars.phone = contact?.phone || sessionVars.phone || null;
  sessionVars.document = contact?.document || sessionVars.document || null;
  sessionVars.email = contact?.email || sessionVars.email || null;
  sessionVars.channel =
    sessionVars.channel || contact?.channel || CHANNELS.WHATSAPP;

  // 🔹 Protocolo base
  if (!sessionVars.protocol) {
    sessionVars.protocol = buildProtocol(sessionVars);
  }

  // 🔹 Escopos p/ expandTemplate / S proxy
  let scopes = buildScopes(sessionVars);

  if (sessionVars?.context?.queue) {
    sessionVars.queue = sessionVars.context.queue;
    scopes = buildScopes(sessionVars);
  }

  console.log("💾 Session loaded:", session);
  console.log("📊 Session vars:", sessionVars);

  let currentBlockId = null;

  const inbound = parseInboundMessage(message);
  console.log("🧠 Parsed message:", inbound);

  // Bufferiza preview da entrada
  if (inbound && (inbound.title || inbound.text || inbound.id)) {
    const incomingPreview = inbound.title ?? inbound.text ?? inbound.id ?? "";
    mergeStageMeta(sessionVars, {
      last_incoming: trimStr(String(incomingPreview), 1200),
    });
  }

  // humano (retomada)
  if (session?.current_block === "human") {
    console.log("🤖 Session is in human mode");
    const sVars = { ...(session?.vars || {}) };
    if (sVars?.handover?.status === "closed") {
      console.log("🔙 Returning from human handover");
      const originId = sVars?.handover?.originBlock;
      const originBlock = originId ? flow.blocks[originId] : null;

      let nextFromHuman = originBlock
        ? determineNextSmart(originBlock, sVars, flow, originId)
        : null;

      if (!nextFromHuman || !flow.blocks[nextFromHuman]) {
        if (flow.blocks?.onhumanreturn) nextFromHuman = "onhumanreturn";
        else if (onErrorId) nextFromHuman = onErrorId;
        else nextFromHuman = flow.start;
      }

      sessionVars = { ...(vars || {}), ...sVars };

      const fresh = await loadContactByUserId(sessionVars.userId);
      sessionVars.contact = fresh || contact;
      sessionVars.id = sessionVars.contact?.id || sessionVars.userId;
      sessionVars.name = sessionVars.contact?.name || sessionVars.name || null;
      sessionVars.phone =
        sessionVars.contact?.phone || sessionVars.phone || null;
      sessionVars.document =
        sessionVars.contact?.document || sessionVars.document || null;
      sessionVars.email =
        sessionVars.contact?.email || sessionVars.email || null;

      // preserva metadata de versão/deploy/env
      sessionVars.flowVersionId =
        sessionVars.flowVersionId ?? flowVersionId ?? null;
      sessionVars.flowDeploymentId =
        sessionVars.flowDeploymentId ?? flowDeploymentId ?? null;
      sessionVars.environment =
        sessionVars.environment ?? environment ?? "prod";

      scopes = buildScopes(sessionVars);
      currentBlockId = nextFromHuman;
    } else {
      console.log("📞 Distributing ticket for human session");
      try {
        const dist = await distribuirTicket(
          rawUserId,
          sVars.queue || sVars?.context?.queue || null,
          sVars.channel
        );
        if (dist?.ticketNumber || dist?.ticketId || dist?.id) {
          const t = String(dist.ticketNumber || dist.ticketId || dist.id);
          sVars.ticketNumber = t;
          sVars.protocol = buildProtocol({ ...sVars, ticketNumber: t });
          await saveSession(
            userId,
            "human",
            flow.id,
            flushStageMetaInto(sVars)
          );
        }
      } catch (e) {
        console.error(
          "[flowExecutor] Falha ao distribuir ticket (sessão humana):",
          e
        );
      }
      return null;
    }
  }

  // inicial / retomada
  if (currentBlockId == null) {
    console.log("🔄 Determining starting block");

    if (session?.current_block === "human") {
      console.log("🙅 Human mode (redundant guard) — stopping here");
      return null;
    }

    if (session?.current_block && flow.blocks[session.current_block]) {
      const stored = session.current_block;
      console.log("📦 Resuming from stored block:", stored);

      if (stored === "despedida") {
        currentBlockId = flow.start;
        console.log("🔄 Restarting flow from start (despedida)");
      } else {
        const awaiting = flow.blocks[stored];
        console.log("⏳ Block awaiting response:", awaiting?.label);

        if (awaiting.actions && awaiting.actions.length > 0) {
          if (!message && stored !== flow.start) {
            console.log("🚫 No message, staying on current block");
            return null;
          }

          const hasInbound = !!(
            inbound &&
            (inbound.title || inbound.text || inbound.id)
          );
          sessionVars.lastUserMessage = hasInbound
            ? inbound.title ?? inbound.text ?? inbound.id ?? ""
            : sessionVars.lastUserMessage ?? "";
          sessionVars.lastReplyId = hasInbound
            ? inbound.id ?? null
            : sessionVars.lastReplyId ?? null;
          sessionVars.lastReplyTitle = hasInbound
            ? inbound.title ?? null
            : sessionVars.lastReplyTitle ?? null;
          sessionVars.lastMessageType = hasInbound
            ? inbound.type
            : sessionVars.lastMessageType ?? "init";

          if (hasInbound && awaiting?.saveResponseVar) {
            try {
              saveInboundTextToVarPath(awaiting.saveResponseVar, inbound, {
                sessionVars,
              });
              scopes = buildScopes(sessionVars);
            } catch (e) {
              console.error(
                "[flowExecutor] saveResponseVar failed:",
                e?.message
              );
            }
          }

          if (awaiting?.type === "interactive") {
            const aliases = buildInteractiveAliases(awaiting);

            if (sessionVars.lastReplyId && !sessionVars.lastReplyTitle) {
              sessionVars.lastReplyTitle =
                aliases.id2title[sessionVars.lastReplyId] ||
                sessionVars.lastReplyTitle;
            }
            if (sessionVars.lastReplyTitle && !sessionVars.lastReplyId) {
              const id =
                aliases.title2id[normalizeStr(sessionVars.lastReplyTitle)];
              sessionVars.lastReplyId = id || sessionVars.lastReplyId;
            }

            sessionVars._candidates = Array.from(
              new Set(
                [
                  sessionVars.lastUserMessage,
                  sessionVars.lastReplyTitle,
                  sessionVars.lastReplyId,
                  aliases.id2title[sessionVars.lastReplyId],
                  aliases.title2id?.[normalizeStr(sessionVars.lastReplyTitle)],
                ].filter(Boolean)
              )
            );
          }

          let next = determineNextSmart(awaiting, sessionVars, flow, stored);
          console.log("➡️ Next block from actions:", next);

          if (
            next &&
            next !== stored &&
            Array.isArray(awaiting.onExit) &&
            awaiting.onExit.length > 0
          ) {
            try {
              scopes = await runSpecialActions(awaiting.onExit, {
                sessionVars,
                scopes,
                phase: "exit",
              });
              if (sessionVars?.context?.queue) {
                sessionVars.queue = sessionVars.context.queue;
                scopes = buildScopes(sessionVars);
              }
            } catch (e) {
              console.error(
                "[flowExecutor] onExit (retomada) falhou:",
                e?.message
              );
            }
          }

          if (!next && onErrorId) next = onErrorId;
          currentBlockId = next || stored;
        } else {
          currentBlockId = stored;
        }
      }
    } else {
      currentBlockId = flow.start;
      console.log("🚀 Starting new session from flow start");

      const hasInbound = !!(
        inbound &&
        (inbound.title || inbound.text || inbound.id)
      );
      sessionVars.lastUserMessage = hasInbound
        ? inbound.title ?? inbound.text ?? inbound.id
        : "";
      sessionVars.lastReplyId = hasInbound ? inbound.id ?? null : null;
      sessionVars.lastReplyTitle = hasInbound ? inbound.title ?? null : null;
      sessionVars.lastMessageType = hasInbound ? inbound.type : "init";
    }
  }

  console.log("🎯 Current block ID:", currentBlockId);

  let lastResponse = null;

  // snapshot do contato no início do loop
  let originalContact = { ...(sessionVars.contact || {}) };

  while (currentBlockId) {
    const block = flow.blocks[currentBlockId];
    if (!block) {
      console.error("❌ Block not found:", currentBlockId);
      break;
    }

    /* ---------- onEnter ---------- */
    if (block.onEnter && sessionVars._enteredBlockId !== currentBlockId) {
      scopes = await runSpecialActions(block.onEnter, {
        sessionVars,
        scopes,
        phase: "enter",
      });
      sessionVars._enteredBlockId = currentBlockId;
      if (sessionVars?.context?.queue) {
        sessionVars.queue = sessionVars.context.queue;
        scopes = buildScopes(sessionVars);
      }
    }

    // transição com version/deployment/env
    if (sessionVars._lastLoggedBlock !== currentBlockId) {
      const blockLabel = (block && (block.label || block.id)) || currentBlockId;
      await logBlockTransition({
        userId,
        channel: sessionVars.channel || CHANNELS.WHATSAPP,
        flowId: flow.id,
        blockId: currentBlockId,
        blockLabel,
        blockType: block?.type || null,
        vars: flushStageMetaInto(sessionVars),
        ticketNumber: sessionVars.ticketNumber,
        flowVersionId: sessionVars.flowVersionId ?? null,
        flowDeploymentId: sessionVars.flowDeploymentId ?? null,
        environment: sessionVars.environment ?? null,
      });
      sessionVars.current_block_label = blockLabel;
      sessionVars._lastLoggedBlock = currentBlockId;
    }

    console.log(
      "🏃‍♂️ Processing block:",
      block.label || block.id,
      "type:",
      block.type
    );

    /* ---------- HUMAN + horários por queue ---------- */
    if (block.type === "human") {
      console.log("👥 Human handover block detected");

      if (block.content?.queueName) {
        sessionVars.queue = block.content.queueName;
        console.log(`🧭 Queue (from block): ${sessionVars.queue}`);
      }
      if (sessionVars?.context?.queue) {
        sessionVars.queue = sessionVars.context.queue;
        scopes = buildScopes(sessionVars);
        console.log(`🧭 Queue (from context): ${sessionVars.queue}`);
      }
      try {
        const queueByRule = await pickQueueByDBRules(
          sessionVars,
          sessionVars.queue || null
        );
        if (queueByRule) sessionVars.queue = queueByRule;
      } catch (e) {
        console.error("[flowExecutor] queue_rules resolver error:", e?.message);
      }

      if (!sessionVars.queue) sessionVars.queue = "Atendimento Geral";
      console.log(`🧭 Queue selected: ${sessionVars.queue}`);

      const bhCfg = await loadQueueBH(sessionVars.queue);
      const { open, reason } = bhCfg
        ? isOpenNow(bhCfg)
        : { open: true, reason: null };

      sessionVars.offhours = !open;
      sessionVars.offhours_reason = reason;
      sessionVars.isHoliday = reason === "holiday";
      sessionVars.offhours_queue = sessionVars.queue || null;

      console.log("🕒 Business hours check - open:", open, "reason:", reason);

      if (!open) {
        console.log("🚫 Outside business hours");

        const msgCfg =
          reason === "holiday" && bhCfg?.off_hours?.holiday
            ? bhCfg.off_hours.holiday
            : reason === "closed" && bhCfg?.off_hours?.closed
            ? bhCfg.off_hours.closed
            : bhCfg?.off_hours || null;

        if (msgCfg) {
          console.log("📤 Sending off-hours message");
          await sendConfiguredMessage(msgCfg, {
            channel: sessionVars.channel || CHANNELS.WHATSAPP,
            userId,
            vars: sessionVars,
          });

          mergeStageMeta(sessionVars, {
            last_outgoing: trimStr(
              typeof msgCfg === "string" ? msgCfg : JSON.stringify(msgCfg),
              1200
            ),
          });
        }

        let cfgNext = null;
        if (reason === "holiday") {
          cfgNext =
            msgCfg?.next ??
            bhCfg?.off_hours?.nextHoliday ??
            bhCfg?.off_hours?.next ??
            null;
        } else {
          cfgNext =
            msgCfg?.next ??
            bhCfg?.off_hours?.nextClosed ??
            bhCfg?.off_hours?.next ??
            null;
        }

        let nextBlock = determineNextSmart(
          block,
          sessionVars,
          flow,
          currentBlockId
        );
        console.log("➡️ Next block from conditions:", nextBlock);

        if (!nextBlock && cfgNext) {
          nextBlock = resolveByIdOrLabel(flow, cfgNext);
          console.log("➡️ Next block from config:", nextBlock);
        }

        if (!nextBlock)
          nextBlock = flow.blocks?.offhours
            ? "offhours"
            : resolveOnErrorId(flow);
        nextBlock = nextBlock || currentBlockId;

        console.log("💾 Saving session with next block:", nextBlock);
        await saveSession(
          userId,
          nextBlock,
          flow.id,
          flushStageMetaInto(sessionVars)
        );
        currentBlockId = nextBlock;
        continue;
      }

      console.log("✅ Business hours - open, proceeding with handover");

      const preEnabled = bhCfg?.pre_human?.enabled !== false;
      const preAlreadySent = !!sessionVars.handover?.preMsgSent;

      if (preEnabled && !preAlreadySent) {
        console.log("📤 Sending pre-human message");
        if (bhCfg?.pre_human) {
          await sendConfiguredMessage(bhCfg.pre_human, {
            channel: sessionVars.channel || CHANNELS.WHATSAPP,
            userId,
            vars: sessionVars,
          });
          mergeStageMeta(sessionVars, {
            last_outgoing: trimStr(
              typeof bhCfg.pre_human === "string"
                ? bhCfg.pre_human
                : JSON.stringify(bhCfg.pre_human),
              1200
            ),
          });
        } else if (block.content?.transferMessage) {
          await sendConfiguredMessage(
            { type: "text", message: block.content.transferMessage },
            {
              channel: sessionVars.channel || CHANNELS.WHATSAPP,
              userId,
              vars: sessionVars,
            }
          );
          mergeStageMeta(sessionVars, {
            last_outgoing: trimStr(block.content.transferMessage, 1200),
          });
        }
      }

      sessionVars.handover = {
        ...(sessionVars.handover || {}),
        status: "open",
        originBlock: currentBlockId,
        preMsgSent: true,
      };
      sessionVars.previousBlock = currentBlockId;
      sessionVars.offhours = false;
      sessionVars.offhours_reason = null;
      sessionVars.isHoliday = false;

      console.log("💾 Saving human session");
      await saveSession(
        userId,
        "human",
        flow.id,
        flushStageMetaInto(sessionVars)
      );

      try {
        const dist = await distribuirTicket(
          rawUserId,
          sessionVars.queue,
          sessionVars.channel
        );
        if (dist?.ticketNumber || dist?.ticketId || dist?.id) {
          const t = String(dist.ticketNumber || dist.ticketId || dist.id);
          sessionVars.ticketNumber = t;
          sessionVars.protocol = buildProtocol({
            ...sessionVars,
            ticketNumber: t,
          });
          await saveSession(
            userId,
            "human",
            flow.id,
            flushStageMetaInto(sessionVars)
          );
        }
        console.log("✅ Ticket distributed successfully");
      } catch (e) {
        console.error("[flowExecutor] Falha ao distribuir ticket (human):", e);
        mergeStageMeta(sessionVars, {
          has_error: true,
          error: buildErrorMeta(e, "handover"),
        });
      }
      return null;
    }

    /* ---------- Conteúdo / API / Script ---------- */
    let content = "";
    if (block.content != null) {
      try {
        if (sessionVars.ticketNumber) {
          const nextProt = buildProtocol(sessionVars);
          if (sessionVars.protocol !== nextProt)
            sessionVars.protocol = nextProt;
        }

        content = expandTemplate(block.content, scopes);
        console.log(
          "📝 Block content processed:",
          typeof content === "string"
            ? content.substring(0, 100) + "..."
            : "[object]"
        );
      } catch (e) {
        console.error("[flowExecutor] Erro ao montar conteúdo do bloco:", e);
        mergeStageMeta(sessionVars, {
          has_error: true,
          error: buildErrorMeta(e, "content_build"),
        });
        content = "";
      }
    }

    try {
      if (block.type === "api_call") {
        console.log("🌐 API call block");

        const method = (block.method || "GET").toUpperCase();
        const url = expandTemplate(block.url, scopes);
        const payload = block.body
          ? expandTemplate(block.body, scopes)
          : undefined;
        const headers = block.headers || undefined;
        const timeout = Number(block.timeout || 10000);

        const started = Date.now();
        console.log("📡 API call to:", method, url);

        try {
          const res = await axios({
            method,
            url,
            data: payload,
            headers,
            timeout,
          });

          const durationMs = Date.now() - started;
          sessionVars.responseStatus = res.status;
          sessionVars.responseData = res.data;

          mergeStageMeta(sessionVars, {
            http: buildHttpMeta({
              url,
              method,
              status: res.status,
              durationMs,
              reqBody: payload,
              resBody: res.data,
            }),
          });

          if (block.script) {
            const S = makeScopeProxy(scopes);
            const sandbox = {
              response: res.data,
              output: "",
              console,
              S,
              fn: null,
            };
            vm.createContext(sandbox);

            const assignFn = `
              "use strict";
              with (S) { fn = ${block.script}; }
            `;
            try {
              vm.runInContext(assignFn, sandbox);
            } catch {
              sandbox.fn = null;
            }

            if (typeof sandbox.fn === "function") {
              const params = getParamNames(sandbox.fn.toString());
              const args = params.map((name) => resolveVar(name, scopes));
              let ret;
              try {
                ret = sandbox.fn(...args);
              } catch {
                ret = "";
              }
              content = sandbox.output ?? ret ?? "";
            } else {
              const wrapped = `
                (function(){
                  "use strict";
                  with (S) { ${block.script} }
                })();
              `;
              vm.runInContext(wrapped, sandbox);
              content = sandbox.output;
            }

            sessionVars.context = scopes.context;
            if (scopes?.contact) sessionVars.contact = scopes.contact;
            scopes = buildScopes(sessionVars);
          } else {
            content =
              typeof res.data === "string"
                ? res.data
                : JSON.stringify(res.data);
          }

          if (block.outputVar) sessionVars[block.outputVar] = content;
          if (block.statusVar) sessionVars[block.statusVar] = res.status;
        } catch (e) {
          const durationMs = Date.now() - started;
          console.error("❌ Erro em API call:", e?.message);
          const errMeta = buildErrorMeta(e, "http", {
            kind: "http",
            url,
            method,
            durationMs,
          });
          sessionVars.lastError = errMeta;
          mergeStageMeta(sessionVars, { has_error: true, error: errMeta });
        }
      } else if (block.type === "script") {
        console.log("📜 Script block");
        try {
          const S = makeScopeProxy(scopes);

          const argExprs = Array.isArray(block.args) ? block.args : [];
          const argVals = argExprs.map((expr) => resolveVar(expr, scopes));

          const sandbox = { output: "", console, S, __ARGS__: argVals };
          const fnName = block.function || "run";
          const code = `
            "use strict";
            with (S) {
              ${block.code}
              try {
                output = (typeof ${fnName} === 'function')
                  ? ${fnName}(...__ARGS__)
                  : '';
              } catch (e) { output = ''; throw e; }
            }
          `;
          vm.createContext(sandbox);
          vm.runInContext(code, sandbox);

          const outStr =
            sandbox.output?.toString?.() ?? String(sandbox.output ?? "");
          content = outStr;
          if (block.outputVar) sessionVars[block.outputVar] = sandbox.output;

          sessionVars.context = scopes.context;
          if (scopes?.contact) sessionVars.contact = scopes.contact;
          scopes = buildScopes(sessionVars);
        } catch (e) {
          console.error("❌ Erro em script:", e?.message);
          const errMeta = buildErrorMeta(e, "script");
          sessionVars.lastError = errMeta;
          mergeStageMeta(sessionVars, { has_error: true, error: errMeta });
        }
      }
    } catch (e) {
      console.error(
        "[flowExecutor] Erro executando api_call/script (wrapper):",
        e
      );
      mergeStageMeta(sessionVars, {
        has_error: true,
        error: buildErrorMeta(e, "exec_wrapper"),
      });
    }

    /* ---------- Envio ---------- */
    const sendableTypes = [
      "text",
      "image",
      "audio",
      "video",
      "file",
      "document",
      "location",
      "interactive",
    ];

    if (
      content &&
      (sendableTypes.includes(block.type) || block.type === "error")
    ) {
      const outType = block.type === "error" ? "text" : block.type;
      console.log("📤 Sending message of type:", outType);

      if (block.sendDelayInSeconds) {
        const ms = Number(block.sendDelayInSeconds) * 1000;
        if (!Number.isNaN(ms) && ms > 0) {
          console.log("⏳ Delaying send by", ms, "ms");
          await new Promise((r) => setTimeout(r, ms));
        }
      }

      try {
        const messageContent =
          outType === "text"
            ? typeof content === "string"
              ? { text: content }
              : content?.text
              ? { text: String(content.text) }
              : {
                  text: String(
                    content?.message ??
                      content ??
                      "Ocorreu um erro. Tente novamente."
                  ),
                }
            : typeof content === "string"
            ? { text: content }
            : content;

        const meta = {
          block: currentBlockId,
          flow_id: flow.id,
          ...(sessionVars?.validation
            ? { validation: sessionVars.validation }
            : {}),
          flow_version_id: sessionVars.flowVersionId ?? null,
          flow_deployment_id: sessionVars.flowDeploymentId ?? null,
          environment: sessionVars.environment ?? null,
        };

        const pendingRecord = await sendMessageByChannel(
          sessionVars.channel || CHANNELS.WHATSAPP,
          userId,
          outType,
          messageContent,
          undefined,
          meta
        );
        lastResponse = pendingRecord;
        console.log("✅ Message sent successfully");

        mergeStageMeta(sessionVars, {
          last_outgoing: trimStr(
            typeof content === "string" ? content : JSON.stringify(content),
            1200
          ),
          send: { type: outType, isFallback: false },
        });

        if (pendingRecord) {
          await publish(userId, "new_message", pendingRecord);
        }
      } catch (mediaErr) {
        console.error(
          "❌ Falha ao enviar mídia (será enviado fallback):",
          mediaErr
        );

        mergeStageMeta(sessionVars, {
          has_error: true,
          error: buildErrorMeta(mediaErr, "send", {
            send: { typeTried: outType },
          }),
        });

        const fallback =
          typeof content === "object" && content?.url
            ? `Aqui está seu conteúdo: ${content.url}`
            : typeof content === "string"
            ? content
            : "Não foi possível enviar o conteúdo solicitado.";
        try {
          const pendingFallback = await sendMessageByChannel(
            sessionVars.channel || CHANNELS.WHATSAPP,
            userId,
            "text",
            { text: fallback },
            undefined,
            {
              block: currentBlockId,
              flow_id: flow.id,
              fallback: true,
              flow_version_id: sessionVars.flowVersionId ?? null,
              flow_deployment_id: sessionVars.flowDeploymentId ?? null,
              environment: sessionVars.environment ?? null,
            }
          );
          lastResponse = pendingFallback;

          mergeStageMeta(sessionVars, {
            last_outgoing: trimStr(fallback, 1200),
            send: { type: "text", isFallback: true },
          });

          if (pendingFallback) {
            await publish(userId, "new_message", pendingFallback);
          }
        } catch (fallbackErr) {
          console.error("❌ Falha ao enviar fallback de texto:", fallbackErr);
          mergeStageMeta(sessionVars, {
            has_error: true,
            error: buildErrorMeta(fallbackErr, "send_fallback"),
          });
        }
      }
    }

    /* ---------- Persistência de alterações no contato ---------- */
    try {
      const nowContact = sessionVars.contact || {};
      const patch = diffContact(originalContact, nowContact);
      if (Object.keys(patch).length > 0) {
        const updated = await updateContactFields(sessionVars.userId, patch);
        if (updated) {
          sessionVars.contact = updated;
          sessionVars.id = updated.id;
          sessionVars.name = updated.name;
          sessionVars.phone = updated.phone;
          sessionVars.document = updated.document;
          sessionVars.email = updated.email;
        }
        originalContact = { ...(sessionVars.contact || {}) };
        scopes = buildScopes(sessionVars);
      }
    } catch (e) {
      console.error("⚠️ Falha ao persistir alterações de contact:", e?.message);
    }

    /* ---------- Próximo ---------- */
    let nextBlock;
    if (currentBlockId === onErrorId) {
      console.log("🔄 Returning from error block");
      const back = sessionVars.previousBlock;
      nextBlock = back && flow.blocks[back] ? back : flow.start;
    } else {
      nextBlock = determineNextSmart(block, sessionVars, flow, currentBlockId);
      console.log("➡️ Next block determined:", nextBlock);
    }

    let resolvedBlock = block.awaitResponse ? currentBlockId : nextBlock;
    console.log(
      "⏸️ Block awaits response:",
      block.awaitResponse,
      "Resolved block:",
      resolvedBlock
    );

    if (typeof resolvedBlock === "string" && resolvedBlock.includes("{")) {
      resolvedBlock = expandTemplate(resolvedBlock, scopes);
      console.log("🔧 Resolved dynamic block:", resolvedBlock);
    }

    if (resolvedBlock && !flow.blocks[resolvedBlock]) {
      console.log("❌ Resolved block not found, using onError");
      resolvedBlock = onErrorId || null;
    }

    const redirectingToStart =
      resolvedBlock === flow.start && currentBlockId !== flow.start;
    if (redirectingToStart) {
      console.log("🔄 Redirecting to flow start");

      if (block.onExit && resolvedBlock !== currentBlockId) {
        scopes = await runSpecialActions(block.onExit, {
          sessionVars,
          scopes,
          phase: "exit",
        });
      }
      if (sessionVars?.context?.queue) {
        sessionVars.queue = sessionVars.context.queue;
        scopes = buildScopes(sessionVars);
      }

      await saveSession(
        userId,
        flow.start,
        flow.id,
        flushStageMetaInto(sessionVars)
      );
      break;
    }

    if (
      currentBlockId !== onErrorId &&
      resolvedBlock &&
      resolvedBlock !== onErrorId
    ) {
      sessionVars.previousBlock = currentBlockId;
    }

    console.log("💾 Saving session with block:", resolvedBlock);

    if (block.onExit && resolvedBlock && resolvedBlock !== currentBlockId) {
      scopes = await runSpecialActions(block.onExit, {
        sessionVars,
        scopes,
        phase: "exit",
      });

      if (sessionVars?.context?.queue) {
        sessionVars.queue = sessionVars.context.queue;
        scopes = buildScopes(sessionVars);
      }
    }

    await saveSession(
      userId,
      resolvedBlock,
      flow.id,
      flushStageMetaInto(sessionVars)
    );

    if (block.awaitResponse) {
      console.log("⏸️ Awaiting response, breaking loop");
      break;
    }

    if (
      block.awaitTimeInSeconds != null &&
      block.awaitTimeInSeconds !== false &&
      !isNaN(Number(block.awaitTimeInSeconds)) &&
      Number(block.awaitTimeInSeconds) > 0
    ) {
      const delay = Number(block.awaitTimeInSeconds) * 1000;
      console.log("⏳ Awaiting time:", delay, "ms");
      await new Promise((r) => setTimeout(r, delay));
    }

    currentBlockId = resolvedBlock;
    console.log("🔄 Moving to next block:", currentBlockId);
  }

  console.log("🏁 Flow execution completed");
  return lastResponse;
}
