// engine/utils.js

/* Utilitários --------------------------------------------------------- */
function normalizeStr(v) {
  if (v == null) return '';
  let s = String(v);
  try {
    s = s.normalize('NFD').replace(/\p{Diacritic}/gu, '');
  } catch {}
  return s.toLowerCase();
}

function splitMulti(val) {
  if (Array.isArray(val)) return val.map((x) => String(x));
  if (val == null) return [];
  // separa por | ou ,  e remove espaços extras
  return String(val)
    .split(/[|,]/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function anyMatch(actualStr, candidates, cmp) {
  // OR: retorna true se qualquer candidato casar
  for (const c of candidates) {
    const cv = normalizeStr(c);
    if (cmp(actualStr, cv)) return true;
  }
  return false;
}

function allMismatch(actualStr, candidates, cmp) {
  // AND: retorna true se NENHUM candidato casar
  for (const c of candidates) {
    const cv = normalizeStr(c);
    if (cmp(actualStr, cv)) return false;
  }
  return true;
}

/* Regras de avaliação -------------------------------------------------- */
export function evaluateConditions(conditions = [], sessionVars = {}) {
  for (const cond of conditions || []) {
    // garante chaves esperadas mesmo se vier cond nula/curta
    const type = String(cond?.type || '').toLowerCase();
    const variable = cond?.variable;
    const rawValue = cond?.value;

    const actual = sessionVars?.[variable];
    const actualStr = normalizeStr(actual);

    // multi-valor: aceita array ou string "a|b|c" ou "a,b,c"
    const multi = splitMulti(rawValue);
    const hasMulti = multi.length > 1;

    switch (type) {
      case 'exists': {
        if (actual == null) return false;
        break;
      }

      case 'not_exists': {
        if (actual != null) return false;
        break;
      }

      case 'equals': {
        if (hasMulti) {
          // OR: qualquer um que seja igual passa
          if (!anyMatch(actualStr, multi, (a, b) => a === b)) return false;
        } else {
          const v = normalizeStr(rawValue);
          if (actualStr !== v) return false;
        }
        break;
      }

      case 'not_equals': {
        if (hasMulti) {
          // AND: precisa ser diferente de todos
          if (!allMismatch(actualStr, multi, (a, b) => a === b)) return false;
        } else {
          const v = normalizeStr(rawValue);
          if (actualStr === v) return false;
        }
        break;
      }

      case 'contains': {
        if (hasMulti) {
          // OR: contém qualquer um
          if (!anyMatch(actualStr, multi, (a, b) => a.includes(b))) return false;
        } else {
          const v = normalizeStr(rawValue);
          if (!actualStr.includes(v)) return false;
        }
        break;
      }

      case 'not_contains': {
        if (hasMulti) {
          // AND: não pode conter nenhum
          if (!allMismatch(actualStr, multi, (a, b) => a.includes(b))) return false;
        } else {
          const v = normalizeStr(rawValue);
          if (actualStr.includes(v)) return false;
        }
        break;
      }

      case 'starts_with': {
        if (hasMulti) {
          if (!anyMatch(actualStr, multi, (a, b) => a.startsWith(b))) return false;
        } else {
          const v = normalizeStr(rawValue);
          if (!actualStr.startsWith(v)) return false;
        }
        break;
      }

      case 'ends_with': {
        if (hasMulti) {
          if (!anyMatch(actualStr, multi, (a, b) => a.endsWith(b))) return false;
        } else {
          const v = normalizeStr(rawValue);
          if (!actualStr.endsWith(v)) return false;
        }
        break;
      }

      case 'regex': {
        // aceita multi (OR) aqui também
        const patterns = hasMulti ? multi : [String(rawValue ?? '')];
        const ok = patterns.some((p) => {
          try { return new RegExp(p, 'i').test(String(actual ?? '')); }
          catch { return false; }
        });
        if (!ok) return false;
        break;
      }

      case 'greater_than': {
        const a = parseFloat(actual);
        const b = parseFloat(Array.isArray(rawValue) ? rawValue[0] : rawValue);
        if (!(a > b)) return false;
        break;
      }

      case 'less_than': {
        const a = parseFloat(actual);
        const b = parseFloat(Array.isArray(rawValue) ? rawValue[0] : rawValue);
        if (!(a < b)) return false;
        break;
      }

      default:
        // tipo desconhecido => falha a condição
        return false;
    }
  }
  return true;
}

/* Seleção de próximo bloco -------------------------------------------- */
export function determineNextBlock(block, sessionVars, flow, currentBlockId) {
  // 1) Tenta todas as ações condicionais
  for (const action of block.actions || []) {
    if (evaluateConditions(action.conditions || [], sessionVars)) {
      return action.next;
    }
  }

  // 2) Se não achar, utiliza defaultNext (se existir)
  if (block.defaultNext && flow.blocks[block.defaultNext]) {
    return block.defaultNext;
  }

  // 3) Se estiver em onerror, retorna previousBlock
  if (currentBlockId === 'onerror' && sessionVars.previousBlock) {
    return sessionVars.previousBlock;
  }

  // 4) Senão, retorna null para indicar sem transição
  return null;
}
