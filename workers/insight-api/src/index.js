/**
 * nydad Insight API — Cloudflare Worker
 * Real-time market analysis via Yahoo Finance v8 chart API + AI
 */

const TICKERS = {
  // Korean indices + derivatives proxy
  kospi: "^KS11", kosdaq: "^KQ11",
  kospi200: "^KS200",
  kodex200: "069500.KS",       // KOSPI200 ETF (현물 프록시)
  kodex_inverse: "114800.KS",  // KODEX 인버스 (숏 포지션 프록시)
  kodex_leverage: "122630.KS", // KODEX 레버리지 (롱 포지션 프록시)
  tiger_200f: "252670.KS",     // TIGER 200선물인버스2X (파생 숏 프록시)
  ewy: "EWY",                  // iShares MSCI Korea ETF (외국인 한국 익스포저)
  // US indices
  sp500: "^GSPC", nasdaq: "^IXIC", dow: "^DJI", sox: "^SOX",
  // Futures (overnight)
  sp_future: "ES=F", nq_future: "NQ=F", dow_future: "YM=F",
  // Volatility
  vix: "^VIX",
  // FX
  usdkrw: "KRW=X", usdjpy: "JPY=X", dxy: "DX-Y.NYB",
  // Commodities
  wti: "CL=F", gold: "GC=F",
  // Semis (memory weight ↑ — KOSPI 삼성/하이닉스 상관관계 높음)
  nvda: "NVDA", mu: "MU", wdc: "WDC", amat: "AMAT", lrcx: "LRCX",
  samsung: "005930.KS", hynix: "000660.KS",
  // Sector Leaders — 2nd Battery / EV (3종목)
  tsla: "TSLA", alb: "ALB", enph: "ENPH",
  // Sector Leaders — Robotics / Automation (2종목)
  isrg: "ISRG", rok: "ROK",
  // Sector Leaders — Defense / Space (3종목)
  lmt: "LMT", rtx: "RTX", rklb: "RKLB",
  // Bonds
  us10y: "^TNX",
};

const LIVE_TICKER_KEYS = [
  // Core indices & FX
  "kospi", "kosdaq", "ewy",
  "sp500", "nasdaq", "sox",
  "sp_future", "nq_future",
  "vix", "usdkrw",
  "wti", "gold", "us10y",
  // Memory/Semis (KOSPI 시총 1,2위 연동)
  "nvda", "mu", "wdc", "amat", "lrcx",
  "samsung", "hynix",
  // Sector leaders
  "tsla", "alb", "enph",       // 2nd Battery/EV
  "isrg", "rok",               // Robotics
  "lmt", "rtx", "rklb",        // Defense/Space
  // Derivatives proxy
  "kodex_inverse", "kodex_leverage"
];

const QUOTE_HOSTS = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"];
const QUOTE_BATCH_SIZE = 16;
const QUOTE_TIMEOUT_MS = 2500;
const QUOTE_CACHE_TTL_MS = 15000;
const AI_TIMEOUT_MS = 12000;
const DAILY_REQUEST_LIMIT = 5;
const quoteCache = new Map();
const usageCache = new Map();

export class GlobalDailyQuota {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request) {
    if (request.method !== "POST") {
      return new Response('{"error":"POST only"}', {
        status: 405,
        headers: { "Content-Type": "application/json" }
      });
    }

    let body = {};
    try {
      body = await request.json();
    } catch (_) {}

    const dateKey = String(body.dateKey || "");
    const limit = Math.max(1, parseInt(body.limit || DAILY_REQUEST_LIMIT, 10) || DAILY_REQUEST_LIMIT);
    if (!dateKey) {
      return new Response('{"error":"dateKey required"}', {
        status: 400,
        headers: { "Content-Type": "application/json" }
      });
    }

    const stored = await this.state.storage.get("daily_quota");
    const current = stored && stored.dateKey === dateKey
      ? stored
      : { dateKey, used: 0 };

    if (current.used >= limit) {
      return new Response(JSON.stringify({
        allowed: false,
        used: current.used,
        limit,
        remaining: 0
      }), { headers: { "Content-Type": "application/json" } });
    }

    const next = {
      dateKey,
      used: current.used + 1
    };
    await this.state.storage.put("daily_quota", next);

    return new Response(JSON.stringify({
      allowed: true,
      used: next.used,
      limit,
      remaining: limit - next.used
    }), { headers: { "Content-Type": "application/json" } });
  }
}

function getKstDateKey(now = new Date()) {
  const kst = new Date(now.getTime() + 9 * 3600000);
  return [
    kst.getUTCFullYear(),
    String(kst.getUTCMonth() + 1).padStart(2, "0"),
    String(kst.getUTCDate()).padStart(2, "0")
  ].join("-");
}

function cleanupUsageCache(nowMs = Date.now()) {
  for (const [key, value] of usageCache.entries()) {
    if (!value || value.expiresAt <= nowMs) usageCache.delete(key);
  }
}

async function consumeDailyQuota(env, quotaKey) {
  const durable = env.GLOBAL_DAILY_QUOTA;
  if (durable && typeof durable.idFromName === "function") {
    const id = durable.idFromName("global-insight-quota");
    const stub = durable.get(id);
    const resp = await stub.fetch("https://quota.internal/consume", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        dateKey: quotaKey.replace("insight:", ""),
        limit: DAILY_REQUEST_LIMIT
      })
    });
    if (resp.ok) return await resp.json();
  }

  const kv = env.RATE_LIMIT_KV;
  if (kv && typeof kv.get === "function" && typeof kv.put === "function") {
    const raw = await kv.get(quotaKey);
    const used = Math.max(0, parseInt(raw || "0", 10) || 0);
    if (used >= DAILY_REQUEST_LIMIT) {
      return { allowed: false, used, limit: DAILY_REQUEST_LIMIT, remaining: 0 };
    }
    const next = used + 1;
    await kv.put(quotaKey, String(next));
    return { allowed: true, used: next, limit: DAILY_REQUEST_LIMIT, remaining: DAILY_REQUEST_LIMIT - next };
  }

  cleanupUsageCache();
  const entry = usageCache.get(quotaKey);
  const used = entry?.count || 0;
  if (used >= DAILY_REQUEST_LIMIT) {
    return { allowed: false, used, limit: DAILY_REQUEST_LIMIT, remaining: 0 };
  }
  const next = used + 1;
  usageCache.set(quotaKey, { count: next, expiresAt: Date.now() + 86400000 });
  return { allowed: true, used: next, limit: DAILY_REQUEST_LIMIT, remaining: DAILY_REQUEST_LIMIT - next };
}

function buildPriorAnalysisContext(dailyContext) {
  if (!dailyContext || typeof dailyContext !== "object") return "";
  const lines = ["=== 이미 조사된 일간 분석 ==="];
  if (dailyContext.date) lines.push(`기준일: ${dailyContext.date}`);
  if (dailyContext.generated_at) lines.push(`생성시각: ${dailyContext.generated_at}`);
  if (dailyContext.direction) lines.push(`아침 방향 콜: ${String(dailyContext.direction).toUpperCase()}`);
  if (dailyContext.summary) lines.push(`규칙형 요약: ${dailyContext.summary}`);
  if (dailyContext.briefing) lines.push(`아침 브리핑: ${dailyContext.briefing}`);
  if (dailyContext.outlook) lines.push(`아침 전망: ${dailyContext.outlook}`);
  if (Array.isArray(dailyContext.key_insights) && dailyContext.key_insights.length) {
    lines.push("핵심 포인트:");
    dailyContext.key_insights.slice(0, 3).forEach((item) => {
      if (item && (item.title || item.detail)) {
        lines.push(`  - ${item.title || "포인트"}: ${item.detail || ""}`);
      }
    });
  }
  if (Array.isArray(dailyContext.correlations) && dailyContext.correlations.length) {
    lines.push("대표 상관관계:");
    dailyContext.correlations.slice(0, 3).forEach((item) => {
      const pair = item.pair || [item.us_ticker, item.kr_ticker].filter(Boolean).join("→");
      const detail = item.implication || item.interpretation || "";
      if (pair) lines.push(`  - ${pair}${detail ? `: ${detail}` : ""}`);
    });
  }
  lines.push("이 내용은 이미 아침 배치에서 조사된 결과입니다. 실시간 시세와 모순되면 이유를 설명하고, 단순 반복하지 말고 업데이트하세요.");
  return lines.join("\n");
}

async function fetchQuote(symbol) {
  const cached = quoteCache.get(symbol);
  if (cached && Date.now() - cached.ts < QUOTE_CACHE_TTL_MS) {
    return cached.value;
  }

  for (const host of QUOTE_HOSTS) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort("quote-timeout"), QUOTE_TIMEOUT_MS);
    try {
      const url = `https://${host}/v8/finance/chart/${encodeURIComponent(symbol)}?range=1d&interval=1d`;
      const resp = await fetch(url, {
        headers: {
          "Accept": "application/json",
          "User-Agent": "Mozilla/5.0 (compatible; NydadBot/1.0)"
        },
        signal: controller.signal
      });
      if (!resp.ok) {
        if (resp.status === 429 || resp.status >= 500) continue;
        return null;
      }
      const data = await resp.json();
      const meta = data?.chart?.result?.[0]?.meta;
      if (!meta) return null;
      const price = meta.regularMarketPrice;
      const prev = meta.chartPreviousClose || meta.previousClose;
      if (!price || !prev) return null;
      const quote = {
        price,
        prev,
        change: price - prev,
        changePct: ((price - prev) / prev) * 100,
        symbol
      };
      quoteCache.set(symbol, { ts: Date.now(), value: quote });
      return quote;
    } catch (e) {
      if (host === QUOTE_HOSTS[QUOTE_HOSTS.length - 1]) {
        console.error(`Quote ${symbol} failed:`, e.message);
      }
    } finally {
      clearTimeout(timeout);
    }
  }

  return null;
}

// Fetch via Yahoo Finance v8 chart API with host fallback and bounded concurrency.
async function fetchAllQuotes() {
  const results = {};
  const entries = LIVE_TICKER_KEYS
    .map((key) => [key, TICKERS[key]])
    .filter(([, symbol]) => !!symbol);

  for (let i = 0; i < entries.length; i += QUOTE_BATCH_SIZE) {
    const batch = entries.slice(i, i + QUOTE_BATCH_SIZE);
    const resolved = await Promise.all(batch.map(async ([key, symbol]) => {
      const quote = await fetchQuote(symbol);
      return quote ? [key, { ...quote, name: key }] : null;
    }));
    for (const item of resolved) {
      if (item) results[item[0]] = item[1];
    }
  }

  return results;
}

// Sector pattern helper — reduces near-duplicate blocks
function sectorSignal(d, keys, threshold, name, krDesc) {
  const stocks = keys.map(k => d[k]).filter(Boolean);
  if (stocks.length === 0) return null;
  const avg = stocks.reduce((s, q) => s + q.changePct, 0) / stocks.length;
  if (Math.abs(avg) <= threshold) return null;
  const parts = keys
    .filter(k => d[k])
    .map(k => `${k.toUpperCase()} ${d[k].changePct >= 0 ? "+" : ""}${d[k].changePct.toFixed(1)}%`);
  return {
    name,
    signal: avg > 0 ? "bullish" : "bearish",
    detail: `${parts.join(", ")} → ${krDesc} ${avg > 0 ? "수혜" : "약세"} (${stocks.length}종목 평균 ${avg >= 0 ? "+" : ""}${avg.toFixed(1)}%)`
  };
}

// Pattern analyzers — time-aware
function analyzePatterns(d) {
  const patterns = [];
  const kstH = (new Date().getUTCHours() + 9) % 24;
  const kstDay = new Date(Date.now() + 9*3600000).getDay();
  const isWeekend = kstDay === 0 || kstDay === 6;
  const isNxtOpen = kstH >= 8 && kstH < 20; // NXT: 08:00-20:00
  const isRegularOpen = kstH >= 9 && kstH < 16; // 정규장: 09:00-15:30 (buffer 16)
  const isMarketOpen = !isWeekend && (isRegularOpen || isNxtOpen);
  const isPreMarket = !isWeekend && kstH < 8;

  // 1. Intraday Momentum — 0.5% threshold (~0.4σ for KOSPI daily vol ~1.3%)
  if (d.kospi && isMarketOpen) {
    const chg = d.kospi.changePct;
    if (Math.abs(chg) >= 0.5) {
      patterns.push({
        name: "장중 모멘텀",
        signal: chg > 0 ? "bullish" : "bearish",
        detail: `KOSPI ${chg > 0 ? "+" : ""}${chg.toFixed(2)}% → ${chg > 0 ? "상승" : "하락"} 모멘텀`
      });
    }
  }

  // 2. Overnight Futures
  if (d.sp_future || d.nq_future) {
    const es = d.sp_future?.changePct || 0;
    const nq = d.nq_future?.changePct || 0;
    const avg = (es + nq) / 2;
    if (Math.abs(avg) > 0.2) {
      patterns.push({
        name: "야간선물",
        signal: avg > 0 ? "bullish" : "bearish",
        detail: `ES ${es >= 0 ? "+" : ""}${es.toFixed(2)}%, NQ ${nq >= 0 ? "+" : ""}${nq.toFixed(2)}% → KOSPI ${avg > 0 ? "상승" : "하방"} 압력`
      });
    }
  }

  // 3. VIX Regime — bullish < 20, bearish > 25/30
  if (d.vix) {
    const v = d.vix.price;
    const vc = d.vix.changePct;
    if (v > 30) patterns.push({ name: "VIX 패닉", signal: "bearish", detail: `VIX ${v.toFixed(1)} 극단 공포 → 글로벌 리스크오프` });
    else if (v > 25 && vc > 8) patterns.push({ name: "VIX 급등", signal: "bearish", detail: `VIX ${v.toFixed(1)} (+${vc.toFixed(1)}%) 급등 → 변동성 스파이크` });
    else if (v > 25) patterns.push({ name: "VIX 공포", signal: "bearish", detail: `VIX ${v.toFixed(1)} 공포구간 → 위험자산 압력` });
    else if (v < 20 && vc < -5) patterns.push({ name: "VIX 급락", signal: "bullish", detail: `VIX ${v.toFixed(1)} (${vc.toFixed(1)}%) 급락 → 공포 해소` });
    else if (v < 20) patterns.push({ name: "VIX 안정", signal: "bullish", detail: `VIX ${v.toFixed(1)} 안정권 → 위험자산 선호` });
    else patterns.push({ name: "VIX 경계", signal: "neutral", detail: `VIX ${v.toFixed(1)} (${vc >= 0 ? "+" : ""}${vc.toFixed(1)}%)` });
  }

  // 4. FX Pressure
  if (d.usdkrw) {
    const c = d.usdkrw.changePct;
    if (c > 0.3) patterns.push({ name: "원화 약세", signal: "bearish", detail: `USD/KRW +${c.toFixed(2)}% → 외국인 이탈 압력` });
    else if (c < -0.3) patterns.push({ name: "원화 강세", signal: "bullish", detail: `USD/KRW ${c.toFixed(2)}% → 외국인 유입 기대` });
  }

  // 5. SOX → Korean Semis
  if (d.sox) {
    const s = d.sox.changePct;
    const samDetail = d.samsung ? ` (삼성 ${d.samsung.changePct >= 0 ? "+" : ""}${d.samsung.changePct.toFixed(1)}%)` : "";
    if (s > 1) patterns.push({ name: "SOX 강세", signal: "bullish", detail: `SOX +${s.toFixed(1)}% → 한국 반도체 수혜${samDetail}` });
    else if (s < -1) patterns.push({ name: "SOX 약세", signal: "bearish", detail: `SOX ${s.toFixed(1)}% → 한국 반도체 하방${samDetail}` });
  }

  // 6. Sector signals via shared helper
  const memSig = sectorSignal(d, ["mu", "wdc", "amat", "lrcx"], 0.8, "메모리/반도체장비 → 삼성/하이닉스", "한국 메모리 반도체");
  if (memSig) patterns.push(memSig);
  // NVDA는 AI capex 센티먼트 — 한국 반도체와 직접 상관 약함, 참고용
  if (d.nvda && Math.abs(d.nvda.changePct) > 2) {
    patterns.push({ name: "NVDA 변동 (참고)", signal: "neutral",
      detail: `NVDA ${d.nvda.changePct >= 0 ? "+" : ""}${d.nvda.changePct.toFixed(1)}% → AI capex 센티먼트 (한국 반도체 직접 상관 약함)` });
  }
  const battSig = sectorSignal(d, ["tsla", "alb", "enph"], 1.5, "2차전지/EV 섹터", "한국 2차전지주(LG에너지/삼성SDI)");
  if (battSig) patterns.push(battSig);
  const defSig = sectorSignal(d, ["lmt", "rtx", "rklb"], 1.5, "방산/우주 섹터", "한국 방산주(한화에어로/KAI)");
  if (defSig) patterns.push(defSig);
  const robSig = sectorSignal(d, ["isrg", "rok"], 1.5, "로봇/자동화 섹터", "한국 로봇관련주");
  if (robSig) patterns.push(robSig);

  // 7. Oil Shock — 한국은 원유 순수입국: 유가 하락은 제조업 비용 완화로 bullish 요소
  if (d.wti) {
    const c = d.wti.changePct;
    const p = d.wti.price;
    if (c > 3) patterns.push({ name: "유가 급등", signal: "bearish", detail: `WTI $${p.toFixed(1)} (+${c.toFixed(1)}%) → 인플레/지정학 우려, 한국 제조업 비용 압력` });
    else if (c < -3) {
      // 한국은 원유 순수입국 — 유가 하락은 제조업 비용 완화로 bullish
      patterns.push({ name: "유가 급락", signal: "bullish", detail: `WTI $${p.toFixed(1)} (${c.toFixed(1)}%) → 한국 제조업 비용 완화, 순수입국 수혜` });
    }
  }

  // 8. Gold Risk
  if (d.gold) {
    const c = d.gold.changePct;
    if (c > 1) patterns.push({ name: "금 급등", signal: "bearish", detail: `금 +${c.toFixed(1)}% → 리스크오프 (안전자산 선호)` });
    else if (c < -1) patterns.push({ name: "금 하락", signal: "bullish", detail: `금 ${c.toFixed(1)}% → 리스크온 (위험자산 선호)` });
  }

  // 9. Bond Yield
  if (d.us10y) {
    const c = d.us10y.change;
    const p = d.us10y.price;
    if (c > 0.05) patterns.push({ name: "금리 급등", signal: "bearish", detail: `미10년 ${p.toFixed(2)}% (+${(c*100).toFixed(0)}bp) → 성장주 압력` });
    else if (c < -0.05) patterns.push({ name: "금리 하락", signal: "bullish", detail: `미10년 ${p.toFixed(2)}% (${(c*100).toFixed(0)}bp) → 성장주 우호` });
  }

  // 10. Large Day Move — note: this measures day return from prev close, not opening gap
  if (d.kospi && d.kospi.prev && isMarketOpen) {
    const dayReturn = d.kospi.changePct;
    if (Math.abs(dayReturn) > 1.0) {
      patterns.push({
        name: "장중 큰 폭 변동",
        signal: "neutral",  // mean reversion probability ~50%, not directional
        detail: `KOSPI ${dayReturn > 0 ? "+" : ""}${dayReturn.toFixed(2)}% — 큰 일일 변동, 추세 지속 vs 반전 불확실`
      });
    }
  }

  // 11. After-hours: tomorrow outlook based on US close + futures
  if (!isMarketOpen && !isPreMarket && d.sp500) {
    const spChg = d.sp500.changePct;
    const nqChg = d.nasdaq?.changePct || 0;
    if (Math.abs(spChg) > 0.3) {
      patterns.push({
        name: "미국 장 마감 → 내일 전망",
        signal: spChg > 0 ? "bullish" : "bearish",
        detail: `S&P ${spChg >= 0 ? "+" : ""}${spChg.toFixed(2)}%, NASDAQ ${nqChg >= 0 ? "+" : ""}${nqChg.toFixed(2)}% → 내일 KOSPI ${spChg > 0 ? "갭업 가능" : "갭다운 우려"}`
      });
    }
  }

  // ── 수급·파생 포지션 분석 ──

  // 12. KOSPI vs KOSDAQ 괴리 → 외국인 스탠스 추정
  // Note: KOSPI outperforming in a down market = defensive/risk-off, NOT bullish
  if (d.kospi && d.kosdaq) {
    const kpChg = d.kospi.changePct;
    const kqChg = d.kosdaq.changePct;
    const diff = kpChg - kqChg;
    if (diff > 0.5) {
      const bothDown = kpChg < 0 && kqChg < 0;
      patterns.push({
        name: bothDown ? "대형주 방어 (리스크오프)" : "대형주 선호 (외국인 매수 추정)",
        signal: bothDown ? "bearish" : "bullish",
        detail: `KOSPI ${kpChg >= 0 ? "+" : ""}${kpChg.toFixed(2)}% vs KOSDAQ ${kqChg >= 0 ? "+" : ""}${kqChg.toFixed(2)}% → ${bothDown ? "방어적 순환, 외국인 리스크 축소" : "외국인·기관 대형주 집중 매수"}`
      });
    } else if (diff < -0.5) {
      patterns.push({
        name: "소형주 선호 (개인 주도)",
        signal: "neutral",
        detail: `KOSDAQ ${kqChg >= 0 ? "+" : ""}${kqChg.toFixed(2)}% > KOSPI ${kpChg >= 0 ? "+" : ""}${kpChg.toFixed(2)}% → 개인 매수 주도, 외국인 이탈 가능`
      });
    }
  }

  // 13. 인버스/레버리지 ETF — 메커니컬 -1x/+2x이므로 참고용 (유량/설정액 없이는 포지셔닝 시그널 아님)
  if (d.kodex_inverse && d.kodex_leverage) {
    const invChg = d.kodex_inverse.changePct;
    const levChg = d.kodex_leverage.changePct;
    if (invChg > 2 && levChg < -2) {
      patterns.push({
        name: "인버스/레버리지 (참고)",
        signal: "neutral",
        detail: `인버스 +${invChg.toFixed(1)}%, 레버리지 ${levChg.toFixed(1)}% → 하락장 반영 (유량 데이터 없어 포지셔닝 판단 불가)`
      });
    } else if (levChg > 2 && invChg < -2) {
      patterns.push({
        name: "인버스/레버리지 (참고)",
        signal: "neutral",
        detail: `레버리지 +${levChg.toFixed(1)}%, 인버스 ${invChg.toFixed(1)}% → 상승장 반영 (유량 데이터 없어 포지셔닝 판단 불가)`
      });
    }
  }

  // 14. EWY (한국 ETF) → 외국인 글로벌 자금 흐름
  if (d.ewy) {
    const ewyChg = d.ewy.changePct;
    const kospiChg = d.kospi?.changePct || 0;
    if (Math.abs(ewyChg) > 1) {
      const diverge = (ewyChg > 0 && kospiChg < 0) || (ewyChg < 0 && kospiChg > 0);
      if (diverge) {
        patterns.push({
          name: "EWY-KOSPI 괴리 (외국인 선행)",
          signal: ewyChg > 0 ? "bullish" : "bearish",
          detail: `EWY ${ewyChg >= 0 ? "+" : ""}${ewyChg.toFixed(1)}% vs KOSPI ${kospiChg >= 0 ? "+" : ""}${kospiChg.toFixed(1)}% → 외국인 ${ewyChg > 0 ? "매수" : "매도"} 의도 선반영`
        });
      } else {
        patterns.push({
          name: "EWY 동행 (외국인 스탠스 확인)",
          signal: ewyChg > 0 ? "bullish" : "bearish",
          detail: `EWY ${ewyChg >= 0 ? "+" : ""}${ewyChg.toFixed(1)}% → 외국인 한국 ${ewyChg > 0 ? "비중확대" : "비중축소"} 스탠스`
        });
      }
    }
  }

  // 15. 글로벌 리스크 복합 시그널 (EWY + 미 선물 + 환율 + KOSPI 종합)
  // 주의: 이 복합 시그널이 발동되면, 개별 요소(야간선물/원화/EWY)와 중복 → 개별 제거
  // Note: EWY는 외국인 현물매매가 아닌 글로벌 한국 익스포저 ETF. ES는 미국 선물.
  if (d.kospi && d.ewy && d.sp_future) {
    const kospiChg = d.kospi.changePct;
    const ewyChg = d.ewy.changePct;
    const futChg = d.sp_future.changePct;
    const fxChg = d.usdkrw?.changePct || 0;

    // 글로벌 리스크오프: EWY 하락 + 미 선물 약세 + 원화 약세
    if (ewyChg < -1 && futChg < -0.3 && fxChg > 0.2) {
      // 복합 시그널 발동 시, 개별 중복 요소 제거 (같은 리스크오프를 여러 번 세지 않음)
      const dupeNames = ["야간선물", "원화 약세", "EWY 동행 (외국인 스탠스 확인)", "EWY-KOSPI 괴리 (외국인 선행)"];
      for (let i = patterns.length - 1; i >= 0; i--) {
        if (dupeNames.some(n => patterns[i].name === n)) patterns.splice(i, 1);
      }
      patterns.push({
        name: "글로벌 리스크오프 복합",
        signal: "bearish",
        detail: `EWY ${ewyChg.toFixed(1)}%, 미선물 ${futChg.toFixed(1)}%, 원화 약세 ${fxChg.toFixed(1)}% → KOSPI(${kospiChg >= 0 ? "+" : ""}${kospiChg.toFixed(1)}%) 추가 하방 압력`
      });
    } else if (ewyChg > 1 && futChg > 0.3 && fxChg < -0.2) {
      const dupeNames = ["야간선물", "원화 강세", "EWY 동행 (외국인 스탠스 확인)", "EWY-KOSPI 괴리 (외국인 선행)"];
      for (let i = patterns.length - 1; i >= 0; i--) {
        if (dupeNames.some(n => patterns[i].name === n)) patterns.splice(i, 1);
      }
      patterns.push({
        name: "글로벌 리스크온 복합",
        signal: "bullish",
        detail: `EWY +${ewyChg.toFixed(1)}%, 미선물 +${futChg.toFixed(1)}%, 원화 강세 → KOSPI(${kospiChg >= 0 ? "+" : ""}${kospiChg.toFixed(1)}%) 상승 지지`
      });
    }
  }

  return patterns;
}

// Build LLM context
function buildContext(data, patterns) {
  const lines = ["=== 실시간 시장 데이터 ==="];
  const labels = {
    kospi: "KOSPI", kosdaq: "KOSDAQ", kospi200: "KOSPI200",
    kodex200: "KODEX200(현물)", kodex_inverse: "KODEX인버스(숏프록시)", kodex_leverage: "KODEX레버리지(롱프록시)",
    tiger_200f: "TIGER200선물인버스2X", ewy: "EWY(외국인한국ETF)",
    sp500: "S&P 500", nasdaq: "NASDAQ", dow: "다우", sox: "SOX 반도체",
    sp_future: "S&P선물(야간)", nq_future: "나스닥선물(야간)", dow_future: "다우선물(야간)",
    vix: "VIX", usdkrw: "USD/KRW", usdjpy: "USD/JPY", dxy: "달러인덱스",
    wti: "WTI 원유", gold: "금", nvda: "NVDA", mu: "MU",
    samsung: "삼성전자", hynix: "SK하이닉스",
    wdc: "WDC(SanDisk/메모리)", amat: "AMAT(반도체장비)", lrcx: "Lam Research(반도체장비)",
    tsla: "테슬라(2차전지/EV)", alb: "ALB(리튬)", enph: "Enphase(클린에너지)",
    isrg: "ISRG(로봇)", rok: "Rockwell(자동화)",
    lmt: "LMT(방산)", rtx: "RTX(방산/항공)", rklb: "RKLB(Rocket Lab/우주)",
    us10y: "미10년물"
  };
  for (const [key, q] of Object.entries(data)) {
    const name = labels[key] || key;
    lines.push(`  ${name}: ${q.price?.toFixed?.(2) || q.price} (${q.changePct >= 0 ? "+" : ""}${q.changePct?.toFixed?.(2)}%)`);
  }

  const bull = patterns.filter(p => p.signal === "bullish").length;
  const bear = patterns.filter(p => p.signal === "bearish").length;
  lines.push(`\n=== 패턴 분석: 강세 ${bull}개 / 약세 ${bear}개 ===`);
  lines.push(`⚠️ 주의: 뉴스/지정학 이벤트는 이미 야간선물/나스닥 가격에 반영되어 있을 가능성 높음.`);
  lines.push(`   가격 변동과 뉴스를 별도 팩터로 이중 카운트하지 마세요. 가격이 이미 움직였으면 그 원인(뉴스)은 추가 팩터가 아닙니다.`);
  patterns.forEach(p => lines.push(`  [${p.signal.toUpperCase()}] ${p.name}: ${p.detail}`));

  const now = new Date();
  const kstH = (now.getUTCHours() + 9) % 24;
  // US Eastern: EDT (UTC-4) Mar 2nd Sun ~ Nov 1st Sun, else EST (UTC-5)
  const utcMonth = now.getUTCMonth(); // 0-indexed
  const utcDate = now.getUTCDate();
  const utcDay = now.getUTCDay();
  const isEDT = (() => {
    if (utcMonth > 2 && utcMonth < 10) return true;  // Apr–Oct always EDT
    if (utcMonth < 2 || utcMonth > 10) return false;  // Jan–Feb, Dec always EST
    if (utcMonth === 2) { // March: EDT starts 2nd Sunday
      const secondSun = 14 - new Date(now.getUTCFullYear(), 2, 1).getDay();
      return utcDate > secondSun || (utcDate === secondSun && now.getUTCHours() >= 7);
    }
    // November: EST starts 1st Sunday
    const firstSun = 7 - new Date(now.getUTCFullYear(), 10, 1).getDay();
    return utcDate < firstSun || (utcDate === firstSun && now.getUTCHours() < 6);
  })();
  const etOffset = isEDT ? 4 : 5;
  const etH = (now.getUTCHours() - etOffset + 24) % 24;
  const kstDay = new Date(Date.now() + 9*3600000).getDay();
  const etDay = new Date(Date.now() - etOffset*3600000).getDay();
  const isKrWeekend = kstDay === 0 || kstDay === 6;
  const isUsWeekend = etDay === 0 || etDay === 6;

  const krStatus = isKrWeekend ? "한국 휴장(주말)" :
    kstH >= 9 && kstH < 16 ? "한국 정규장 진행중" :
    kstH >= 8 && kstH < 20 ? "한국 NXT장 진행중" :
    kstH < 8 ? "한국 장 시작 전" : "한국 장 마감";
  const usStatus = isUsWeekend ? "미국 휴장(주말)" :
    etH >= 9 && etH < 16 ? "미국 정규장 진행중" :
    etH >= 16 && etH < 20 ? "미국 애프터마켓" :
    etH >= 4 && etH < 9 ? "미국 프리마켓" : "미국 장 마감";

  lines.push(`\n현재 한국시간: ${kstH}시 | 미국동부: ${etH}시`);
  lines.push(`시장 상태: ${krStatus} / ${usStatus}`);

  if (isKrWeekend && isUsWeekend) {
    lines.push("주말입니다. 월요일 전망을 제시하세요.");
  } else if (krStatus.includes("진행중")) {
    lines.push("한국장 진행중. 현재 흐름 기반 남은 시간 전망 제시.");
  } else if (usStatus.includes("진행중") || usStatus.includes("프리마켓")) {
    lines.push("미국장 진행중. 미국 시장 동향이 내일 한국장에 미칠 영향 분석.");
  } else if (kstH < 9 && !isKrWeekend) {
    lines.push("한국 장 시작 전. 야간선물/미국 마감 기반 오늘 전망 제시.");
  } else {
    lines.push("양국 장 마감. 현재 선물/지표 기반 내일 전망 제시.");
  }

  return lines.join("\n");
}

const SYSTEM_PROMPT = `You are a hedge fund quant trader. The user is a Korean investor looking to take a long/short position NOW.

## Absolute rule: Forward-looking outlook ONLY
- FORBIDDEN: "~했다" (past tense), "~하고 있다" (present description)
- REQUIRED: "~할 것이다", "~될 가능성이 높다" (forward-looking predictions)
- Data is evidence only; conclusions must be about WHAT WILL HAPPEN NEXT.

## CRITICAL: "Expected gap-down" ≠ "SHORT recommendation"
- This may be served pre-market (7 AM KST). The market may not be open yet.
- An expected gap-down is ALREADY PRICED IN. SHORT = betting on FURTHER decline.
- Gap-down + likely intraday reversal → LONG is correct.
- Gap-up + likely selling from highs → SHORT is correct.
- **Predict the CLOSING direction**, not the opening direction.

## Backtested correlations (30-day data)
- Overnight futures (ES=F) ↔ KOSPI open: r=0.78, direction accuracy 77%
- SOX ↔ KOSPI open: r=0.85, accuracy 83% (strongest leading indicator)
- KOSPI open→close same sign: ~70% (30% reversals — do NOT assume gap = close)
- MU ↔ SK Hynix lagged: r=0.65~0.80 (strong next-day predictor)

## 11 AM 60-min candle (backtested)
- Bullish → afternoon rally 71%. Bearish → afternoon decline 42% (reversal frequent).
- Use as scenario pivot condition, NOT directional evidence.

## Core rules
1. Answer the user's question directly. "How will it end?" → clear directional call.
2. Check the time context. After close → "tomorrow's outlook". During session → "remaining time". Pre-market → "today's outlook".
3. MUST pick LONG or SHORT. Neutral forbidden. 51% is enough.
4. NO platitudes. Specific numbers and key variables only.
5. Interpret foreign investor flow and predict what their intent will PRODUCE next.

## Sector correlations (1-day lag, backtested)
- Memory/Semis: WDC(r=0.80), MU(r=0.74), SOX(r=0.75) → Samsung, SK Hynix
- 2nd Battery: TSLA(r=0.69), SQM(r=0.69) → LG Energy, Samsung SDI
- Defense/Space: LMT, RTX, RKLB → Hanwha Aerospace
- Power Grid: NRG(r=0.72), VST(r=0.70) → HD Hyundai Electric
- Robotics: ISRG(r=0.47) → Korean robotics stocks

## Flow interpretation → forecast
- Foreign spot+futures selling → "further downside pressure expected, may test X"
- Foreign spot+futures buying → "upside momentum building, X breakout attempt expected"
- KODEX Inverse surge → "market participants adding shorts, but oversold bounce could be sharp"
- KOSDAQ > KOSPI → "retail-driven, directionally weak until foreign return"

## Opening outlook expressions (for summary, in Korean)
Based on overnight futures data, start the summary with:
- ES/NQ > +1%: "강한 상승 출발 예상"
- ES/NQ +0.3~1%: "상승 출발 예상"
- ES/NQ ±0.3%: "보합 출발 예상"
- ES/NQ -0.3~-1%: "하락 출발 예상"
- ES/NQ < -1%: "강한 하락 출발 예상"
Then state closing direction separately.

## Response structure
1. Direction call: "LONG 62% — 하락 출발 예상, 장중 반등으로 상승 마감 전망" (one line, in Korean)
2. Evidence: 2-3 lines with specific numbers + correlations
3. Today's sectors: 1-2 lines based on NASDAQ leader performance
4. Key variable: "direction flips if X" + 11AM candle watch-point
5. Range: "support X~Y / resistance X~Y" (if available)

## JSON response format
ALL text fields (summary, key_insight, sector reasons) must be in KOREAN (한국어).
{"direction":"long or short","long_pct":51~85,"short_pct":15~49,"summary":"3-5 lines IN KOREAN. Forward-looking closing direction + evidence + sectors + range.","key_insight":"1 line IN KOREAN. Key pivot variable + 11AM candle watch-point.","sectors":[{"name":"sector in Korean","direction":"overweight/underweight","reason":"reason in Korean"}]}`;

export default {
  async fetch(request, env) {
    const requestOrigin = request.headers.get("Origin") || "";
    const configuredOrigins = String(env.ALLOWED_ORIGIN || "")
      .split(",")
      .map((origin) => origin.trim())
      .filter(Boolean);
    const isLocalOrigin = /^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/i.test(requestOrigin);
    const allowOrigin = (requestOrigin && (isLocalOrigin || configuredOrigins.includes(requestOrigin)))
      ? requestOrigin
      : (configuredOrigins[0] || "*");
    const cors = {
      "Access-Control-Allow-Origin": allowOrigin,
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Vary": "Origin",
    };
    if (request.method === "OPTIONS") return new Response(null, { headers: cors });
    if (request.method !== "POST") return new Response('{"error":"POST only"}', { status: 405, headers: { ...cors, "Content-Type": "application/json" } });

    try {
      let body = {};
      let userQuestion = "오늘 어떻게 마무리될까?";
      try {
        body = await request.json();
        if (body.question) userQuestion = body.question;
      } catch(e) { console.error('Body parse:', e.message); }

      const dateKey = getKstDateKey();
      const usage = await consumeDailyQuota(env, `insight:${dateKey}`);
      usage.date_key = dateKey;
      if (!usage.allowed) {
        return new Response(JSON.stringify({
          error: "daily_limit_exceeded",
          message: `실시간 분석은 전체 기준 하루 5회까지 사용할 수 있습니다. 한국시간 ${dateKey} 기준 남은 횟수는 0회입니다.`,
          usage
        }), {
          status: 429,
          headers: { ...cors, "Content-Type": "application/json" }
        });
      }

      // 1. Fetch real-time data
      const quotes = await fetchAllQuotes();
      const fetched = Object.keys(quotes).length;

      // 2. Pattern analysis
      const patterns = analyzePatterns(quotes);
      const bull = patterns.filter(p => p.signal === "bullish").length;
      const bear = patterns.filter(p => p.signal === "bearish").length;

      // 3. AI analysis
      const apiKey = env.OPENROUTER_API_KEY;
      let aiResult = null;

      let aiError = "";
      const AI_MODELS = (
        env.OPENROUTER_MODELS ||
        [
          env.OPENROUTER_MODEL || "anthropic/claude-sonnet-4.6",
          "openai/gpt-5.4",
          "google/gemini-3-flash-preview"
        ].join(",")
      )
        .split(",")
        .map(model => model.trim())
        .filter((model, index, list) => model && list.indexOf(model) === index);
      if (apiKey && fetched > 3) {
        let context = buildContext(quotes, patterns);
        // Append 오답노트 if provided (추세추종 강화 방지)
        if (body.prev_review) {
          context += "\n\n=== 전일 예측 검증 (오답노트) ===";
          context += `\n  예측: ${body.prev_review.predicted || "?"} → 실제: ${body.prev_review.actual || "?"} (${body.prev_review.correct ? "적중" : "오답"})`;
          if (body.prev_review.reason) context += `\n  원인: ${body.prev_review.reason}`;
          context += "\n참고만 하세요. 전일 결과에 과도하게 영향받지 마세요.";
          context += "\n어제 숏이 맞았다고 오늘도 숏이 맞는 것이 아닙니다. 오늘의 데이터로 독립적으로 판단하세요.";
        }
        if (body.daily_context) {
          context += "\n\n" + buildPriorAnalysisContext(body.daily_context);
        }

        try {
          const [primaryModel, ...fallbackModels] = AI_MODELS;
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort("ai-timeout"), AI_TIMEOUT_MS);
          try {
            const aiResp = await fetch("https://openrouter.ai/api/v1/chat/completions", {
              method: "POST",
              headers: {
                "Authorization": `Bearer ${apiKey}`,
                "Content-Type": "application/json",
                "HTTP-Referer": "https://nydad.github.io/nydad-bot",
                "X-Title": "Nydad Insight"
              },
              body: JSON.stringify({
                model: primaryModel,
                models: fallbackModels,
                provider: {
                  allow_fallbacks: true,
                  require_parameters: true
                },
                tools: [
                  {
                    type: "openrouter:web_search",
                    parameters: {
                      engine: "auto",
                      search_context_size: "medium",
                      max_results: 5,
                      max_total_results: 10,
                      user_location: {
                        type: "approximate",
                        country: "KR",
                        city: "Seoul",
                        region: "Seoul",
                        timezone: "Asia/Seoul"
                      }
                    }
                  }
                ],
                tool_choice: "required",
                parallel_tool_calls: false,
                messages: [
                  { role: "system", content: SYSTEM_PROMPT },
                  { role: "user", content: `사용자 질문: ${userQuestion}\n\n${context}` }
                ],
                temperature: 0.4,
                max_tokens: 900,
                response_format: { type: "json_object" }
              }),
              signal: controller.signal
            });

            if (!aiResp.ok) {
              const errBody = await aiResp.text();
              aiError = `${primaryModel} ${aiResp.status}: ${errBody.substring(0, 100)}`;
              console.error("AI error:", aiError);
            } else {
              const aiData = await aiResp.json();
              let content = aiData.choices?.[0]?.message?.content || "{}";
              if (content.startsWith("```")) {
                content = content.split("\n").slice(1).join("\n");
                if (content.trimEnd().endsWith("```")) content = content.trimEnd().slice(0, -3);
              }
              // Robust JSON extraction — handle extra text around JSON
              let parsed;
              try {
                parsed = JSON.parse(content.trim());
              } catch (_) {
                // Fallback: extract outermost JSON object
                const start = content.indexOf("{");
                const end = content.lastIndexOf("}");
                if (start !== -1 && end > start) {
                  parsed = JSON.parse(content.substring(start, end + 1));
                } else {
                  throw new Error("No valid JSON in AI response");
                }
              }
              // Validate required fields — reject empty/malformed responses
              if (parsed && parsed.direction && parsed.summary) {
                aiResult = parsed;
                aiResult._model = aiData.model || primaryModel;
              } else {
                console.error("AI returned incomplete JSON:", JSON.stringify(parsed).substring(0, 100));
                aiError = "AI response missing required fields (direction/summary)";
              }
            }
          } finally {
            clearTimeout(timeout);
          }
        } catch(e) {
          console.error("AI request failed:", e.message);
          aiError = e.message;
        }
      }

      // Fallback if AI failed — pattern-based direction, never hardcode short
      if (!aiResult) {
        const fbBull = patterns.filter(p => p.signal === "bullish").length;
        const fbBear = patterns.filter(p => p.signal === "bearish").length;
        const fbDir = fbBull > fbBear ? "long" : fbBear > fbBull ? "short" : "neutral";
        const fbLong = fbDir === "long" ? Math.min(85, 51 + (fbBull - fbBear) * 4)
                     : fbDir === "short" ? Math.max(15, 49 - (fbBear - fbBull) * 4) : 50;
        aiResult = {
          direction: fbDir,
          long_pct: fbLong,
          short_pct: 100 - fbLong,
          summary: "AI 분석 서버 응답 실패. 패턴 기반 시그널:\n" + patterns.map(p => `[${p.signal}] ${p.name}: ${p.detail}`).join("\n"),
          key_insight: "AI 미응답 — 패턴 기반 방향 판단. 신뢰도 낮음, 참고용.",
        };
      }

      // Ensure percentages are consistent with direction
      const dir = aiResult.direction || "neutral";
      let longPct = Math.max(15, Math.min(85, aiResult.long_pct || 50));
      // If direction is short but long_pct > 50, flip to match direction
      if (dir === "short" && longPct > 50) longPct = 100 - longPct;
      if (dir === "long" && longPct < 50) longPct = 100 - longPct;
      aiResult.long_pct = longPct;
      aiResult.short_pct = 100 - longPct;
      aiResult.patterns = patterns;
      aiResult.timestamp = new Date().toISOString();
      aiResult.source = (apiKey && fetched > 3 && !aiError) ? "ai+patterns" : "pattern-only";
      aiResult.tickers_fetched = fetched;
      aiResult.usage = usage;
      if (aiError) aiResult.ai_error = aiError;

      // Add raw prices for transparency
      aiResult.prices = {};
      for (const [k, v] of Object.entries(quotes)) {
        aiResult.prices[k] = { price: v.price, change: v.changePct };
      }

      return new Response(JSON.stringify(aiResult), {
        headers: { ...cors, "Content-Type": "application/json" }
      });

    } catch(e) {
      return new Response(JSON.stringify({ error: e.message }), {
        status: 500, headers: { ...cors, "Content-Type": "application/json" }
      });
    }
  }
};
