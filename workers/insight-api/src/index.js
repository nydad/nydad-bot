/**
 * nydad Insight API — Cloudflare Worker
 * Real-time market analysis: fetches live data, discovers patterns, returns AI insight
 */

const YAHOO_TICKERS = {
  // Korean
  kospi: "^KS11", kosdaq: "^KQ11",
  // US indices
  sp500: "^GSPC", nasdaq: "^IXIC", dow: "^DJI", sox: "^SOX",
  // Futures (overnight / pre-market)
  sp_future: "ES=F", nq_future: "NQ=F", dow_future: "YM=F",
  // Volatility
  vix: "^VIX",
  // FX
  usdkrw: "KRW=X", usdjpy: "JPY=X", dxy: "DX-Y.NYB",
  // Commodities
  wti: "CL=F", gold: "GC=F",
  // Semis
  nvda: "NVDA", mu: "MU",
  // Korean semis
  samsung: "005930.KS", hynix: "000660.KS",
  // Bonds
  us10y: "^TNX",
};

// Known intraday patterns with academic/empirical backing
const KNOWN_PATTERNS = [
  {
    id: "morning_momentum",
    name: "오전 모멘텀 (9:00-10:30)",
    description: "KOSPI 첫 90분 방향이 당일 종가 방향과 일치하는 경향 (Lai et al. 2022, 약 58% 적중률)",
    check: (data) => {
      const k = data.kospi;
      if (!k) return null;
      const change = k.regularMarketChangePercent;
      const hour = new Date().getUTCHours() + 9; // KST
      if (hour < 10 || hour > 15) return null;
      if (Math.abs(change) < 0.15) return { signal: "neutral", detail: `KOSPI ${change > 0 ? "+" : ""}${change.toFixed(2)}% 보합권 — 모멘텀 미형성` };
      return {
        signal: change > 0 ? "bullish" : "bearish",
        detail: `오전 KOSPI ${change > 0 ? "+" : ""}${change.toFixed(2)}% → 장중 모멘텀 ${change > 0 ? "상승" : "하락"} 지속 확률 58%`
      };
    }
  },
  {
    id: "overnight_gap_reversal",
    name: "야간 갭 반전 패턴",
    description: "미국 시장 영향으로 형성된 시가 갭이 장중 부분 반전되는 경향 (Choe/Kwon 연구)",
    check: (data) => {
      const k = data.kospi;
      if (!k) return null;
      const open = k.regularMarketOpen;
      const prev = k.regularMarketPreviousClose;
      if (!open || !prev) return null;
      const gap = ((open - prev) / prev) * 100;
      if (Math.abs(gap) < 0.3) return null;
      return {
        signal: gap > 0 ? "bearish" : "bullish",
        detail: `시가 갭 ${gap > 0 ? "+" : ""}${gap.toFixed(2)}% → 장중 부분 반전(갭 메우기) 가능성. 갭 방향 반대 포지션 유리`
      };
    }
  },
  {
    id: "vix_regime",
    name: "VIX 레짐 시그널",
    description: "VIX > 25: 변동성 확대로 한국 시장 하방 압력. VIX < 16: 안정권으로 상승 우호",
    check: (data) => {
      const v = data.vix;
      if (!v) return null;
      const vix = v.regularMarketPrice;
      const change = v.regularMarketChangePercent;
      if (vix > 25) return { signal: "bearish", detail: `VIX ${vix.toFixed(1)} (고공포) — 글로벌 리스크오프, KOSPI 하방 압력` };
      if (vix > 20 && change > 5) return { signal: "bearish", detail: `VIX ${vix.toFixed(1)} 급등(${change > 0 ? "+" : ""}${change.toFixed(1)}%) — 변동성 스파이크, 단기 하락 가능` };
      if (vix < 16) return { signal: "bullish", detail: `VIX ${vix.toFixed(1)} 안정권 — 위험자산 선호 환경, KOSPI 상승 우호` };
      return { signal: "neutral", detail: `VIX ${vix.toFixed(1)} 경계구간` };
    }
  },
  {
    id: "overnight_futures",
    name: "야간선물 (ES/NQ) 방향성",
    description: "미국 선물이 아시아 장중 +0.3% 이상이면 KOSPI 동반 상승 확률 높음",
    check: (data) => {
      const es = data.sp_future;
      const nq = data.nq_future;
      if (!es && !nq) return null;
      const esChg = es?.regularMarketChangePercent || 0;
      const nqChg = nq?.regularMarketChangePercent || 0;
      const avg = (esChg + nqChg) / 2;
      if (avg > 0.3) return { signal: "bullish", detail: `야간선물 강세 (ES ${esChg > 0 ? "+" : ""}${esChg.toFixed(2)}%, NQ ${nqChg > 0 ? "+" : ""}${nqChg.toFixed(2)}%) → KOSPI 상승 견인` };
      if (avg < -0.3) return { signal: "bearish", detail: `야간선물 약세 (ES ${esChg > 0 ? "+" : ""}${esChg.toFixed(2)}%, NQ ${nqChg > 0 ? "+" : ""}${nqChg.toFixed(2)}%) → KOSPI 하방 압력` };
      return { signal: "neutral", detail: `야간선물 보합 (ES ${esChg > 0 ? "+" : ""}${esChg.toFixed(2)}%)` };
    }
  },
  {
    id: "fx_pressure",
    name: "환율 압력 (USD/KRW)",
    description: "원화 약세 시 외국인 이탈 → KOSPI 하락, 원화 강세 시 유입 → KOSPI 상승",
    check: (data) => {
      const fx = data.usdkrw;
      if (!fx) return null;
      const change = fx.regularMarketChangePercent;
      if (change > 0.3) return { signal: "bearish", detail: `원화 약세 (USD/KRW +${change.toFixed(2)}%) — 외국인 자금 이탈 압력, KOSPI 하방` };
      if (change < -0.3) return { signal: "bullish", detail: `원화 강세 (USD/KRW ${change.toFixed(2)}%) — 외국인 자금 유입 기대, KOSPI 상방` };
      return { signal: "neutral", detail: `환율 보합 (USD/KRW ${change > 0 ? "+" : ""}${change.toFixed(2)}%)` };
    }
  },
  {
    id: "sox_semi_spillover",
    name: "SOX → 한국 반도체 스필오버",
    description: "필라델피아 반도체(SOX) 강세 → 삼성전자/하이닉스 다음날 or 당일 상승",
    check: (data) => {
      const sox = data.sox;
      const sam = data.samsung;
      const hx = data.hynix;
      if (!sox) return null;
      const soxChg = sox.regularMarketChangePercent;
      const samChg = sam?.regularMarketChangePercent;
      const hxChg = hx?.regularMarketChangePercent;
      if (Math.abs(soxChg) < 0.5) return null;
      const krDetail = samChg != null ? ` (삼성 ${samChg > 0 ? "+" : ""}${samChg.toFixed(1)}%, 하이닉스 ${hxChg != null ? (hxChg > 0 ? "+" : "") + hxChg.toFixed(1) + "%" : "N/A"})` : "";
      if (soxChg > 1) return { signal: "bullish", detail: `SOX +${soxChg.toFixed(1)}% 강세 → 한국 반도체 수혜 기대${krDetail}` };
      if (soxChg < -1) return { signal: "bearish", detail: `SOX ${soxChg.toFixed(1)}% 약세 → 한국 반도체 하방 압력${krDetail}` };
      return { signal: soxChg > 0 ? "bullish" : "bearish", detail: `SOX ${soxChg > 0 ? "+" : ""}${soxChg.toFixed(1)}%${krDetail}` };
    }
  },
  {
    id: "oil_shock",
    name: "유가 충격 분류",
    description: "유가 +3% 이상 급등 시 지정학 리스크 or 인플레 우려 → 한국 시장 부정적",
    check: (data) => {
      const oil = data.wti;
      if (!oil) return null;
      const change = oil.regularMarketChangePercent;
      const price = oil.regularMarketPrice;
      if (change > 3) return { signal: "bearish", detail: `WTI $${price.toFixed(1)} 급등(+${change.toFixed(1)}%) — 지정학/인플레 우려, 제조업 비용 상승` };
      if (change < -3) return { signal: "bearish", detail: `WTI $${price.toFixed(1)} 급락(${change.toFixed(1)}%) — 수요 둔화 우려 시그널` };
      if (change > 1.5) return { signal: "neutral", detail: `WTI $${price.toFixed(1)} 상승(+${change.toFixed(1)}%) — 에너지주 강세, 운송 비용 부담` };
      return null;
    }
  },
  {
    id: "gold_risk_signal",
    name: "금 가격 리스크 시그널",
    description: "금 급등 = 안전자산 선호(리스크오프) → 주식 약세",
    check: (data) => {
      const gold = data.gold;
      if (!gold) return null;
      const change = gold.regularMarketChangePercent;
      if (change > 1) return { signal: "bearish", detail: `금 +${change.toFixed(1)}% 급등 — 안전자산 선호(리스크오프), 주식 약세 가능` };
      if (change < -1) return { signal: "bullish", detail: `금 ${change.toFixed(1)}% 하락 — 위험자산 선호(리스크온), 주식 강세 가능` };
      return null;
    }
  },
  {
    id: "bond_yield_signal",
    name: "미국 10년물 금리 방향",
    description: "금리 급등 → 성장주/기술주 압력, 급락 → 성장주 우호",
    check: (data) => {
      const bond = data.us10y;
      if (!bond) return null;
      const change = bond.regularMarketChange;
      const price = bond.regularMarketPrice;
      if (change > 0.05) return { signal: "bearish", detail: `미10년 ${price.toFixed(3)}% (+${(change*100).toFixed(1)}bp) 급등 — 성장주/기술주 밸류에이션 압력` };
      if (change < -0.05) return { signal: "bullish", detail: `미10년 ${price.toFixed(3)}% (${(change*100).toFixed(1)}bp) 하락 — 성장주 우호 환경` };
      return null;
    }
  },
  {
    id: "nvda_mu_lead",
    name: "NVDA/MU → 삼성/하이닉스 선행",
    description: "NVDA, MU 전일 종가 움직임이 한국 반도체 당일 방향 선행",
    check: (data) => {
      const nvda = data.nvda;
      const mu = data.mu;
      if (!nvda && !mu) return null;
      const nvdaChg = nvda?.regularMarketChangePercent || 0;
      const muChg = mu?.regularMarketChangePercent || 0;
      const avg = (nvdaChg + muChg) / 2;
      if (Math.abs(avg) < 1) return null;
      if (avg > 2) return { signal: "bullish", detail: `NVDA ${nvdaChg > 0 ? "+" : ""}${nvdaChg.toFixed(1)}%, MU ${muChg > 0 ? "+" : ""}${muChg.toFixed(1)}% 강세 → 삼성/하이닉스 상승 기대 (상관계수 0.7+)` };
      if (avg < -2) return { signal: "bearish", detail: `NVDA ${nvdaChg.toFixed(1)}%, MU ${muChg.toFixed(1)}% 약세 → 삼성/하이닉스 하방 압력` };
      return { signal: avg > 0 ? "bullish" : "bearish", detail: `NVDA ${nvdaChg > 0 ? "+" : ""}${nvdaChg.toFixed(1)}%, MU ${muChg > 0 ? "+" : ""}${muChg.toFixed(1)}%` };
    }
  }
];

// Fetch real-time quotes from Yahoo Finance v8 API
async function fetchQuotes(tickers) {
  const symbols = Object.values(tickers).join(",");
  const url = `https://query1.finance.yahoo.com/v8/finance/spark?symbols=${symbols}&range=1d&interval=1d`;

  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "NydadInsightBot/1.0" }
    });
    if (!resp.ok) throw new Error(`Yahoo ${resp.status}`);
    const data = await resp.json();
    // v8 spark doesn't have full quote data, use v6 instead
  } catch(e) { /* fallback below */ }

  // Use v6 quote endpoint for richer data
  const url2 = `https://query2.finance.yahoo.com/v6/finance/quote?symbols=${symbols}`;
  try {
    const resp = await fetch(url2, {
      headers: { "User-Agent": "NydadInsightBot/1.0" }
    });
    if (!resp.ok) {
      // Try v7 as fallback
      const url3 = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${symbols}`;
      const resp3 = await fetch(url3, { headers: { "User-Agent": "NydadInsightBot/1.0" } });
      if (!resp3.ok) throw new Error(`Yahoo v7 ${resp3.status}`);
      const d3 = await resp3.json();
      return mapQuotes(tickers, d3.quoteResponse?.result || []);
    }
    const data = await resp.json();
    return mapQuotes(tickers, data.quoteResponse?.result || []);
  } catch(e) {
    console.error("Yahoo fetch failed:", e);
    return {};
  }
}

function mapQuotes(tickers, results) {
  const bySymbol = {};
  results.forEach(r => { bySymbol[r.symbol] = r; });
  const mapped = {};
  for (const [key, symbol] of Object.entries(tickers)) {
    if (bySymbol[symbol]) mapped[key] = bySymbol[symbol];
  }
  return mapped;
}

// Run all pattern checks
function analyzePatterns(data) {
  const results = [];
  for (const pattern of KNOWN_PATTERNS) {
    try {
      const result = pattern.check(data);
      if (result) {
        results.push({ id: pattern.id, name: pattern.name, ...result });
      }
    } catch(e) { /* skip failed pattern */ }
  }
  return results;
}

// Build market summary for LLM context
function buildContext(data, patterns) {
  const lines = ["=== REAL-TIME MARKET DATA (just fetched) ==="];

  const items = [
    ["KOSPI", data.kospi], ["KOSDAQ", data.kosdaq],
    ["S&P 500", data.sp500], ["NASDAQ", data.nasdaq], ["SOX", data.sox],
    ["S&P 선물(야간)", data.sp_future], ["나스닥 선물(야간)", data.nq_future], ["다우 선물(야간)", data.dow_future],
    ["VIX", data.vix],
    ["USD/KRW", data.usdkrw], ["DXY", data.dxy],
    ["WTI", data.wti], ["Gold", data.gold],
    ["미10년", data.us10y],
    ["NVDA", data.nvda], ["MU", data.mu],
    ["삼성전자", data.samsung], ["SK하이닉스", data.hynix],
  ];

  for (const [name, q] of items) {
    if (!q) continue;
    const p = q.regularMarketPrice;
    const c = q.regularMarketChangePercent;
    const sign = c >= 0 ? "+" : "";
    lines.push(`  ${name}: ${p?.toFixed?.(2) || p} (${sign}${c?.toFixed?.(2) || c}%)`);
  }

  lines.push("\n=== PATTERN ANALYSIS (algorithm-detected) ===");
  const bull = patterns.filter(p => p.signal === "bullish").length;
  const bear = patterns.filter(p => p.signal === "bearish").length;
  lines.push(`  Bullish signals: ${bull} | Bearish signals: ${bear}`);
  for (const p of patterns) {
    lines.push(`  [${p.signal.toUpperCase()}] ${p.name}: ${p.detail}`);
  }

  const kstHour = (new Date().getUTCHours() + 9) % 24;
  lines.push(`\n현재 한국시간: ${kstHour}시`);
  if (kstHour < 9) lines.push("장 시작 전입니다. 야간선물/글로벌 지표 기반으로 오늘 전망을 제시하세요.");
  else if (kstHour < 15) lines.push("장중입니다. 현재 흐름과 패턴 기반으로 남은 시간 전망을 제시하세요.");
  else lines.push("장 마감 후입니다. 내일 전망을 제시하세요.");

  return lines.join("\n");
}

const SYSTEM_PROMPT = `당신은 헤지펀드 퀀트 애널리스트입니다. 개인 투자자가 아침에 2분 안에 읽고 방향을 잡을 수 있는 인사이트를 제공합니다.

규칙:
1. 반드시 LONG 또는 SHORT 방향을 제시하세요. 중립은 불가합니다. 51%라도 한쪽을 선택하세요.
2. "시장은 불확실하다", "주의가 필요하다" 같은 뻔한 말 절대 금지.
3. 구체적 수치와 근거를 포함하세요. 야간선물, 환율, VIX, 반도체 등 데이터를 인용하세요.
4. 패턴 분석 결과를 반영하되, 단순 나열하지 말고 종합 판단을 내리세요.
5. 상호작용 효과를 분석하세요 (예: "SOX 강세 + 원화 강세가 동시에 오면 반도체 효과 2배")

JSON으로 응답:
{
  "direction": "long" 또는 "short",
  "long_pct": 51~85 사이 정수,
  "short_pct": 15~49 사이 정수,
  "summary": "핵심 3줄 요약 (한국어, 구체적 수치 포함)",
  "key_insight": "가장 중요한 1줄 인사이트 (남들이 못 보는 것)"
}`;

export default {
  async fetch(request, env) {
    // CORS
    const corsHeaders = {
      "Access-Control-Allow-Origin": env.ALLOWED_ORIGIN || "*",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    if (request.method !== "POST") {
      return new Response(JSON.stringify({ error: "POST only" }), {
        status: 405, headers: { ...corsHeaders, "Content-Type": "application/json" }
      });
    }

    try {
      // 1. Fetch real-time market data
      const quotes = await fetchQuotes(YAHOO_TICKERS);

      // 2. Run pattern analysis
      const patterns = analyzePatterns(quotes);

      // 3. Build context for LLM
      const context = buildContext(quotes, patterns);

      // 4. Call OpenRouter for AI insight
      const apiKey = env.OPENROUTER_API_KEY;
      if (!apiKey) {
        // No API key — return pattern-only result
        const bull = patterns.filter(p => p.signal === "bullish").length;
        const bear = patterns.filter(p => p.signal === "bearish").length;
        const dir = bull > bear ? "long" : "short";
        const total = bull + bear || 1;
        const pct = Math.round((Math.max(bull, bear) / total) * 100);
        return new Response(JSON.stringify({
          direction: dir,
          long_pct: dir === "long" ? Math.max(51, pct) : 100 - Math.max(51, pct),
          short_pct: dir === "short" ? Math.max(51, pct) : 100 - Math.max(51, pct),
          summary: `패턴 분석 기반: ${bull}개 강세, ${bear}개 약세 시그널. AI 분석은 API 키 설정 후 사용 가능합니다.`,
          key_insight: patterns.length ? patterns[0].detail : "시장 데이터 수집 중",
          patterns: patterns,
          source: "pattern-only"
        }), {
          headers: { ...corsHeaders, "Content-Type": "application/json" }
        });
      }

      const aiResp = await fetch("https://openrouter.ai/api/v1/chat/completions", {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${apiKey}`,
          "Content-Type": "application/json",
          "HTTP-Referer": "https://nydad.github.io/nydad-bot",
          "X-Title": "Nydad Insight"
        },
        body: JSON.stringify({
          model: env.OPENROUTER_MODEL || "anthropic/claude-sonnet-4.6",
          messages: [
            { role: "system", content: SYSTEM_PROMPT },
            { role: "user", content: context }
          ],
          temperature: 0.3,
          max_tokens: 1000,
          response_format: { type: "json_object" }
        })
      });

      let aiResult;
      if (aiResp.ok) {
        const aiData = await aiResp.json();
        let content = aiData.choices?.[0]?.message?.content || "{}";
        if (content.startsWith("```")) {
          content = content.split("\n").slice(1).join("\n");
          if (content.endsWith("```")) content = content.slice(0, -3);
        }
        aiResult = JSON.parse(content.trim());
      } else {
        // Fallback to pattern-based
        const bull = patterns.filter(p => p.signal === "bullish").length;
        const bear = patterns.filter(p => p.signal === "bearish").length;
        aiResult = {
          direction: bull >= bear ? "long" : "short",
          long_pct: bull >= bear ? 55 + bull * 3 : 45 - bear * 3,
          short_pct: bull >= bear ? 45 - bull * 3 : 55 + bear * 3,
          summary: "AI 서버 응답 지연. 패턴 분석 기반 결과입니다.",
          key_insight: patterns[0]?.detail || "데이터 분석 중"
        };
      }

      // Ensure long_pct + short_pct = 100
      aiResult.long_pct = Math.max(15, Math.min(85, aiResult.long_pct || 50));
      aiResult.short_pct = 100 - aiResult.long_pct;
      aiResult.patterns = patterns;
      aiResult.timestamp = new Date().toISOString();
      aiResult.source = "ai+patterns";

      return new Response(JSON.stringify(aiResult), {
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      });

    } catch(e) {
      return new Response(JSON.stringify({ error: e.message, patterns: [] }), {
        status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" }
      });
    }
  }
};
