/**
 * nydad Insight API — Cloudflare Worker
 * Real-time market analysis via Yahoo Finance v8 chart API + AI
 */

const TICKERS = {
  kospi: "^KS11", kosdaq: "^KQ11",
  sp500: "^GSPC", nasdaq: "^IXIC", dow: "^DJI", sox: "^SOX",
  sp_future: "ES=F", nq_future: "NQ=F", dow_future: "YM=F",
  vix: "^VIX",
  usdkrw: "KRW=X", usdjpy: "JPY=X", dxy: "DX-Y.NYB",
  wti: "CL=F", gold: "GC=F",
  nvda: "NVDA", mu: "MU",
  samsung: "005930.KS", hynix: "000660.KS",
  us10y: "^TNX",
};

// Fetch via Yahoo Finance v8 chart API (still public)
async function fetchAllQuotes() {
  const symbols = Object.entries(TICKERS);
  const results = {};

  // Batch in groups of 5 for parallel fetch
  for (let i = 0; i < symbols.length; i += 5) {
    const batch = symbols.slice(i, i + 5);
    const promises = batch.map(async ([key, symbol]) => {
      try {
        const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=1d&interval=1d`;
        const resp = await fetch(url, {
          headers: { "User-Agent": "Mozilla/5.0 (compatible; NydadBot/1.0)" }
        });
        if (!resp.ok) return;
        const data = await resp.json();
        const meta = data?.chart?.result?.[0]?.meta;
        if (!meta) return;
        const price = meta.regularMarketPrice;
        const prev = meta.chartPreviousClose || meta.previousClose;
        if (!price || !prev) return;
        const change = price - prev;
        const changePct = ((change) / prev) * 100;
        results[key] = {
          price: price,
          prev: prev,
          change: change,
          changePct: changePct,
          symbol: symbol,
          name: key,
        };
      } catch(e) { /* skip */ }
    });
    await Promise.all(promises);
  }
  return results;
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

  // 1. Morning Momentum — only during market hours
  if (d.kospi && isMarketOpen) {
    const chg = d.kospi.changePct;
    if (Math.abs(chg) >= 0.15) {
      patterns.push({
        name: "장중 모멘텀",
        signal: chg > 0 ? "bullish" : "bearish",
        detail: `KOSPI ${chg > 0 ? "+" : ""}${chg.toFixed(2)}% → ${chg > 0 ? "상승" : "하락"} 모멘텀 지속 확률 58%`
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

  // 3. VIX Regime
  if (d.vix) {
    const v = d.vix.price;
    const vc = d.vix.changePct;
    if (v > 25) patterns.push({ name: "VIX 공포", signal: "bearish", detail: `VIX ${v.toFixed(1)} 고공포 → 글로벌 리스크오프` });
    else if (v > 20 && vc > 5) patterns.push({ name: "VIX 급등", signal: "bearish", detail: `VIX ${v.toFixed(1)} (+${vc.toFixed(1)}%) 급등 → 변동성 스파이크` });
    else if (v < 16) patterns.push({ name: "VIX 안정", signal: "bullish", detail: `VIX ${v.toFixed(1)} 안정권 → 위험자산 선호` });
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

  // 6. NVDA/MU Lead
  if (d.nvda || d.mu) {
    const nv = d.nvda?.changePct || 0;
    const mc = d.mu?.changePct || 0;
    const avg = (nv + mc) / 2;
    if (Math.abs(avg) > 1) {
      patterns.push({
        name: "NVDA/MU 선행",
        signal: avg > 0 ? "bullish" : "bearish",
        detail: `NVDA ${nv >= 0 ? "+" : ""}${nv.toFixed(1)}%, MU ${mc >= 0 ? "+" : ""}${mc.toFixed(1)}% → 삼성/하이닉스 연동`
      });
    }
  }

  // 7. Oil Shock
  if (d.wti) {
    const c = d.wti.changePct;
    const p = d.wti.price;
    if (c > 3) patterns.push({ name: "유가 급등", signal: "bearish", detail: `WTI $${p.toFixed(1)} (+${c.toFixed(1)}%) → 인플레/지정학 우려` });
    else if (c < -3) patterns.push({ name: "유가 급락", signal: "bearish", detail: `WTI $${p.toFixed(1)} (${c.toFixed(1)}%) → 수요 둔화 우려` });
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

  // 10. Gap Reversal — only during market hours
  if (d.kospi && d.kospi.prev && isMarketOpen) {
    const gap = d.kospi.changePct;
    if (Math.abs(gap) > 0.5) {
      patterns.push({
        name: "갭 반전 가능",
        signal: gap > 0 ? "bearish" : "bullish",
        detail: `KOSPI ${gap > 0 ? "+" : ""}${gap.toFixed(2)}% 갭 → 장중 부분 반전 가능성`
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

  return patterns;
}

// Build LLM context
function buildContext(data, patterns) {
  const lines = ["=== 실시간 시장 데이터 ==="];
  const labels = {
    kospi: "KOSPI", kosdaq: "KOSDAQ", sp500: "S&P 500", nasdaq: "NASDAQ", dow: "다우", sox: "SOX 반도체",
    sp_future: "S&P선물(야간)", nq_future: "나스닥선물(야간)", dow_future: "다우선물(야간)",
    vix: "VIX", usdkrw: "USD/KRW", usdjpy: "USD/JPY", dxy: "달러인덱스",
    wti: "WTI 원유", gold: "금", nvda: "NVDA", mu: "MU",
    samsung: "삼성전자", hynix: "SK하이닉스", us10y: "미10년물"
  };
  for (const [key, q] of Object.entries(data)) {
    const name = labels[key] || key;
    lines.push(`  ${name}: ${q.price?.toFixed?.(2) || q.price} (${q.changePct >= 0 ? "+" : ""}${q.changePct?.toFixed?.(2)}%)`);
  }

  const bull = patterns.filter(p => p.signal === "bullish").length;
  const bear = patterns.filter(p => p.signal === "bearish").length;
  lines.push(`\n=== 패턴 분석: 강세 ${bull}개 / 약세 ${bear}개 ===`);
  patterns.forEach(p => lines.push(`  [${p.signal.toUpperCase()}] ${p.name}: ${p.detail}`));

  const now = new Date();
  const kstH = (now.getUTCHours() + 9) % 24;
  const etH = (now.getUTCHours() - 4 + 24) % 24; // US Eastern (EDT = UTC-4)
  const kstDay = new Date(Date.now() + 9*3600000).getDay();
  const etDay = new Date(Date.now() - 4*3600000).getDay();
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

const SYSTEM_PROMPT = `당신은 헤지펀드 퀀트 애널리스트입니다. 실시간 시장 데이터를 기반으로 투자 인사이트를 제공합니다.

## 핵심 규칙
1. **사용자 질문에 직접 답하세요.** 삼성전자를 물으면 삼성전자에 대해, KOSPI를 물으면 KOSPI에 대해 답하세요.
2. **시간대를 반드시 확인하세요.** 한국 장(9:00-15:30) 마감 후면 "내일 전망"을, 장중이면 "남은 시간 전망"을, 장 전이면 "오늘 전망"을 제시하세요.
3. 반드시 LONG(매수) 또는 SHORT(매도) 방향을 제시하세요. 중립 불가. 51%라도 한쪽 선택.
4. "시장은 불확실" 같은 뻔한 말 절대 금지. 구체적 수치와 근거를 제시.
5. 패턴 분석 결과 중 현재 시간대에 유효한 것만 언급하세요. 장 마감 후에 "장중 모멘텀"이나 "갭 반전" 패턴은 무의미합니다.
6. 특정 종목 질문 시: 해당 종목의 실시간 가격 + 관련 글로벌 종목 연동 + 수급/패턴을 분석하세요.

## JSON 응답 형식
{"direction":"long또는short","long_pct":51~85,"short_pct":15~49,"summary":"핵심 3줄 한국어. 사용자 질문에 직접 답변. 구체적 수치 포함.","key_insight":"남들이 못보는 1줄 인사이트. 시간대에 맞는 내용."}`;

export default {
  async fetch(request, env) {
    const cors = {
      "Access-Control-Allow-Origin": env.ALLOWED_ORIGIN || "*",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };
    if (request.method === "OPTIONS") return new Response(null, { headers: cors });
    if (request.method !== "POST") return new Response('{"error":"POST only"}', { status: 405, headers: { ...cors, "Content-Type": "application/json" } });

    try {
      // Get user question if provided
      let userQuestion = "오늘 어떻게 마무리될까?";
      try {
        const body = await request.json();
        if (body.question) userQuestion = body.question;
      } catch(e) { /* no body or not JSON */ }

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

      if (apiKey && fetched > 3) {
        const context = buildContext(quotes, patterns);
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
              model: env.OPENROUTER_MODEL || "anthropic/claude-sonnet-4.6",
              messages: [
                { role: "system", content: SYSTEM_PROMPT },
                { role: "user", content: `사용자 질문: ${userQuestion}\n\n${context}` }
              ],
              temperature: 0.3,
              max_tokens: 800,
              response_format: { type: "json_object" }
            })
          });
          if (aiResp.ok) {
            const aiData = await aiResp.json();
            let content = aiData.choices?.[0]?.message?.content || "{}";
            if (content.startsWith("```")) {
              content = content.split("\n").slice(1).join("\n");
              if (content.trimEnd().endsWith("```")) content = content.trimEnd().slice(0, -3);
            }
            aiResult = JSON.parse(content.trim());
          }
        } catch(e) { /* AI failed, use pattern fallback */ }
      }

      // Fallback if AI failed
      if (!aiResult) {
        const dir = bull >= bear ? "long" : "short";
        const confidence = Math.max(bull, bear) / (bull + bear || 1);
        aiResult = {
          direction: dir,
          long_pct: dir === "long" ? Math.min(50 + bull * 5, 75) : Math.max(50 - bear * 5, 25),
          short_pct: dir === "short" ? Math.min(50 + bear * 5, 75) : Math.max(50 - bull * 5, 25),
          summary: patterns.map(p => p.detail).join(". ") || "시장 데이터 수집 완료.",
          key_insight: patterns.length > 0 ? patterns[0].detail : "주요 시그널 없음",
        };
      }

      aiResult.long_pct = Math.max(15, Math.min(85, aiResult.long_pct || 50));
      aiResult.short_pct = 100 - aiResult.long_pct;
      aiResult.patterns = patterns;
      aiResult.timestamp = new Date().toISOString();
      aiResult.source = apiKey && fetched > 3 ? "ai+patterns" : "pattern-only";
      aiResult.tickers_fetched = fetched;

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
