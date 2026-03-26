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
  // Semis
  nvda: "NVDA", mu: "MU",
  samsung: "005930.KS", hynix: "000660.KS",
  // Bonds
  us10y: "^TNX",
};

// Fetch via Yahoo Finance v8 chart API (still public)
async function fetchAllQuotes() {
  const results = {};
  const entries = Object.entries(TICKERS);
  await Promise.all(entries.map(async ([key, symbol]) => {
    try {
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=1d&interval=1d`;
      const resp = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0 (compatible; NydadBot/1.0)" } });
      if (!resp.ok) return;
      const data = await resp.json();
      const meta = data?.chart?.result?.[0]?.meta;
      if (!meta) return;
      const price = meta.regularMarketPrice;
      const prev = meta.chartPreviousClose || meta.previousClose;
      if (!price || !prev) return;
      results[key] = { price, prev, change: price - prev, changePct: ((price - prev) / prev) * 100, symbol, name: key };
    } catch(e) { console.error(`Ticker ${key} failed:`, e.message); }
  }));
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

  // ── 수급·파생 포지션 분석 ──

  // 12. KOSPI vs KOSDAQ 괴리 → 외국인 스탠스 추정
  if (d.kospi && d.kosdaq) {
    const kpChg = d.kospi.changePct;
    const kqChg = d.kosdaq.changePct;
    const diff = kpChg - kqChg;
    if (diff > 0.5) {
      patterns.push({
        name: "대형주 선호 (외국인 매수 추정)",
        signal: "bullish",
        detail: `KOSPI ${kpChg >= 0 ? "+" : ""}${kpChg.toFixed(2)}% vs KOSDAQ ${kqChg >= 0 ? "+" : ""}${kqChg.toFixed(2)}% → 외국인·기관 대형주 집중 매수`
      });
    } else if (diff < -0.5) {
      patterns.push({
        name: "소형주 선호 (개인 주도)",
        signal: "neutral",
        detail: `KOSDAQ ${kqChg >= 0 ? "+" : ""}${kqChg.toFixed(2)}% > KOSPI ${kpChg >= 0 ? "+" : ""}${kpChg.toFixed(2)}% → 개인 매수 주도, 외국인 이탈 가능`
      });
    }
  }

  // 13. 인버스/레버리지 ETF → 파생 포지션 방향
  if (d.kodex_inverse && d.kodex_leverage) {
    const invChg = d.kodex_inverse.changePct;
    const levChg = d.kodex_leverage.changePct;
    if (invChg > 1.5 && levChg < -1) {
      patterns.push({
        name: "파생 숏 포지션 증가",
        signal: "bearish",
        detail: `인버스 +${invChg.toFixed(1)}%, 레버리지 ${levChg.toFixed(1)}% → 시장 참여자 숏 베팅 강화`
      });
    } else if (levChg > 1.5 && invChg < -1) {
      patterns.push({
        name: "파생 롱 포지션 증가",
        signal: "bullish",
        detail: `레버리지 +${levChg.toFixed(1)}%, 인버스 ${invChg.toFixed(1)}% → 시장 참여자 롱 베팅 강화`
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

  // 15. 현물+선물 동반 분석 → 외국인 의도 추정 (종합)
  if (d.kospi && d.ewy && d.sp_future) {
    const spotChg = d.kospi.changePct;
    const ewyChg = d.ewy.changePct;
    const futChg = d.sp_future.changePct;
    const fxChg = d.usdkrw?.changePct || 0;

    // 현물(EWY) + 선물(ES) 동반 매도 = 명확한 숏 스탠스
    if (ewyChg < -1 && futChg < -0.3 && fxChg > 0.2) {
      patterns.push({
        name: "외국인 현물+선물 동반 매도",
        signal: "bearish",
        detail: `EWY ${ewyChg.toFixed(1)}% + 선물 약세 + 원화 약세 → 명확한 숏 스탠스, 한국 자금 이탈`
      });
    } else if (ewyChg > 1 && futChg > 0.3 && fxChg < -0.2) {
      patterns.push({
        name: "외국인 현물+선물 동반 매수",
        signal: "bullish",
        detail: `EWY +${ewyChg.toFixed(1)}% + 선물 강세 + 원화 강세 → 명확한 롱 스탠스, 한국 자금 유입`
      });
    }

    // 현물 매도 + 선물 매수 = 베이시스 트레이드 (방향성 중립)
    if (ewyChg < -0.5 && futChg > 0.3) {
      patterns.push({
        name: "차익거래 추정 (현물↓ 선물↑)",
        signal: "neutral",
        detail: `EWY ${ewyChg.toFixed(1)}% but 선물 강세 → 프로그램 매도/차익거래, 방향성 약함`
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

const SYSTEM_PROMPT = `당신은 헤지펀드 퀀트 트레이더입니다. 사용자는 지금 롱/숏 포지션을 잡으려는 투자자입니다.

## 절대 규칙: 선행적 전망만 말하세요
- **금지:** "~했다", "~하고 있다", "~인 상황이다" (후행적 설명)
- **필수:** "~할 것이다", "~될 가능성이 높다", "~에서 반등/하락 예상" (선행적 전망)
- 데이터는 근거로만 쓰고, 결론은 반드시 **앞으로 어떻게 될 것인지**
- 예시: ❌ "외국인이 매도하고 있다" → ✅ "외국인 숏 스탠스가 지속되면 5,400 이탈 가능성 높다. 다만 브렌트유 $100 하회 시 숏커버 유입으로 반등 트리거."

## 핵심 규칙
1. **사용자 질문에 직접 답하세요.** "어떻게 될까?" → 앞으로의 방향을 명확히.
2. **시간대 확인.** 장 마감 후면 "내일 전망", 장중이면 "남은 시간", 장 전이면 "오늘 전망".
3. 반드시 LONG 또는 SHORT 방향 제시. 중립 불가. 51%라도 한쪽 선택.
4. 뻔한 말 절대 금지. 구체적 수치와 핵심 변수를 명시.
5. 수급·파생 분석으로 외국인 의도를 해석하고, 그 의도가 **앞으로 어떤 결과를 만들지** 전망.

## 수급 해석 → 전망 연결
- 외국인 현물+선물 동반 매도 → "추가 하방 압력 예상, X원까지 하락 가능"
- 외국인 현물+선물 동반 매수 → "상승 모멘텀 확대, X원 돌파 시도 예상"
- 인버스 급등 → "시장 참여자 숏 강화, 그러나 과매도 시 반등도 빠를 것"
- KOSDAQ > KOSPI → "개인 주도 장세, 외국인 복귀 전까지 방향성 약할 것"

## 전망 구조 (이 순서로 답하세요)
1. **방향 콜:** "SHORT 68% — 단기 하방 바이어스" (한 줄)
2. **근거:** 수급/파생/글로벌 데이터 기반 2-3줄 (구체적 수치)
3. **핵심 변수:** "X 발생 시 방향 전환" (한 줄)
4. **레인지:** "지지 X~Y / 저항 X~Y" (있으면)

## JSON 응답 형식
{"direction":"long또는short","long_pct":51~85,"short_pct":15~49,"summary":"선행적 전망 3~5줄. 앞으로 어떻게 될지 + 근거 + 레인지.","key_insight":"방향 전환 핵심 변수 1줄 (예: 브렌트유 $100 하회 시 숏커버 트리거)"}`;

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
      } catch(e) { console.error('Body parse:', e.message); }

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
      const AI_MODELS = [
        env.OPENROUTER_MODEL || "anthropic/claude-sonnet-4.6",
        "openai/gpt-5.4",
        "google/gemini-3-flash-preview"
      ];
      if (apiKey && fetched > 3) {
        let context = buildContext(quotes, patterns);
        // Append 오답노트 if provided
        try {
          const body = await request.clone().json();
          if (body.prev_review) {
            context += "\n\n=== 전일 예측 검증 (오답노트) ===";
            context += `\n  예측: ${body.prev_review.predicted || "?"} → 실제: ${body.prev_review.actual || "?"} (${body.prev_review.correct ? "적중" : "오답"})`;
            if (body.prev_review.reason) context += `\n  원인: ${body.prev_review.reason}`;
            context += "\n이 오답노트를 반영하세요. 같은 실수를 반복하지 마세요.";
          }
        } catch(e) {}
        // Try models in order: Sonnet → GPT-5.4 → Gemini Flash
        for (const aiModel of AI_MODELS) {
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
                model: aiModel,
                messages: [
                  { role: "system", content: SYSTEM_PROMPT },
                  { role: "user", content: `사용자 질문: ${userQuestion}\n\n${context}` }
                ],
                temperature: 0.4,
                max_tokens: 3000,
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
              // Robust JSON extraction — handle extra text around JSON
              let parsed;
              const jsonMatch = content.match(/\{[\s\S]*\}/);
              if (jsonMatch) {
                parsed = JSON.parse(jsonMatch[0]);
              } else {
                parsed = JSON.parse(content.trim());
              }
              aiResult = parsed;
              aiResult._model = aiModel;
              break; // Success — stop trying
            } else if (aiResp.status === 403) {
              console.error(`Model ${aiModel} region-blocked, trying next...`);
              continue; // Try next model
            } else {
              const errBody = await aiResp.text();
              aiError = `${aiModel} ${aiResp.status}: ${errBody.substring(0, 100)}`;
              console.error("AI error:", aiError);
              continue;
            }
          } catch(e) {
            console.error(`Model ${aiModel} failed:`, e.message);
            aiError = e.message;
            continue;
          }
        }
      }

      // Fallback if AI failed — just show raw data, no forced direction
      if (!aiResult) {
        aiResult = {
          direction: "short",
          long_pct: 50,
          short_pct: 50,
          summary: "AI 분석 서버 응답 실패. 아래 패턴 데이터를 참고하세요:\n" + patterns.map(p => `[${p.signal}] ${p.name}: ${p.detail}`).join("\n"),
          key_insight: "AI 서버 재시도 필요 — 패턴 데이터만 표시 중",
        };
      }

      aiResult.long_pct = Math.max(15, Math.min(85, aiResult.long_pct || 50));
      aiResult.short_pct = 100 - aiResult.long_pct;
      aiResult.patterns = patterns;
      aiResult.timestamp = new Date().toISOString();
      aiResult.source = (apiKey && fetched > 3 && !aiError) ? "ai+patterns" : "pattern-only";
      aiResult.tickers_fetched = fetched;
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
