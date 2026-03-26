/**
 * nydad Daily Digest v2.0
 * 5-tab dashboard: 국내 투자 | 코인 투자 | AI 업계 | AI 코딩 | KBO
 * Paper Ledger design — connects to real JSON data from /data/
 */
(function () {
  "use strict";

  var dates = [], currentDate = "", currentTab = "invest", cache = {};
  var INSIGHT_API = "https://nydad-insight-api.nydad.workers.dev";

  // ── Korean labels ──
  var INSIGHT_KR = { bullish: "강세", bearish: "약세", neutral: "중립", alert: "주의" };
  var HL_KR = { model: "모델", tool: "도구", trend: "동향" };
  var MKT_TITLES = {
    us_indices: "미국 지수", kr_indices: "한국 지수", kr_sectors: "섹터 ETF",
    futures: "선물", volatility: "변동성", forex: "환율",
    commodities: "원자재", bonds: "채권"
  };
  var DIR_KR = { long: "LONG 롱", short: "SHORT 숏", neutral: "NEUTRAL 중립" };
  var STREAK_KR = { win: "연승", lose: "연패" };

  // ══════════════════════════════════════
  // INIT
  // ══════════════════════════════════════
  async function init() {
    setupTabs();
    setupDock();
    setupTheme();
    setupCollapse();
    // setupChat removed — insight button setup happens in renderInvest
    setupReveal();

    try {
      var r = await fetch("./data/index.json");
      if (!r.ok) throw 0;
      var d = await r.json();
      dates = d.dates || [];
      if (!dates.length) return showEmpty();
      renderDateBar();
      selectDate(dates[0]);
    } catch (e) { showEmpty(); }
  }

  // ══════════════════════════════════════
  // TAB SWITCHING
  // ══════════════════════════════════════
  function setupTabs() {
    document.querySelectorAll(".tab-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        switchTab(btn.dataset.tab);
      });
    });
  }

  function setupDock() {
    document.querySelectorAll(".dock-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        switchTab(btn.dataset.tab);
      });
    });
  }

  function switchTab(tab) {
    if (tab === currentTab) return;
    currentTab = tab;

    document.querySelectorAll(".tab-btn").forEach(function (b) {
      b.classList.toggle("active", b.dataset.tab === tab);
    });
    document.querySelectorAll(".dock-btn").forEach(function (b) {
      b.classList.toggle("active", b.dataset.tab === tab);
    });

    if (cache[currentDate]) render(cache[currentDate]);
  }

  // ══════════════════════════════════════
  // THEME
  // ══════════════════════════════════════
  function setupTheme() {
    var btn = document.getElementById("theme-toggle");
    if (!btn) return;
    // Restore saved theme
    var saved = localStorage.getItem("nydad-theme");
    if (saved) document.documentElement.setAttribute("data-theme", saved);

    btn.addEventListener("click", function () {
      var isDark = document.documentElement.getAttribute("data-theme") === "dark";
      var next = isDark ? "light" : "dark";
      document.documentElement.setAttribute("data-theme", next);
      localStorage.setItem("nydad-theme", next);
    });
  }

  // ══════════════════════════════════════
  // COLLAPSE
  // ══════════════════════════════════════
  function setupCollapse() {
    var toggle = document.getElementById("market-toggle");
    var more = document.getElementById("market-more");
    if (!toggle || !more) return;
    toggle.addEventListener("click", function () {
      var closed = more.classList.contains("closed");
      if (closed) {
        more.classList.remove("closed");
        more.style.maxHeight = more.scrollHeight + "px";
        toggle.classList.add("open");
      } else {
        more.style.maxHeight = "0";
        more.classList.add("closed");
        toggle.classList.remove("open");
      }
    });
  }

  // ══════════════════════════════════════
  // REAL-TIME INSIGHT BUTTON
  // ══════════════════════════════════════
  function setupInsightBtn() {
    var btn = document.getElementById("insight-btn");
    var result = document.getElementById("insight-result");
    var questionInput = document.getElementById("insight-question");
    if (!btn || !result) return;

    function doAnalysis() {
      var question = questionInput ? questionInput.value.trim() : "";
      btn.disabled = true;
      btn.querySelector("span").textContent = "분석 중...";
      result.classList.remove("hidden");
      result.innerHTML = '<div style="text-align:center;padding:16px;color:var(--text-3)"><div class="loading-bar" style="width:40px;margin:0 auto 8px"></div>실시간 데이터 수집 + AI 분석 중...</div>';

      fetchInsight(question);
    }

    btn.addEventListener("click", doAnalysis);
    if (questionInput) {
      questionInput.addEventListener("keypress", function(e) { if (e.key === "Enter") doAnalysis(); });
    }

    async function fetchInsight(question) {
      try {
        var resp;
        if (INSIGHT_API) {
          resp = await fetch(INSIGHT_API, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ question: question || "오늘 어떻게 마무리될까?" })
          });
        } else {
          // Fallback: use existing daily data to generate a quick summary
          var d = cache[currentDate];
          if (d) {
            var sig = d.investment_signal || d.kospi_signal || {};
            result.innerHTML = renderInsightResult({
              direction: sig.direction || "neutral",
              long_pct: sig.long_pct || 50,
              short_pct: sig.short_pct || 50,
              summary: sig.summary || sig.key_insight || "데이터 기반 분석 결과입니다.",
              key_insight: sig.key_insight || "실시간 분석을 위해 Cloudflare Worker를 설정하세요.",
              patterns: (sig.factors || []).map(function(f) { return { name: f.name, signal: f.signal, detail: f.detail }; }),
              source: "daily-data"
            });
            btn.disabled = false;
            btn.querySelector("span").textContent = "실시간 인사이트";
            return;
          }
          throw new Error("No data");
        }

        if (!resp.ok) throw new Error("API " + resp.status);
        var data = await resp.json();
        result.innerHTML = renderInsightResult(data);
      } catch (e) {
        result.innerHTML = '<div style="padding:12px;color:var(--bear);font-size:13px">분석 실패: ' + esc(e.message) + '</div>';
      }
      btn.disabled = false;
      btn.querySelector("span").textContent = "분석";
    }
  }

  function renderInsightResult(data) {
    var dir = data.direction || "neutral";
    var dirCls = dir === "long" ? "long" : "short";
    var pct = dir === "long" ? (data.long_pct || 50) : (data.short_pct || 50);

    var h = '<div class="live-card">';
    // Header bar
    h += '<div class="live-topbar ' + dirCls + '">';
    h += '<div class="live-topbar-left">';
    h += '<span class="live-pulse"></span>';
    h += '<span class="live-label">실시간 분석</span>';
    h += '<span class="live-time">' + new Date().toLocaleTimeString("ko-KR", {hour:"2-digit",minute:"2-digit"}) + '</span>';
    h += '</div>';
    h += '<div class="live-direction ' + dirCls + '">' + (dir === "long" ? "LONG" : "SHORT") + " " + pct + '%</div>';
    h += '</div>';

    // AI summary — clean sentence
    if (data.summary) {
      h += '<div class="live-summary">' + esc(data.summary) + '</div>';
    }

    // Key insight
    if (data.key_insight) {
      h += '<div class="live-highlight">' + esc(data.key_insight) + '</div>';
    }

    // Patterns as collapsible detail
    if (data.patterns && data.patterns.length) {
      h += '<details class="live-details"><summary class="live-details-toggle">패턴 상세 (' + data.patterns.length + '개 시그널)</summary>';
      h += '<div class="live-patterns">';
      data.patterns.forEach(function (p) {
        var ico = p.signal === "bullish" ? "+" : p.signal === "bearish" ? "-" : "·";
        h += '<div class="live-pattern-row ' + safeSignal(p.signal) + '">';
        h += '<span class="live-pattern-ico">' + ico + '</span>';
        h += '<span class="live-pattern-name">' + esc(p.name || "") + '</span>';
        h += '<span class="live-pattern-detail">' + esc(p.detail || "") + '</span>';
        h += '</div>';
      });
      h += '</div></details>';
    }
    h += '</div>';
    return h;
  }

  // ══════════════════════════════════════
  // SCROLL REVEAL
  // ══════════════════════════════════════
  function setupReveal() {
    if (!("IntersectionObserver" in window)) {
      document.querySelectorAll(".reveal").forEach(function (el) { el.classList.add("visible"); });
      return;
    }
    var obs = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) e.target.classList.add("visible");
      });
    }, { threshold: 0.1, rootMargin: "0px 0px -30px 0px" });
    document.querySelectorAll(".reveal").forEach(function (el) { obs.observe(el); });
  }

  function reReveal(container) {
    container.querySelectorAll(".reveal").forEach(function (el) {
      el.classList.remove("visible");
      void el.offsetWidth;
      el.classList.add("visible");
    });
  }

  // ══════════════════════════════════════
  // DATE BAR
  // ══════════════════════════════════════
  function renderDateBar() {
    var el = document.getElementById("date-scroll");
    if (!el) return;
    if (dates.length <= 1) return;
    var h = '<select class="date-select" id="date-select">';
    h += '<option value="' + dates[0] + '">오늘 (' + fmtDateShort(dates[0]) + ')</option>';
    for (var i = 1; i < dates.length; i++) {
      h += '<option value="' + dates[i] + '">' + fmtDateShort(dates[i]) + '</option>';
    }
    h += '</select>';
    el.innerHTML = h;
    var sel = document.getElementById("date-select");
    if (sel) sel.addEventListener("change", function () { if (sel.value) selectDate(sel.value); });
  }
  function fmtDateShort(d) {
    var p = d.split("-");
    var day = ["일","월","화","수","목","금","토"][new Date(+p[0], p[1] - 1, +p[2]).getDay()];
    return parseInt(p[1]) + "/" + parseInt(p[2]) + " " + day;
  }

  function selectDate(date) {
    currentDate = date;
    var p = date.split("-");
    var day = ["일","월","화","수","목","금","토"][new Date(+p[0], p[1] - 1, +p[2]).getDay()];
    // Update main date display
    var main = document.querySelector(".date-main");
    if (main) {
      var isToday = date === dates[0];
      main.textContent = p[0] + "년 " + parseInt(p[1]) + "월 " + parseInt(p[2]) + "일 " + day + "요일" + (isToday ? "" : " (지난호)");
      main.style.color = isToday ? "" : "var(--text-3)";
    }
    // Update header date
    var hd = document.querySelector(".header-date");
    if (hd) hd.textContent = p[0] + "." + p[1] + "." + p[2] + " " + day;
    loadDigest(date);
  }

  async function loadDigest(date) {
    show("loading");
    hideAllTabs();
    if (cache[date]) { try { render(cache[date]); } catch(e) { console.error("Render error:", e); showError(date); } return; }
    try {
      var r = await fetch("./data/" + date + ".json");
      if (!r.ok) throw new Error("HTTP " + r.status);
      cache[date] = await r.json();
      try { render(cache[date]); } catch(e) { console.error("Render error:", e); showError(date); }
    } catch (e) { console.error("Load error:", e); showError(date); }
  }

  // ══════════════════════════════════════
  // RENDER DISPATCHER
  // ══════════════════════════════════════
  function render(d) {
    hideAllTabs();
    hide("loading");

    var tabEl = document.getElementById("tab-" + currentTab);
    if (!tabEl) return;

    // Build content
    var fn = {
      invest: renderInvest,
      crypto: renderCrypto,
      ai_industry: renderAI,
      ai_dev: renderDev,
      kbo: renderKBO
    };
    (fn[currentTab] || renderInvest)(d, tabEl);
    tabEl.classList.remove("hidden");
    reReveal(tabEl);
  }

  function hideAllTabs() {
    document.querySelectorAll(".tab-content").forEach(function (el) { el.classList.add("hidden"); });
  }

  // ══════════════════════════════════════
  // INVEST TAB RENDERER
  // ══════════════════════════════════════
  function renderInvest(d, el) {
    var tab = (d.tabs || {}).invest || {};
    var sig = d.investment_signal || d.kospi_signal || {};
    var h = "";

    // Hero Signal Card
    var dir = sig.direction || "neutral";
    var longPct = sig.long_pct || (dir === "long" ? 62 : dir === "short" ? 38 : 50);
    var shortPct = sig.short_pct || (100 - longPct);
    var conf = sig.confidence || 0;

    h += '<div class="signal-hero ' + safeDir(dir) + ' reveal" style="margin-top:20px">';
    h += '<div class="signal-top"><div class="signal-direction-wrap">';
    h += '<div class="signal-eyebrow">오늘의 전망 · KOSPI Direction</div>';
    h += '<div class="signal-direction">' + (DIR_KR[dir] || dir) + '</div>';
    h += '<div class="signal-pct">';
    h += '<div class="signal-pct-item"><div class="signal-pct-bar"><div class="signal-pct-fill bull" style="width:' + longPct + '%"></div></div>';
    h += '<span class="signal-pct-label bull">L ' + longPct + '%</span></div>';
    h += '<div class="signal-pct-item"><div class="signal-pct-bar"><div class="signal-pct-fill bear" style="width:' + shortPct + '%"></div></div>';
    h += '<span class="signal-pct-label bear">S ' + shortPct + '%</span></div>';
    h += '</div></div>';
    h += '<div class="signal-confidence"><div class="signal-conf-label">Confidence</div>';
    h += '<div class="signal-conf-value">' + Math.round(conf * 100) + '%</div>';
    h += '<div class="signal-conf-meter"><div class="signal-conf-fill" style="width:' + Math.round(conf * 100) + '%"></div></div></div>';
    h += '</div>';

    // Summary
    if (sig.summary || tab.briefing) {
      h += '<div class="signal-summary">' + esc(sig.summary || tab.briefing) + '</div>';
    }

    // Factors
    if (sig.factors && sig.factors.length) {
      h += '<div class="signal-factors">';
      sig.factors.forEach(function (f) {
        h += '<span class="factor-tag ' + safeSignal(f.signal) + '">' + esc(f.name) + ' ' + esc(f.detail || "") + '</span>';
      });
      h += '</div>';
    }
    // Interactive Insight — question input + button
    h += '<div class="insight-btn-section" style="margin-top:16px;padding-top:14px;border-top:1px solid var(--border)">';
    h += '<div class="chat-input-wrap" style="display:flex;gap:8px;align-items:center">';
    h += '<input type="text" class="chat-input" id="insight-question" placeholder="오늘 어떻게 마무리될까?" style="flex:1">';
    h += '<button class="insight-btn" id="insight-btn">';
    h += '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>';
    h += '<span>분석</span></button>';
    h += '</div>';
    h += '<div class="insight-result hidden" id="insight-result"></div>';
    h += '</div>';
    h += '</div>';

    h += '<hr class="divider">';

    // 오답노트: Previous day signal accuracy check
    if (d.prev_signal_review) {
      var rev = d.prev_signal_review;
      h += '<div class="reveal"><div class="section-label">전일 인사이트 검증 (오답노트)</div>';
      var correct = rev.correct;
      h += '<div class="insight-card" style="border-left:3px solid var(--' + (correct ? "bull" : "bear") + ')">';
      h += '<span class="insight-tag ' + (correct ? "bullish" : "bearish") + '">' + (correct ? "적중" : "오답") + '</span>';
      h += '<div class="insight-body">';
      h += '<div class="insight-title">전일 예측: ' + esc(rev.predicted || "") + ' → 실제: ' + esc(rev.actual || "") + '</div>';
      h += '<div class="insight-detail">' + esc(rev.reason || "") + '</div>';
      h += '</div></div></div><hr class="divider">';
    }

    // Correlation Insights — show in readable format
    var corr = sig.correlations || tab.correlations || [];
    if (corr.length) {
      h += '<div class="reveal"><div class="section-label">반도체 상관관계</div><div class="insight-list">';
      corr.forEach(function (c) {
        var coef = c.coefficient || 0;
        var strength = Math.abs(coef) > 0.6 ? "강한" : Math.abs(coef) > 0.3 ? "보통" : "약한";
        var direction = coef > 0 ? "동행" : "역행";
        var cls = coef > 0 ? "bullish" : "bearish";
        var pair = c.pair || ((c.us_ticker || "") + " → " + (c.kr_ticker || ""));
        var move = c.implied_move || c.interpretation || (strength + " " + direction + " 관계");
        h += '<div class="insight-card"><span class="insight-tag ' + cls + '">' + strength + '</span>';
        h += '<div class="insight-body"><div class="insight-title">' + esc(pair) + '</div>';
        h += '<div class="insight-detail">' + esc(move) + '</div></div></div>';
      });
      h += '</div></div><hr class="divider">';
    }

    // Foreign Flow
    var flow = sig.foreign_flow || tab.foreign_flow || {};
    if (flow.net_amount) {
      var flowDir = flow.direction || (flow.net_amount > 0 ? "buy" : "sell");
      h += '<div class="reveal"><div class="flow-bar-wrap"><div class="flow-header">';
      h += '<span class="flow-title">외국인 수급</span>';
      if (flow.consecutive_days) {
        h += '<span class="flow-streak ' + flowDir + '">' + flow.consecutive_days + '일 연속 순' + (flowDir === "buy" ? "매수" : "매도") + '</span>';
      }
      h += '</div>';
      var pct = flowDir === "buy" ? Math.min(Math.max(55, 50 + Math.abs(flow.net_amount) / 100), 90) : Math.min(Math.max(10, 50 - Math.abs(flow.net_amount) / 100), 45);
      h += '<div class="flow-track"><div class="flow-fill" style="width:' + pct + '%"></div></div>';
      h += '<div class="flow-labels"><span class="flow-label buy">' + (flow.net_amount > 0 ? "+" : "") + fmtBillion(flow.net_amount) + '</span>';
      h += '<span class="flow-label sell"></span></div>';
      h += '</div></div><hr class="divider">';
    }

    // Key Insights
    if (tab.key_insights && tab.key_insights.length) {
      h += '<div class="reveal"><div class="section-label">핵심 인사이트</div><div class="insight-list">';
      tab.key_insights.forEach(function (ins) {
        var t = ins.type || "neutral";
        h += '<div class="insight-card"><span class="insight-tag ' + safeSignal(t) + '">' + (INSIGHT_KR[t] || t) + '</span>';
        h += '<div class="insight-body"><div class="insight-title">' + esc(ins.title) + '</div>';
        h += '<div class="insight-detail">' + esc(ins.detail) + '</div></div></div>';
      });
      h += '</div></div><hr class="divider">';
    }

    // Market Data
    if (d.market_data) {
      h += '<div class="reveal"><div class="section-label">시장 데이터</div>';
      var primary = ["kr_indices", "us_indices"];
      var secondary = ["futures", "volatility", "forex", "commodities", "bonds", "kr_sectors"];

      primary.forEach(function (cat) {
        var items = d.market_data[cat];
        if (items && items.length) {
          h += renderMarketSection(cat, items);
        }
      });

      h += '<button class="collapse-btn" id="market-toggle"><span>환율 · 원자재 · 채권</span>';
      h += '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="6 9 12 15 18 9"/></svg></button>';
      h += '<div class="collapsible closed" id="market-more">';
      secondary.forEach(function (cat) {
        var items = d.market_data[cat];
        if (items && items.length) {
          h += renderMarketSection(cat, items);
        }
      });
      h += '</div></div><hr class="divider">';
    }

    // News
    h += renderNewsSection(tab.articles, "투자 뉴스");

    el.innerHTML = h;
    setupCollapse();
    setupInsightBtn();
  }

  function renderMarketSection(cat, items) {
    var h = '<div class="market-section"><div class="market-section-title">' + (MKT_TITLES[cat] || cat) + '</div><div class="market-grid">';
    items.forEach(function (m) {
      var cls = m.change > 0 ? "up" : m.change < 0 ? "down" : "flat";
      var s = m.change > 0 ? "+" : "", arr = m.change > 0 ? "▲" : m.change < 0 ? "▼" : "";
      h += '<div class="market-tile"><div class="market-name">' + esc(m.name) + '</div>';
      h += '<div class="market-price">' + fmtNum(m.price) + '</div>';
      h += '<div class="market-change ' + cls + '">' + arr + ' ' + s + fmtNum(m.change) + ' (' + s + m.change_pct + '%)</div></div>';
    });
    return h + '</div></div>';
  }

  // ══════════════════════════════════════
  // CRYPTO TAB RENDERER
  // ══════════════════════════════════════
  function renderCrypto(d, el) {
    var tab = (d.tabs || {}).crypto || {};
    var h = "";

    // Ticker ribbon
    if (d.crypto_prices && d.crypto_prices.length) {
      h += '<div class="reveal" style="margin-top:16px"><div class="ticker-ribbon">';
      d.crypto_prices.forEach(function (c) {
        var cls = c.change_pct > 0 ? "up" : c.change_pct < 0 ? "down" : "";
        var sign = c.change_pct > 0 ? "+" : "";
        h += '<div class="ticker-item"><span class="ticker-symbol">' + esc(c.symbol) + '</span>';
        h += '<span class="ticker-price">$' + fmtNum(c.price) + '</span>';
        h += '<span class="ticker-pct ' + cls + '">' + sign + c.change_pct + '%</span></div>';
      });
      h += '</div></div>';
    }

    // F&G
    if (d.fear_greed && d.fear_greed.crypto) {
      var fg = d.fear_greed.crypto;
      var fgColor = fg.score >= 60 ? "var(--amber)" : fg.score <= 40 ? "var(--bear)" : "var(--text-3)";
      h += '<div class="reveal"><div class="fg-gauge">';
      h += '<div style="text-align:center"><div class="fg-score-big" style="color:' + fgColor + '">' + fg.score + '</div></div>';
      h += '<div class="fg-meta"><div class="fg-label-text">Crypto Fear & Greed</div>';
      h += '<div class="fg-rating">' + esc(fg.rating) + '</div></div></div></div>';
    }

    h += '<hr class="divider">';

    // Briefing
    if (tab.briefing) {
      h += '<div class="reveal"><div class="section-label">코인 브리핑</div>';
      h += '<div class="editorial-card"><div class="editorial-text">' + esc(tab.briefing) + '</div></div>';
      h += renderTrends(tab.trends);
      h += '</div><hr class="divider">';
    }

    // Key Events
    if (tab.key_events && tab.key_events.length) {
      h += '<div class="reveal"><div class="section-label">주요 이벤트</div><div class="insight-list">';
      tab.key_events.forEach(function (ev) {
        var t = ev.type || "neutral";
        h += '<div class="insight-card"><span class="insight-tag ' + safeSignal(t) + '">' + (INSIGHT_KR[t] || t) + '</span>';
        h += '<div class="insight-body"><div class="insight-title">' + esc(ev.title) + '</div>';
        h += '<div class="insight-detail">' + esc(ev.detail) + '</div></div></div>';
      });
      h += '</div></div><hr class="divider">';
    }

    h += renderNewsSection(tab.articles, "코인 뉴스");
    el.innerHTML = h;
  }

  // ══════════════════════════════════════
  // AI INDUSTRY TAB RENDERER
  // ══════════════════════════════════════
  function renderAI(d, el) {
    var tab = (d.tabs || {}).ai_industry || {};
    var h = "";

    if (tab.briefing) {
      h += '<div class="reveal" style="margin-top:20px"><div class="section-label">오늘의 AI 브리핑</div>';
      h += '<div class="editorial-card"><div class="editorial-text">' + esc(tab.briefing) + '</div></div>';
      h += renderTrends(tab.trends);
      h += '</div><hr class="divider">';
    }

    if (tab.quotes && tab.quotes.length) {
      h += '<div class="reveal"><div class="section-label">주요 발언</div>';
      tab.quotes.forEach(function (q) {
        h += '<div class="quote-card"><div class="quote-text">\u201C' + esc(q.quote) + '\u201D</div>';
        h += '<div class="quote-speaker">' + esc(q.speaker) + '</div>';
        h += '<div class="quote-context">' + esc(q.context) + '</div></div>';
      });
      h += '</div><hr class="divider">';
    }

    h += renderNewsSection(tab.articles, "AI 업계 뉴스");
    el.innerHTML = h;
  }

  // ══════════════════════════════════════
  // AI DEV TAB RENDERER
  // ══════════════════════════════════════
  function renderDev(d, el) {
    var tab = (d.tabs || {}).ai_dev || {};
    var h = "";

    if (tab.briefing) {
      h += '<div class="reveal" style="margin-top:20px"><div class="section-label">개발자 브리핑</div>';
      h += '<div class="editorial-card"><div class="editorial-text">' + esc(tab.briefing) + '</div></div>';
      h += renderTrends(tab.trends);
      h += '</div><hr class="divider">';
    }

    if (tab.highlights && tab.highlights.length) {
      h += '<div class="reveal"><div class="section-label">주요 업데이트</div><div class="highlight-list">';
      tab.highlights.forEach(function (hl) {
        var t = hl.type || "trend";
        h += '<div class="highlight-item"><span class="hl-type ' + safeHL(t) + '">' + (HL_KR[t] || t) + '</span>';
        h += '<div class="hl-body"><div class="hl-title">' + esc(hl.title) + '</div>';
        h += '<div class="hl-detail">' + esc(hl.detail) + '</div></div></div>';
      });
      h += '</div></div><hr class="divider">';
    }

    h += renderNewsSection(tab.articles, "AI 코딩 뉴스");
    el.innerHTML = h;
  }

  // ══════════════════════════════════════
  // KBO TAB RENDERER
  // ══════════════════════════════════════
  function renderKBO(d, el) {
    var tab = (d.tabs || {}).kbo || {};
    var h = "";

    // Standings
    if (tab.standings && tab.standings.length) {
      h += '<div class="reveal" style="margin-top:20px"><div class="section-label">KBO 순위</div>';
      h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);overflow:hidden">';
      h += '<table class="standings-table"><thead><tr>';
      h += '<th></th><th>팀</th><th>승</th><th>패</th><th>무</th><th>승률</th><th>차</th><th>연속</th></tr></thead><tbody>';
      tab.standings.forEach(function (t, i) {
        var streakCls = (t.streak || "").includes("승") ? "win" : "lose";
        h += '<tr><td><span class="standings-rank">' + (i + 1) + '</span></td>';
        h += '<td class="standings-team">' + esc(t.team) + '</td>';
        h += '<td>' + t.wins + '</td><td>' + t.losses + '</td><td>' + (t.draws || 0) + '</td>';
        h += '<td class="standings-pct">' + (t.win_pct || ".000") + '</td>';
        h += '<td>' + (t.games_behind || "—") + '</td>';
        h += '<td><span class="standings-streak ' + streakCls + '">' + esc(t.streak || "") + '</span></td></tr>';
      });
      h += '</tbody></table></div></div>';
      h += '<hr class="divider">';
    }

    // Today's Games
    if (tab.games_today && tab.games_today.length) {
      h += '<div class="reveal"><div class="section-label">오늘의 경기</div><div class="game-tickets">';
      tab.games_today.forEach(function (g) {
        var homeWin = g.home_score > g.away_score;
        var awayWin = g.away_score > g.home_score;
        h += '<div class="game-ticket"><div class="game-ticket-teams">';
        h += '<div class="game-ticket-row"><span class="game-team-name ' + (awayWin ? "winner" : homeWin ? "loser" : "") + '">' + esc(g.away_team) + '</span>';
        h += '<span class="game-score ' + (awayWin ? "winner" : homeWin ? "loser" : "") + '">' + (g.away_score != null ? g.away_score : "—") + '</span></div>';
        h += '<div class="game-ticket-row"><span class="game-team-name ' + (homeWin ? "winner" : awayWin ? "loser" : "") + '">' + esc(g.home_team) + '</span>';
        h += '<span class="game-score ' + (homeWin ? "winner" : awayWin ? "loser" : "") + '">' + (g.home_score != null ? g.home_score : "—") + '</span></div>';
        h += '</div>';
        var statusCls = g.status === "진행중" || g.status === "LIVE" ? "live" : "";
        h += '<div class="game-ticket-status ' + statusCls + '">' + esc(g.status || g.time || "") + '</div>';
        h += '</div>';
      });
      h += '</div></div><hr class="divider">';
    }

    // Briefing
    if (tab.briefing) {
      h += '<div class="reveal"><div class="section-label">KBO 브리핑</div>';
      h += '<div class="editorial-card"><div class="editorial-text">' + esc(tab.briefing) + '</div></div>';
      h += renderTrends(tab.trends);
      h += '</div><hr class="divider">';
    }

    h += renderNewsSection(tab.articles, "KBO 뉴스");
    el.innerHTML = h;
  }

  // ══════════════════════════════════════
  // SHARED RENDERERS
  // ══════════════════════════════════════
  function renderNewsSection(articles, label) {
    if (!articles || !articles.length) return "";
    var h = '<div class="reveal"><div class="section-label">' + label + ' <span style="font-weight:400;color:var(--text-4)">' + articles.length + '</span></div>';
    h += '<div class="news-list">';
    articles.forEach(function (a) {
      var imp = a.importance || "medium";
      h += '<div class="news-item"><span class="news-importance ' + imp + '"></span>';
      h += '<div class="news-body"><div class="news-title"><a href="' + safeUrl(a.url) + '" target="_blank" rel="noopener">' + esc(a.title) + '</a></div>';
      h += '<div class="news-meta"><span class="news-source">' + esc(a.source) + '</span><span>·</span><span>' + timeAgo(a.published) + '</span></div>';
      h += '</div></div>';
    });
    h += '</div></div>';
    return h;
  }

  function renderTrends(trends) {
    if (!trends || !trends.length) return "";
    var h = '<div class="trends">';
    trends.forEach(function (t) { h += '<span class="trend-kw">' + esc(t) + '</span>'; });
    return h + '</div>';
  }

  // ══════════════════════════════════════
  // HELPERS
  // ══════════════════════════════════════
  function fmtNum(n) {
    if (typeof n !== "number" || !Number.isFinite(n)) return "—";
    if (Math.abs(n) >= 1000) return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    if (Math.abs(n) < 0.01) return n.toPrecision(4);
    if (Math.abs(n) < 1) return n.toFixed(4);
    return n.toFixed(2);
  }

  function fmtBillion(n) {
    if (!n) return "";
    var abs = Math.abs(n);
    if (abs >= 10000) return (n > 0 ? "+" : "") + (n / 10000).toFixed(1) + "조";
    return (n > 0 ? "+" : "") + n.toLocaleString() + "억";
  }

  function timeAgo(iso) {
    if (!iso) return "";
    try {
      var ms = Date.now() - new Date(iso).getTime(), h = Math.floor(ms / 3600000);
      if (h < 1) return "방금";
      if (h < 24) return h + "시간 전";
      return Math.floor(h / 24) + "일 전";
    } catch (e) { return ""; }
  }

  var SAFE_SIGNALS = { bullish: 1, bearish: 1, neutral: 1, alert: 1 };
  function safeSignal(s) { return SAFE_SIGNALS[s] ? s : "neutral"; }

  var SAFE_DIRS = { long: 1, short: 1, neutral: 1 };
  function safeDir(s) { return SAFE_DIRS[s] ? s : "neutral"; }

  var SAFE_HL = { model: 1, tool: 1, trend: 1 };
  function safeHL(s) { return SAFE_HL[s] ? s : "trend"; }

  function esc(s) {
    if (!s) return "";
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function safeUrl(s) {
    if (!s) return "#";
    try {
      var u = new URL(s);
      return (u.protocol === "http:" || u.protocol === "https:") ? s.replace(/"/g, "&quot;") : "#";
    } catch (e) { return "#"; }
  }

  function show(id) { var e = document.getElementById(id); if (e) e.classList.remove("hidden"); }
  function hide(id) { var e = document.getElementById(id); if (e) e.classList.add("hidden"); }
  function showEmpty() {
    hide("loading");
    var el = document.getElementById("tab-invest");
    if (el) {
      el.innerHTML = '<div class="loading-state"><p class="loading-text" style="padding:60px 0">아직 발행된 다이제스트가 없습니다.<br>매일 오전 7시에 자동으로 생성됩니다.</p></div>';
      el.classList.remove("hidden");
    }
  }
  function showError(date) {
    hide("loading");
    var el = document.getElementById("tab-" + currentTab);
    if (el) {
      var p = date.split("-");
      var dayStr = p[0] + "년 " + parseInt(p[1]) + "월 " + parseInt(p[2]) + "일";
      el.innerHTML = '<div class="loading-state" style="padding:60px 0"><p class="loading-text">' + dayStr + ' 데이터가 없습니다.</p></div>';
      el.classList.remove("hidden");
    }
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
