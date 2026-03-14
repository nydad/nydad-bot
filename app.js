/**
 * nydad Daily Digest v1.0
 * 4-tab unified dashboard: Invest | AI Industry | Crypto | AI Dev
 */
(function () {
  "use strict";

  var dates = [], currentDate = "", currentTab = "invest", cache = {};
  var INSIGHT_KR = { bullish: "\uAC15\uC138", bearish: "\uC57D\uC138", neutral: "\uC911\uB9BD", alert: "\uC8FC\uC758" };
  var HL_KR = { model: "\uBAA8\uB378", tool: "\uB3C4\uAD6C", trend: "\uB3D9\uD5A5" };
  var MKT_TITLES = {
    us_indices: "\uBBF8\uAD6D \uC9C0\uC218", kr_indices: "\uD55C\uAD6D \uC9C0\uC218", kr_sectors: "\uC139\uD130 ETF",
    futures: "\uC120\uBB3C", volatility: "\uBCC0\uB3D9\uC131", forex: "\uD658\uC728",
    commodities: "\uC6D0\uC790\uC7AC", bonds: "\uCC44\uAD8C"
  };
  var DIR_KR = { long: "LONG \uB871", short: "SHORT \uC20F", neutral: "NEUTRAL \uC911\uB9BD" };
  var DIR_SUB = { long: "\uCF54\uC2A4\uD53C \uB871 \uCD94\uCC9C", short: "\uCF54\uC2A4\uD53C \uC20F \uCD94\uCC9C", neutral: "\uC911\uB9BD \uAD00\uB9DD" };

  async function init() {
    setupTabs(); setupSync();
    try {
      var r = await fetch("./data/index.json");
      if (!r.ok) throw 0;
      var d = await r.json();
      dates = d.dates || [];
      if (!dates.length) return showEmpty();
      renderDateBar(); selectDate(dates[0]);
    } catch (e) { showEmpty(); }
  }

  function setupTabs() {
    document.querySelectorAll(".tab").forEach(function (btn) {
      btn.addEventListener("click", function () {
        if (btn.dataset.tab === currentTab) return;
        document.querySelectorAll(".tab").forEach(function (b) { b.classList.remove("active"); });
        btn.classList.add("active"); currentTab = btn.dataset.tab;
        var syncBtn = document.getElementById("sync-btn");
        if (syncBtn) { currentTab === "invest" ? syncBtn.classList.remove("hidden") : syncBtn.classList.add("hidden"); }
        if (cache[currentDate]) render(cache[currentDate]);
      });
    });
  }

  function setupSync() {
    var btn = document.getElementById("sync-btn");
    if (!btn) return;
    btn.addEventListener("click", async function () {
      btn.classList.add("syncing"); btn.querySelector("span").textContent = "Syncing...";
      try {
        var r = await fetch("./data/live.json?t=" + Date.now());
        if (r.ok) {
          var live = await r.json();
          if (cache[currentDate] && live.market_data) {
            cache[currentDate].market_data = live.market_data;
            if (live.crypto_prices) cache[currentDate].crypto_prices = live.crypto_prices;
            cache[currentDate]._synced = live.synced_at;
            render(cache[currentDate]);
          }
        }
      } catch (e) {}
      // Also try live CoinGecko
      try {
        var cr = await fetch("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=15&page=1&sparkline=false&price_change_percentage=24h");
        if (cr.ok) {
          var coins = await cr.json();
          if (cache[currentDate]) {
            cache[currentDate].crypto_prices = coins.map(function(c) {
              return { name: c.name, symbol: (c.symbol||"").toUpperCase(), price: c.current_price,
                change_pct: Math.round((c.price_change_percentage_24h||0)*100)/100,
                market_cap: c.market_cap||0, rank: c.market_cap_rank||0 };
            });
            render(cache[currentDate]);
          }
        }
      } catch (e) {}
      btn.classList.remove("syncing"); btn.querySelector("span").textContent = "Sync";
    });
  }

  function renderDateBar() {
    var el = document.getElementById("date-scroll"); if (!el) return;
    el.innerHTML = dates.map(function (d) {
      var p = d.split("-"), label = parseInt(p[1]) + "/" + parseInt(p[2]);
      var day = ["\uC77C","\uC6D4","\uD654","\uC218","\uBAA9","\uAE08","\uD1A0"][new Date(+p[0], p[1]-1, +p[2]).getDay()];
      return '<button class="date-chip" data-date="' + d + '">' + label + ' ' + day + '</button>';
    }).join("");
    el.addEventListener("click", function (e) { var c = e.target.closest(".date-chip"); if (c) selectDate(c.dataset.date); });
  }

  function selectDate(date) {
    currentDate = date;
    document.querySelectorAll(".date-chip").forEach(function (c) { c.classList.toggle("active", c.dataset.date === date); });
    loadDigest(date);
  }

  async function loadDigest(date) {
    show("loading"); hide("digest"); hide("empty-state");
    if (!isValidDate(date)) { showError(date); return; }
    if (cache[date]) { render(cache[date]); return; }
    try {
      var r = await fetch("./data/" + date + ".json"); if (!r.ok) throw 0;
      cache[date] = await r.json(); render(cache[date]);
    } catch (e) { showError(date); }
  }

  function render(d) {
    var fn = { invest: renderInvest, ai_industry: renderAI, crypto: renderCrypto, ai_dev: renderDev };
    (fn[currentTab] || renderInvest)(d);
    var syncBtn = document.getElementById("sync-btn");
    if (syncBtn) { currentTab === "invest" ? syncBtn.classList.remove("hidden") : syncBtn.classList.add("hidden"); }
  }

  // ── Invest Tab ──
  function renderInvest(d) {
    var el = document.getElementById("digest"); if (!el) return;
    var tab = (d.tabs || {}).invest || {};
    var h = "";

    // KOSPI Signal
    var sig = d.kospi_signal || {};
    var dir = sig.direction || "neutral";
    h += '<div class="signal-card ' + safeClass(dir) + ' fade-in">' +
      '<div class="signal-header">' +
      '<div><div class="signal-label">\uCF54\uC2A4\uD53C \uBC29\uD5A5\uC131</div>' +
      '<div class="signal-direction ' + safeClass(dir) + '">' + (DIR_KR[dir] || dir) + '</div></div>' +
      '<div><span class="signal-badge ' + safeClass(dir) + '">' + (DIR_SUB[dir] || "") + '</span>' +
      '<div class="signal-conf">\uC2E0\uB8B0\uB3C4 ' + Math.round((sig.confidence || 0) * 100) + '%</div></div>' +
      '</div>';
    if (sig.factors && sig.factors.length) {
      h += '<div class="signal-factors">';
      sig.factors.forEach(function (f) {
        h += '<span class="signal-factor ' + safeClass(f.signal) + '">' + esc(f.name) + ' ' + esc(f.detail) + '</span>';
      }); h += '</div>';
    }
    // Geopolitical risk badge
    var geo = sig.geo_risk || {};
    if (geo.level && geo.level !== "low") {
      var geoColors = { elevated: "var(--yellow)", high: "var(--orange)", critical: "var(--red)" };
      var geoLabels = { elevated: "\uC9C0\uC815\uD559 \uC8FC\uC758", high: "\uC9C0\uC815\uD559 \uC704\uD5D8", critical: "\uC9C0\uC815\uD559 \uC2EC\uAC01" };
      h += '<div style="margin-top:14px;padding:10px 14px;border-radius:8px;background:rgba(239,68,68,0.06);border:1px solid rgba(239,68,68,0.15);display:flex;align-items:center;gap:10px">' +
        '<span style="font-size:16px">\u26A0\uFE0F</span>' +
        '<div><div style="font-size:12px;font-weight:700;color:' + (geoColors[geo.level] || "var(--yellow)") + '">' + (geoLabels[geo.level] || geo.level) + ' (' + geo.hit_count + '\uAC74)</div>';
      if (geo.top_hits && geo.top_hits.length) {
        h += '<div style="font-size:11px;color:var(--text-3);margin-top:3px">';
        geo.top_hits.slice(0, 3).forEach(function (hit) {
          h += '<div>\u2022 [' + esc(hit.source) + '] ' + esc(hit.title) + '</div>';
        });
        h += '</div>';
      }
      h += '</div></div>';
    }

    if (sig.sectors && sig.sectors.length) {
      h += '<div class="sector-list">';
      sig.sectors.forEach(function (s) {
        var dirCls = s.direction || "overweight";
        h += '<div class="sector-chip"><span class="sector-name">' + esc(s.name) + '</span>' +
          '<span class="sector-dir ' + dirCls + '">' + dirCls + '</span>' +
          '<div class="sector-reason">' + esc(s.reason) + '</div></div>';
      }); h += '</div>';
    }
    h += '</div><hr class="divider">';

    // Briefing
    if (tab.briefing) {
      h += '<section class="briefing fade-in"><p class="section-label">\uC2DC\uD669 \uBE0C\uB9AC\uD551</p>' +
        '<div class="editorial">' + esc(tab.briefing) + '</div>' +
        '<p class="stats">' + fmtDate(d.date) + ' \u00B7 ' + (tab.articles ? tab.articles.length : 0) + '\uAC1C \uAE30\uC0AC' +
        (d._synced ? ' \u00B7 Synced ' + timeAgo(d._synced) : '') + '</p>' +
        renderTrends(tab.trends) + '</section><hr class="divider">';
    }

    // Market Data
    if (d.market_data) {
      h += '<section class="fade-in"><p class="section-label">\uC2DC\uC7A5 \uB370\uC774\uD130</p>';
      ["us_indices","kr_indices","kr_sectors","futures","volatility","forex","commodities","bonds"].forEach(function (cat) {
        var items = d.market_data[cat];
        if (items && items.length) {
          h += '<div class="market-section"><div class="market-section-title">' + (MKT_TITLES[cat] || cat) + '</div><div class="market-grid">';
          items.forEach(function (m) { h += renderMarketCard(m); });
          h += '</div></div>';
        }
      });
      h += '</section><hr class="divider">';
    }

    // Fear & Greed
    if (d.fear_greed && (d.fear_greed.us || d.fear_greed.crypto)) {
      h += '<section class="fade-in"><p class="section-label">Fear & Greed</p><div class="fg-row">';
      if (d.fear_greed.us) {
        var u = d.fear_greed.us, uc = u.score >= 60 ? "var(--green)" : u.score <= 40 ? "var(--red)" : "var(--yellow)";
        h += '<div class="fg-card"><div class="fg-score" style="color:' + uc + '">' + u.score + '</div>' +
          '<div class="fg-meta"><div class="fg-label">US Market</div><div class="fg-rating">' + esc(u.rating) + '</div>' +
          '<div class="fg-prev">\uC804\uC77C ' + u.previous + '</div></div></div>';
      }
      if (d.fear_greed.crypto) {
        var c = d.fear_greed.crypto, cc = c.score >= 60 ? "var(--green)" : c.score <= 40 ? "var(--red)" : "var(--yellow)";
        h += '<div class="fg-card"><div class="fg-score" style="color:' + cc + '">' + c.score + '</div>' +
          '<div class="fg-meta"><div class="fg-label">Crypto</div><div class="fg-rating">' + esc(c.rating) + '</div></div></div>';
      }
      h += '</div></section><hr class="divider">';
    }

    // Key Insights
    if (tab.key_insights && tab.key_insights.length) {
      h += '<section class="fade-in"><p class="section-label">\uD575\uC2EC \uC778\uC0AC\uC774\uD2B8</p><div class="insight-list">';
      tab.key_insights.forEach(function (ins) {
        var t = ins.type || "neutral";
        h += '<div class="insight-item"><span class="insight-type ' + safeClass(t) + '">' + (INSIGHT_KR[t]||t) + '</span>' +
          '<div class="insight-body"><div class="insight-title">' + esc(ins.title) + '</div>' +
          '<div class="insight-detail">' + esc(ins.detail) + '</div></div></div>';
      }); h += '</div></section><hr class="divider">';
    }

    // Commentary
    if (tab.forex_commentary || tab.commodity_commentary) {
      h += '<section class="fade-in"><div class="commentary-row">';
      if (tab.forex_commentary) h += '<div class="commentary-card"><div class="commentary-title">\uD658\uC728</div><div class="commentary-text">' + esc(tab.forex_commentary) + '</div></div>';
      if (tab.commodity_commentary) h += '<div class="commentary-card"><div class="commentary-title">\uC6D0\uC790\uC7AC</div><div class="commentary-text">' + esc(tab.commodity_commentary) + '</div></div>';
      h += '</div></section><hr class="divider">';
    }

    // Outlook
    if (tab.outlook) {
      h += '<section class="fade-in"><p class="section-label">\uC624\uB298\uC758 \uC804\uB9DD</p>' +
        '<div class="outlook-card"><div class="outlook-text">' + esc(tab.outlook) + '</div></div></section><hr class="divider">';
    }

    // Articles
    h += renderArticles(tab.articles, "\uD22C\uC790 \uB274\uC2A4");
    el.innerHTML = h; hide("loading"); hide("empty-state"); show("digest");
  }

  // ── AI Industry Tab ──
  function renderAI(d) {
    var el = document.getElementById("digest"); if (!el) return;
    var tab = (d.tabs || {}).ai_industry || {}; var h = "";
    if (tab.briefing) {
      h += '<section class="briefing fade-in"><p class="section-label">\uC624\uB298\uC758 AI \uBE0C\uB9AC\uD551</p>' +
        '<div class="editorial">' + esc(tab.briefing) + '</div>' + renderTrends(tab.trends) + '</section><hr class="divider">';
    }
    if (tab.quotes && tab.quotes.length) {
      h += '<section class="fade-in"><p class="section-label">\uC8FC\uC694 \uBC1C\uC5B8</p>';
      tab.quotes.forEach(function (q) {
        h += '<div class="quote-item"><p class="quote-text">\u201C' + esc(q.quote) + '\u201D</p>' +
          '<p class="quote-speaker">' + esc(q.speaker) + '</p><p class="quote-context">' + esc(q.context) + '</p></div>';
      }); h += '</section><hr class="divider">';
    }
    h += renderArticles(tab.articles, "AI \uC5C5\uACC4 \uB274\uC2A4");
    el.innerHTML = h; hide("loading"); hide("empty-state"); show("digest");
  }

  // ── Crypto Tab ──
  function renderCrypto(d) {
    var el = document.getElementById("digest"); if (!el) return;
    var tab = (d.tabs || {}).crypto || {}; var h = "";

    // Crypto prices
    if (d.crypto_prices && d.crypto_prices.length) {
      h += '<section class="fade-in" style="padding-top:24px"><p class="section-label">\uCF54\uC778 \uC2DC\uC138</p><div class="crypto-grid">';
      d.crypto_prices.forEach(function (c) {
        var cls = c.change_pct > 0 ? "up" : c.change_pct < 0 ? "down" : "flat";
        var sign = c.change_pct > 0 ? "+" : "";
        h += '<div class="crypto-card"><div class="crypto-left"><span class="crypto-rank">#' + c.rank + '</span>' +
          '<div><div class="crypto-symbol">' + esc(c.symbol) + '</div><div class="crypto-name">' + esc(c.name) + '</div></div></div>' +
          '<div><div class="crypto-price">$' + fmtNum(c.price) + '</div>' +
          '<div class="crypto-pct ' + cls + '">' + sign + c.change_pct + '%</div></div></div>';
      }); h += '</div></section><hr class="divider">';
    }

    // Crypto F&G
    if (d.fear_greed && d.fear_greed.crypto) {
      var fg = d.fear_greed.crypto, cc = fg.score >= 60 ? "var(--green)" : fg.score <= 40 ? "var(--red)" : "var(--yellow)";
      h += '<section class="fade-in"><div class="fg-row"><div class="fg-card"><div class="fg-score" style="color:' + cc + '">' + fg.score + '</div>' +
        '<div class="fg-meta"><div class="fg-label">Crypto Fear & Greed</div><div class="fg-rating">' + esc(fg.rating) + '</div></div></div></div></section><hr class="divider">';
    }

    if (tab.briefing) {
      h += '<section class="fade-in"><p class="section-label">\uCF54\uC778 \uBE0C\uB9AC\uD551</p>' +
        '<div class="editorial">' + esc(tab.briefing) + '</div>' + renderTrends(tab.trends) + '</section><hr class="divider">';
    }
    if (tab.key_events && tab.key_events.length) {
      h += '<section class="fade-in"><p class="section-label">\uC8FC\uC694 \uC774\uBCA4\uD2B8</p><div class="insight-list">';
      tab.key_events.forEach(function (ev) {
        var t = ev.type || "neutral";
        h += '<div class="insight-item"><span class="insight-type ' + safeClass(t) + '">' + (INSIGHT_KR[t]||t) + '</span>' +
          '<div class="insight-body"><div class="insight-title">' + esc(ev.title) + '</div>' +
          '<div class="insight-detail">' + esc(ev.detail) + '</div></div></div>';
      }); h += '</div></section><hr class="divider">';
    }
    h += renderArticles(tab.articles, "\uCF54\uC778 \uB274\uC2A4");
    el.innerHTML = h; hide("loading"); hide("empty-state"); show("digest");
  }

  // ── AI Dev Tab ──
  function renderDev(d) {
    var el = document.getElementById("digest"); if (!el) return;
    var tab = (d.tabs || {}).ai_dev || {}; var h = "";
    if (tab.briefing) {
      h += '<section class="briefing fade-in"><p class="section-label">\uAC1C\uBC1C\uC790 \uBE0C\uB9AC\uD551</p>' +
        '<div class="editorial">' + esc(tab.briefing) + '</div>' + renderTrends(tab.trends) + '</section><hr class="divider">';
    }
    if (tab.highlights && tab.highlights.length) {
      h += '<section class="fade-in"><p class="section-label">\uC8FC\uC694 \uC5C5\uB370\uC774\uD2B8</p><div class="highlight-list">';
      tab.highlights.forEach(function (hl) {
        var t = hl.type || "trend";
        h += '<div class="highlight-item"><span class="highlight-type ' + safeClass(t) + '">' + (HL_KR[t]||t) + '</span>' +
          '<div class="highlight-body"><div class="highlight-title">' + esc(hl.title) + '</div>' +
          '<div class="highlight-detail">' + esc(hl.detail) + '</div></div></div>';
      }); h += '</div></section><hr class="divider">';
    }
    h += renderArticles(tab.articles, "AI \uCF54\uB529 \uB274\uC2A4");
    el.innerHTML = h; hide("loading"); hide("empty-state"); show("digest");
  }

  // ── Shared Renderers ──
  function renderMarketCard(m) {
    var cls = m.change > 0 ? "up" : m.change < 0 ? "down" : "flat";
    var s = m.change > 0 ? "+" : "", arr = m.change > 0 ? "\u25B2" : m.change < 0 ? "\u25BC" : "";
    return '<div class="market-card"><div class="market-name">' + esc(m.name) + '</div>' +
      '<div class="market-price">' + fmtNum(m.price) + '</div>' +
      '<div class="market-change ' + cls + '">' + arr + ' ' + s + fmtNum(m.change) + ' (' + s + m.change_pct + '%)</div></div>';
  }

  function renderArticles(articles, label) {
    if (!articles || !articles.length) return "";
    var h = '<section class="fade-in"><p class="section-label">' + label + ' <span style="font-weight:400;color:var(--text-3)">' + articles.length + '</span></p>';
    articles.forEach(function (a) {
      var t = timeAgo(a.published), imp = a.importance === "high" ? '<span class="article-important">\uC8FC\uC694</span>' : "";
      var tags = "";
      if (a.tags && a.tags.length) {
        tags = '<div class="article-tags">';
        a.tags.slice(0, 4).forEach(function (tag) { tags += '<span class="article-tag">' + esc(tag) + '</span>'; });
        tags += '</div>';
      }
      h += '<article class="article"><h3 class="article-title"><a href="' + safeUrl(a.url) + '" target="_blank" rel="noopener">' + esc(a.title) + '</a></h3>' +
        '<div class="article-meta"><span class="article-source">' + esc(a.source) + '</span><span>\u00B7</span><span>' + t + '</span>' + imp + '</div>' +
        '<p class="article-summary">' + esc(a.summary || "") + '</p>' + tags + '</article>';
    });
    return h + '</section>';
  }

  function renderTrends(trends) {
    if (!trends || !trends.length) return "";
    var h = '<div class="trends"><span class="trend-label">\uD0A4\uC6CC\uB4DC</span>';
    trends.forEach(function (t) { h += '<span class="trend-item">' + esc(t) + '</span>'; });
    return h + '</div>';
  }

  // ── Helpers ──
  function fmtDate(s) {
    var p = s.split("-").map(Number);
    var day = ["\uC77C","\uC6D4","\uD654","\uC218","\uBAA9","\uAE08","\uD1A0"][new Date(p[0], p[1]-1, p[2]).getDay()];
    return p[0] + "\uB144 " + p[1] + "\uC6D4 " + p[2] + "\uC77C " + day + "\uC694\uC77C";
  }
  function fmtNum(n) {
    if (typeof n !== "number" || !Number.isFinite(n)) return "-";
    if (Math.abs(n) >= 1000) return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    if (Math.abs(n) < 0.01) return n.toPrecision(4);
    if (Math.abs(n) < 1) return n.toFixed(4);
    return n.toFixed(2);
  }
  function timeAgo(iso) {
    if (!iso) return ""; try { var ms = Date.now() - new Date(iso).getTime(), h = Math.floor(ms / 3600000);
      if (h < 1) return "\uBC29\uAE08"; if (h < 24) return h + "\uC2DC\uAC04 \uC804"; return Math.floor(h / 24) + "\uC77C \uC804";
    } catch (e) { return ""; }
  }
  // XSS: allow-list for class attribute values from JSON
  var SAFE_CLASSES = { bullish:1, bearish:1, neutral:1, alert:1, up:1, down:1, flat:1, long:1, short:1,
    overweight:1, underweight:1, model:1, tool:1, trend:1, elevated:1, high:1, critical:1 };
  function safeClass(s) { return SAFE_CLASSES[s] ? s : "neutral"; }
  function esc(s) { if (!s) return ""; var d = document.createElement("div"); d.textContent = s; return d.innerHTML; }
  // XSS: URL protocol validation (only http/https)
  function safeUrl(s) {
    if (!s) return "#";
    try { var u = new URL(s); return (u.protocol === "http:" || u.protocol === "https:") ? s.replace(/"/g, "&quot;") : "#"; }
    catch (e) { return "#"; }
  }
  // Date validation
  function isValidDate(s) { return /^\d{4}-\d{2}-\d{2}$/.test(s); }
  function show(id) { var e = document.getElementById(id); if (e) e.classList.remove("hidden"); }
  function hide(id) { var e = document.getElementById(id); if (e) e.classList.add("hidden"); }
  function showEmpty() { hide("loading"); hide("digest"); show("empty-state"); }
  function showError(date) {
    var el = document.getElementById("digest");
    if (el) el.innerHTML = '<div class="error-state fade-in"><h2>\uD574\uB2F9 \uB0A0\uC9DC \uB370\uC774\uD130 \uC5C6\uC74C</h2><p>' + fmtDate(date) + '</p></div>';
    hide("loading"); hide("empty-state"); show("digest");
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init); else init();
})();
