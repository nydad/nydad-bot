#!/usr/bin/env python3
"""
nydad-bot — Unified Daily Digest v1.0
4-tab architecture: Investment | AI Industry | Crypto | AI Dev
Dual model: Gemini Flash (batch summaries) + Claude Sonnet (editorial)

Pipeline:
  Phase 0 — Market data (yfinance + CoinGecko + CNN/Crypto F&G)
  Phase 1 — KOSPI direction signal + sector recommendations
  Phase 2 — News collection (40+ RSS feeds across 4 tabs)
  Phase 3 — Batch summarization (Gemini Flash)
  Phase 4 — Editorial generation per tab (Claude Sonnet)
  Phase 5 — JSON output
"""

import os, sys, json, hashlib, logging, re, time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import requests

try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    import trafilatura
except ImportError:
    trafilatura = None

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass

API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
MODEL_FAST = os.environ.get("OPENROUTER_MODEL_FAST", "google/gemini-3-flash-preview")
MODEL_QUALITY = os.environ.get("OPENROUTER_MODEL_QUALITY", "anthropic/claude-sonnet-4.6")
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

MAX_PER_FEED = 12
BATCH_SIZE = 8
RETRY = 2
RETRY_DELAY = 5

KST = timezone(timedelta(hours=9))
NOW_KST = datetime.now(KST)
TODAY = NOW_KST.strftime("%Y-%m-%d")
DOW = NOW_KST.weekday()  # 0=Mon
IS_MONDAY = DOW == 0
AGE_HOURS = 72 if IS_MONDAY else 36

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("nydad-bot")

HEADERS = {"User-Agent": "NydadBot/1.0 (github.com/nydad/nydad-bot)"}

# ---------------------------------------------------------------------------
# Tickers
# ---------------------------------------------------------------------------
US_TICKERS = {
    "us_indices": [
        ("S&P 500", "^GSPC"), ("NASDAQ", "^IXIC"), ("다우존스", "^DJI"),
        ("러셀 2000", "^RUT"), ("필라델피아 반도체", "^SOX"),
    ],
    "futures": [
        ("S&P 500 선물", "ES=F"), ("나스닥 선물", "NQ=F"), ("다우 선물", "YM=F"),
    ],
    "volatility": [("VIX", "^VIX")],
    "forex": [
        ("달러/원", "KRW=X"), ("달러/엔", "JPY=X"),
        ("유로/달러", "EURUSD=X"), ("달러인덱스", "DX-Y.NYB"),
    ],
    "commodities": [
        ("WTI 원유", "CL=F"), ("브렌트유", "BZ=F"), ("금", "GC=F"),
        ("은", "SI=F"), ("천연가스", "NG=F"), ("구리", "HG=F"),
    ],
    "bonds": [("미국 10년물", "^TNX"), ("미국 2년물", "^IRX")],
}

KR_TICKERS = {
    "kr_indices": [
        ("KOSPI", "^KS11"), ("KOSDAQ", "^KQ11"), ("KOSPI 200", "^KS200"),
    ],
    "kr_sectors": [
        ("반도체", "091160.KS"), ("2차전지", "305720.KS"),
        ("바이오", "143860.KS"), ("은행", "091170.KS"),
        ("KODEX 200", "069500.KS"),
    ],
}

# ---------------------------------------------------------------------------
# News Sources by Tab
# ---------------------------------------------------------------------------
INVEST_FEEDS = [
    {"name": "CNBC Top", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114"},
    {"name": "CNBC Economy", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258"},
    {"name": "CNBC World", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362"},
    {"name": "MarketWatch", "url": "https://feeds.marketwatch.com/marketwatch/topstories/"},
    {"name": "Yahoo Finance", "url": "https://finance.yahoo.com/news/rssindex"},
    {"name": "Investing.com", "url": "https://www.investing.com/rss/news.rss"},
    {"name": "Seeking Alpha", "url": "https://seekingalpha.com/market_currents.xml"},
    {"name": "Reuters", "url": "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best"},
    {"name": "Bloomberg", "url": "https://feeds.bloomberg.com/markets/news.rss"},
    {"name": "한국경제", "url": "https://www.hankyung.com/feed/world-news"},
    {"name": "한경 증권", "url": "https://www.hankyung.com/feed/stock"},
    {"name": "매일경제", "url": "https://www.mk.co.kr/rss/30100041/"},
    {"name": "연합뉴스", "url": "https://www.yna.co.kr/rss/economy.xml"},
    {"name": "연합인포맥스", "url": "https://news.einfomax.co.kr/rss/S1N1.xml"},
    {"name": "조선비즈", "url": "https://biz.chosun.com/rss/finance/"},
]

AI_INDUSTRY_FEEDS = [
    {"name": "TechCrunch AI", "url": "https://techcrunch.com/category/artificial-intelligence/feed/"},
    {"name": "The Verge AI", "url": "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml"},
    {"name": "Ars Technica", "url": "https://feeds.arstechnica.com/arstechnica/technology-lab"},
    {"name": "VentureBeat AI", "url": "https://venturebeat.com/category/ai/feed/"},
    {"name": "Wired AI", "url": "https://www.wired.com/feed/tag/ai/latest/rss"},
    {"name": "OpenAI Blog", "url": "https://openai.com/blog/rss.xml"},
    {"name": "Google AI", "url": "https://blog.google/technology/ai/rss/"},
    {"name": "Microsoft AI", "url": "https://blogs.microsoft.com/ai/feed/"},
    {"name": "Meta AI", "url": "https://ai.meta.com/blog/rss/"},
    {"name": "MIT Tech Review", "url": "https://www.technologyreview.com/feed/"},
]

CRYPTO_FEEDS = [
    {"name": "CoinDesk", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/"},
    {"name": "CoinTelegraph", "url": "https://cointelegraph.com/rss"},
    {"name": "The Block", "url": "https://www.theblock.co/rss.xml"},
    {"name": "Decrypt", "url": "https://decrypt.co/feed"},
    {"name": "CryptoSlate", "url": "https://cryptoslate.com/feed/"},
    {"name": "Blockworks", "url": "https://blockworks.com/feed"},
    {"name": "DL News", "url": "https://www.dlnews.com/arc/outboundfeeds/rss/"},
    {"name": "블록미디어", "url": "https://www.blockmedia.co.kr/feed/"},
    {"name": "토큰포스트", "url": "https://www.tokenpost.kr/rss"},
]

AI_DEV_FEEDS = [
    {"name": "GitHub Blog", "url": "https://github.blog/feed/"},
    {"name": "GitHub Changelog", "url": "https://github.blog/changelog/feed/"},
    {"name": "Hugging Face", "url": "https://huggingface.co/blog/feed.xml"},
    {"name": "Dev.to AI", "url": "https://dev.to/feed/tag/ai"},
    {"name": "arXiv AI", "url": "https://rss.arxiv.org/rss/cs.AI"},
    {"name": "arXiv LLM", "url": "https://rss.arxiv.org/rss/cs.CL"},
    {"name": "GeekNews", "url": "https://news.hada.io/rss/news"},
    {"name": "HN AI", "url": "https://hnrss.org/newest?q=AI+OR+LLM+OR+GPT+OR+Claude&points=80"},
    {"name": "Lobsters AI", "url": "https://lobste.rs/t/ai.rss"},
]

# ---------------------------------------------------------------------------
# Phase 0: Market Data
# ---------------------------------------------------------------------------
def fetch_market_data() -> dict:
    if not yf:
        log.warning("yfinance not installed")
        return {}
    log.info("=== Phase 0: Market Data ===")
    all_tickers = {}
    combined = {**US_TICKERS, **KR_TICKERS}
    for cat, items in combined.items():
        for name, sym in items:
            all_tickers[sym] = (cat, name)

    symbols = list(all_tickers.keys())
    market = {}
    try:
        df = yf.download(symbols, period="5d", interval="1d", progress=False, threads=True, timeout=30)
        for sym, (cat, name) in all_tickers.items():
            try:
                close = df["Close"][sym].dropna() if len(symbols) > 1 else df["Close"].dropna()
                if len(close) < 1:
                    continue
                cur = float(close.iloc[-1])
                prev = float(close.iloc[-2]) if len(close) >= 2 else cur
                chg = cur - prev
                pct = (chg / prev) * 100 if prev else 0
                prec = 4 if cat in ("forex", "bonds") else 2
                market.setdefault(cat, []).append({
                    "name": name, "ticker": sym,
                    "price": round(cur, prec), "change": round(chg, prec),
                    "change_pct": round(pct, 2),
                })
            except Exception as e:
                log.warning("  %s: %s", name, e)
    except Exception as e:
        log.error("Batch download failed: %s", e)

    for cat in combined:
        market.setdefault(cat, [])
        log.info("  %s: %d", cat, len(market[cat]))
    return market


def fetch_crypto_prices() -> list[dict]:
    """Top 15 crypto prices from CoinGecko."""
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "order": "market_cap_desc",
                    "per_page": 15, "page": 1, "sparkline": "false",
                    "price_change_percentage": "24h"},
            headers=HEADERS, timeout=15)
        r.raise_for_status()
        return [{
            "name": c["name"], "symbol": c["symbol"].upper(),
            "price": c["current_price"], "change_pct": round(c.get("price_change_percentage_24h") or 0, 2),
            "market_cap": c.get("market_cap", 0),
            "volume_24h": c.get("total_volume", 0),
            "rank": c.get("market_cap_rank", 0),
        } for c in r.json()]
    except Exception as e:
        log.warning("CoinGecko failed: %s", e)
        return []


def fetch_fear_greed() -> dict:
    result = {}
    # US Fear & Greed
    try:
        r = requests.get("https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
                         headers={**HEADERS, "Referer": "https://www.cnn.com/markets/fear-and-greed"}, timeout=15)
        r.raise_for_status()
        fg = r.json().get("fear_and_greed", {})
        result["us"] = {"score": round(fg.get("score", 0), 1), "rating": fg.get("rating", ""),
                        "previous": round(fg.get("previous_close", 0), 1)}
    except Exception as e:
        log.warning("US F&G: %s", e)

    # Crypto Fear & Greed
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", headers=HEADERS, timeout=10)
        r.raise_for_status()
        d = r.json().get("data", [{}])[0]
        result["crypto"] = {"score": int(d.get("value", 0)), "rating": d.get("value_classification", "")}
    except Exception as e:
        log.warning("Crypto F&G: %s", e)

    return result


# ---------------------------------------------------------------------------
# Phase 1: KOSPI Signal
# ---------------------------------------------------------------------------
def calculate_kospi_signal(market: dict) -> dict:
    """Composite long/short signal from available data."""
    factors = []

    def find(cat, name_part):
        for item in market.get(cat, []):
            if name_part in item["name"]:
                return item
        return None

    # 1. VIX
    vix = find("volatility", "VIX")
    if vix:
        v = vix["price"]
        if v < 18:
            factors.append({"name": "VIX 안정권", "signal": "bullish", "detail": f"VIX {v:.1f}"})
        elif v > 28:
            factors.append({"name": "VIX 공포 구간", "signal": "bearish", "detail": f"VIX {v:.1f}"})
        else:
            factors.append({"name": "VIX 경계 구간", "signal": "neutral", "detail": f"VIX {v:.1f}"})

    # 2. US Futures
    spf = find("futures", "S&P 500")
    if spf:
        if spf["change_pct"] > 0.3:
            factors.append({"name": "미 선물 강세", "signal": "bullish", "detail": f"{spf['change_pct']:+.2f}%"})
        elif spf["change_pct"] < -0.3:
            factors.append({"name": "미 선물 약세", "signal": "bearish", "detail": f"{spf['change_pct']:+.2f}%"})
        else:
            factors.append({"name": "미 선물 보합", "signal": "neutral", "detail": f"{spf['change_pct']:+.2f}%"})

    # 3. USD/KRW
    krw = find("forex", "달러/원")
    if krw:
        if krw["change_pct"] < -0.2:
            factors.append({"name": "원화 강세", "signal": "bullish", "detail": f"USD/KRW {krw['price']:.0f}"})
        elif krw["change_pct"] > 0.2:
            factors.append({"name": "원화 약세", "signal": "bearish", "detail": f"USD/KRW {krw['price']:.0f}"})
        else:
            factors.append({"name": "환율 보합", "signal": "neutral", "detail": f"USD/KRW {krw['price']:.0f}"})

    # 4. S&P 500 close
    sp = find("us_indices", "S&P 500")
    if sp:
        if sp["change_pct"] > 0.3:
            factors.append({"name": "미 증시 강세 마감", "signal": "bullish", "detail": f"{sp['change_pct']:+.2f}%"})
        elif sp["change_pct"] < -0.3:
            factors.append({"name": "미 증시 약세 마감", "signal": "bearish", "detail": f"{sp['change_pct']:+.2f}%"})
        else:
            factors.append({"name": "미 증시 보합 마감", "signal": "neutral", "detail": f"{sp['change_pct']:+.2f}%"})

    # 5. KOSPI trend
    kospi = find("kr_indices", "KOSPI")
    if kospi:
        if kospi["change_pct"] > 0.3:
            factors.append({"name": "KOSPI 상승 흐름", "signal": "bullish", "detail": f"{kospi['change_pct']:+.2f}%"})
        elif kospi["change_pct"] < -0.3:
            factors.append({"name": "KOSPI 하락 흐름", "signal": "bearish", "detail": f"{kospi['change_pct']:+.2f}%"})
        else:
            factors.append({"name": "KOSPI 보합", "signal": "neutral", "detail": f"{kospi['change_pct']:+.2f}%"})

    if not factors:
        return {"direction": "neutral", "confidence": 0, "factors": [], "sectors": []}

    bull = sum(1 for f in factors if f["signal"] == "bullish")
    bear = sum(1 for f in factors if f["signal"] == "bearish")
    total = len(factors)

    if bull >= 4:
        direction = "long"
    elif bear >= 4:
        direction = "short"
    elif bull >= 3:
        direction = "long"
    elif bear >= 3:
        direction = "short"
    else:
        direction = "neutral"

    confidence = round(max(bull, bear) / total, 2) if total else 0

    # Sector recommendations based on signal
    sectors = []
    if direction == "long":
        sectors = [
            {"name": "반도체", "reason": "미 증시 강세 시 외국인 수급 유입 기대"},
            {"name": "2차전지", "reason": "성장주 랠리 시 수혜"},
            {"name": "자동차", "reason": "원화 강세 시 수출주 반등"},
        ]
    elif direction == "short":
        sectors = [
            {"name": "방어주/유틸리티", "reason": "하락장 방어 섹터"},
            {"name": "통신", "reason": "배당 매력 부각"},
            {"name": "금/원자재 ETF", "reason": "안전자산 선호 구간"},
        ]
    else:
        sectors = [
            {"name": "배당주", "reason": "박스권 장세에서 배당 수익 확보"},
            {"name": "바이오", "reason": "개별 모멘텀 중심 접근"},
        ]

    return {"direction": direction, "confidence": confidence, "factors": factors, "sectors": sectors}


# ---------------------------------------------------------------------------
# Phase 2: News Collection
# ---------------------------------------------------------------------------
def _fetch_rss(cfg, cutoff):
    name = cfg["name"]
    try:
        feed = feedparser.parse(cfg["url"], request_headers=HEADERS)
        if feed.bozo and not feed.entries:
            return []
        results = []
        for entry in feed.entries[:MAX_PER_FEED]:
            pub = _parse_date(entry)
            if pub and pub < cutoff:
                continue
            title = (entry.get("title") or "").strip()
            link = (entry.get("link") or "").strip()
            if not title or not link:
                continue
            results.append({
                "title": title, "url": link, "source": name,
                "published": (pub or datetime.now(timezone.utc)).isoformat(),
                "description": _clean(entry.get("summary", ""))[:600],
            })
        log.info("  %-20s -> %d", name, len(results))
        return results
    except Exception as e:
        log.error("  %s: %s", name, e)
        return []


def fetch_tab_feeds(feeds, tab_name):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=AGE_HOURS)
    articles = []
    log.info("--- %s feeds ---", tab_name)
    for f in feeds:
        articles.extend(_fetch_rss(f, cutoff))
    seen = set()
    unique = []
    for a in articles:
        key = hashlib.md5(a["url"].lower().split("?")[0].rstrip("/").encode()).hexdigest()
        if key not in seen:
            seen.add(key)
            unique.append(a)
    log.info("  %s unique: %d", tab_name, len(unique))
    return unique


def _parse_date(entry):
    for f in ("published_parsed", "updated_parsed", "created_parsed"):
        p = entry.get(f)
        if p:
            try:
                return datetime(*p[:6], tzinfo=timezone.utc)
            except (ValueError, TypeError):
                pass
    return None


def _clean(text):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text)).strip()


def extract_content(url):
    if not trafilatura:
        return ""
    try:
        dl = trafilatura.fetch_url(url)
        if dl:
            t = trafilatura.extract(dl, include_comments=False, include_tables=False, deduplicate=True)
            if t:
                return t[:2500]
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# OpenRouter API
# ---------------------------------------------------------------------------
def _call_api(model, system, user, max_tokens=4096):
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json",
               "HTTP-Referer": "https://github.com/nydad/nydad-bot", "X-Title": "Nydad Bot"}
    payload = {"model": model, "messages": [{"role": "system", "content": system},
               {"role": "user", "content": user}], "temperature": 0.2, "max_tokens": max_tokens,
               "response_format": {"type": "json_object"}}
    for attempt in range(1, RETRY + 1):
        try:
            resp = requests.post("https://openrouter.ai/api/v1/chat/completions",
                                 headers=headers, json=payload, timeout=180)
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                content = "\n".join(content.split("\n")[1:])
                if content.rstrip().endswith("```"):
                    content = content.rstrip()[:-3]
                content = content.strip()
            result = json.loads(content)
            log.info("API OK (%s, attempt %d)", model.split("/")[-1], attempt)
            return result
        except Exception as e:
            log.warning("API fail (attempt %d): %s", attempt, e)
            if attempt < RETRY:
                time.sleep(RETRY_DELAY)
    return None


# ---------------------------------------------------------------------------
# Phase 3: Batch Summarization
# ---------------------------------------------------------------------------
SUMMARY_PROMPTS = {
    "invest": """You are a financial news analyst. For EACH article, provide:
- "summary": 2-3 sentence Korean summary. Keep numbers, tickers, proper nouns in English.
- "importance": "high"/"medium"/"low"
- "tags": 2-4 lowercase English tags
Return: {"articles": [{"summary":"...", "importance":"...", "tags":[...]}, ...]}
CRITICAL: Exactly one entry per article, SAME ORDER.""",

    "ai_industry": """You are an AI industry analyst. For EACH article, provide:
- "summary": 2-3 sentence Korean summary. Keep proper nouns in English.
- "importance": "high"/"medium"/"low"
- "tags": 2-4 lowercase English tags
Return: {"articles": [{"summary":"...", "importance":"...", "tags":[...]}, ...]}
CRITICAL: Exactly one entry per article, SAME ORDER.""",

    "crypto": """You are a crypto market analyst. For EACH article, provide:
- "summary": 2-3 sentence Korean summary. Keep coin names, numbers in English.
- "importance": "high"/"medium"/"low"
- "tags": 2-4 lowercase English tags
Return: {"articles": [{"summary":"...", "importance":"...", "tags":[...]}, ...]}
CRITICAL: Exactly one entry per article, SAME ORDER.""",

    "ai_dev": """You are an AI/dev tools analyst. For EACH article, provide:
- "summary": 2-3 sentence Korean summary. Keep tech terms in English.
- "importance": "high"/"medium"/"low"
- "tags": 2-4 lowercase English tags
Return: {"articles": [{"summary":"...", "importance":"...", "tags":[...]}, ...]}
CRITICAL: Exactly one entry per article, SAME ORDER.""",
}


def summarize_tab(articles, tab_id):
    if not articles:
        return []
    enriched = []
    for i in range(0, len(articles), BATCH_SIZE):
        batch = articles[i:i + BATCH_SIZE]
        log.info("  Batch %d: %d articles", i // BATCH_SIZE + 1, len(batch))
        parts = []
        for j, a in enumerate(batch):
            content = extract_content(a["url"]) or a.get("description", "")
            parts.append(f"---ARTICLE {j+1}---\nTitle: {a['title']}\nSource: {a['source']}\nContent:\n{content[:2000]}\n")
            time.sleep(0.15)
        user = f"Summarize {len(batch)} articles.\n\n" + "\n".join(parts)
        ai = _call_api(MODEL_FAST, SUMMARY_PROMPTS.get(tab_id, SUMMARY_PROMPTS["invest"]), user)
        ai_list = ai.get("articles", []) if ai else []
        for j, article in enumerate(batch):
            if j < len(ai_list):
                item = ai_list[j]
                enriched.append({**article, "summary": item.get("summary", article.get("description", "")[:300]),
                                 "importance": item.get("importance", "medium"), "tags": item.get("tags", [])})
            else:
                enriched.append({**article, "summary": article.get("description", "")[:300],
                                 "importance": "medium", "tags": []})
    imp = {"high": 0, "medium": 1, "low": 2}
    enriched.sort(key=lambda a: imp.get(a.get("importance"), 2))
    return enriched


# ---------------------------------------------------------------------------
# Phase 4: Editorial Generation
# ---------------------------------------------------------------------------
EDITORIAL_INVEST = """You are a senior financial analyst writing a morning briefing for Korean investors.

Given market data + KOSPI signal + news, generate IN KOREAN:

1. "briefing": 6-8 sentences. Professional market overview covering US close, futures, key drivers.
   {monday_note}
2. "key_insights": 3-5 items, each: {{"title":"...(max 20 chars)", "detail":"1-2 sentences", "type":"bullish/bearish/neutral/alert"}}
3. "forex_commentary": 2-3 sentences about USD/KRW.
4. "commodity_commentary": 2-3 sentences about oil/gold.
5. "outlook": 2-3 sentences. Today's outlook + key events.
6. "sector_analysis": Validate/refine the provided sector recommendations. Return 3 items: {{"name":"sector", "direction":"overweight/underweight/neutral", "reason":"1 sentence"}}
7. "trends": 3-5 short Korean keywords.

Return JSON with all 7 fields. Korean text, keep numbers/tickers/names in English."""

EDITORIAL_AI = """You are a senior AI analyst writing daily briefings in KOREAN.

Given AI industry news articles, generate:
1. "briefing": 4-6 sentences about today's key AI industry developments.
2. "quotes": 3-5 notable quotes: {{"quote":"original language","speaker":"Korean name","context":"Korean"}}
3. "trends": 2-3 Korean trend keywords.

Return JSON. Korean text, keep proper nouns in English."""

EDITORIAL_CRYPTO = """You are a senior crypto analyst writing daily briefings in KOREAN.

Given crypto market data + news, generate:
1. "briefing": 4-6 sentences covering major crypto market moves, BTC/ETH price action, notable events.
2. "key_events": 3-5 items: {{"title":"short Korean title","detail":"1-2 sentences","type":"bullish/bearish/neutral/alert"}}
3. "trends": 2-3 Korean trend keywords.

Return JSON. Korean text, keep coin names/numbers in English."""

EDITORIAL_DEV = """You are a senior AI/dev analyst writing briefings for developers in KOREAN.

Given dev/coding news, generate:
1. "briefing": 4-6 sentences from developer perspective about new tools, models, methodologies.
2. "highlights": 3-5 items: {{"type":"model/tool/trend","title":"short Korean title","detail":"1-2 sentences"}}
3. "trends": 2-3 Korean developer-focused trend keywords.

Return JSON. Korean text, keep tech terms in English."""


def generate_invest_editorial(market, signal, fg, articles):
    monday = "Include weekend recap in briefing." if IS_MONDAY else ""
    prompt = EDITORIAL_INVEST.replace("{monday_note}", monday)

    parts = ["=== MARKET DATA ==="]
    for cat, items in market.items():
        if items:
            parts.append(f"[{cat}]")
            for i in items:
                s = "+" if i["change"] >= 0 else ""
                parts.append(f"  {i['name']}: {i['price']} ({s}{i['change']}, {s}{i['change_pct']}%)")

    parts.append(f"\n=== KOSPI SIGNAL: {signal['direction'].upper()} (conf: {signal['confidence']}) ===")
    for f in signal["factors"]:
        parts.append(f"  {f['name']}: {f['signal']} ({f['detail']})")
    parts.append("Sectors: " + ", ".join(s["name"] for s in signal.get("sectors", [])))

    if fg.get("us"):
        parts.append(f"\n=== US F&G: {fg['us']['score']} ({fg['us']['rating']}) ===")

    parts.append("\n=== NEWS ===")
    for a in articles[:20]:
        parts.append(f"  [{a['source']}] {a['title']}\n    {a.get('summary', '')[:200]}")

    user = f"Today: {NOW_KST.strftime('%Y-%m-%d %A')}\nWindow: {AGE_HOURS}h\n\n" + "\n".join(parts)
    return _call_api(MODEL_QUALITY, prompt, user, 3000) or {}


def generate_editorial(prompt, articles, extra_context=""):
    parts = []
    if extra_context:
        parts.append(extra_context)
    parts.append("=== ARTICLES ===")
    for a in articles[:20]:
        parts.append(f"[{a['source']}] {a['title']}\n  {a.get('summary', '')[:200]}")
    user = f"Today: {NOW_KST.strftime('%Y-%m-%d %A')}\n{len(articles)} articles\n\n" + "\n".join(parts)
    return _call_api(MODEL_QUALITY, prompt, user, 2500) or {}


# ---------------------------------------------------------------------------
# Phase 5: Build Digest
# ---------------------------------------------------------------------------
def article_out(a):
    return {"title": a["title"], "url": a["url"], "source": a["source"],
            "published": a["published"], "summary": a.get("summary", ""),
            "importance": a.get("importance", "medium"), "tags": a.get("tags", [])}


def build_digest():
    # Phase 0
    market = fetch_market_data()
    crypto_prices = fetch_crypto_prices()
    fg = fetch_fear_greed()

    # Phase 1
    log.info("=== Phase 1: KOSPI Signal ===")
    signal = calculate_kospi_signal(market)
    log.info("  Direction: %s (conf: %s)", signal["direction"], signal["confidence"])

    # Phase 2
    log.info("=== Phase 2: News Collection ===")
    invest_raw = fetch_tab_feeds(INVEST_FEEDS, "invest")
    ai_raw = fetch_tab_feeds(AI_INDUSTRY_FEEDS, "ai_industry")
    crypto_raw = fetch_tab_feeds(CRYPTO_FEEDS, "crypto")
    dev_raw = fetch_tab_feeds(AI_DEV_FEEDS, "ai_dev")

    # Phase 3
    log.info("=== Phase 3: Summarization (%s) ===", MODEL_FAST)
    log.info("--- invest ---")
    invest_articles = summarize_tab(invest_raw, "invest")
    log.info("--- ai_industry ---")
    ai_articles = summarize_tab(ai_raw, "ai_industry")
    log.info("--- crypto ---")
    crypto_articles = summarize_tab(crypto_raw, "crypto")
    log.info("--- ai_dev ---")
    dev_articles = summarize_tab(dev_raw, "ai_dev")

    total = len(invest_articles) + len(ai_articles) + len(crypto_articles) + len(dev_articles)
    log.info("Phase 3 done: %d total articles", total)

    # Phase 4
    log.info("=== Phase 4: Editorial (%s) ===", MODEL_QUALITY)
    log.info("--- invest editorial ---")
    invest_ed = generate_invest_editorial(market, signal, fg, invest_articles)

    log.info("--- ai editorial ---")
    ai_ed = generate_editorial(EDITORIAL_AI, ai_articles)

    log.info("--- crypto editorial ---")
    crypto_ctx = ""
    if crypto_prices:
        lines = ["=== CRYPTO PRICES ==="]
        for c in crypto_prices[:10]:
            lines.append(f"  #{c['rank']} {c['symbol']}: ${c['price']:,.2f} ({c['change_pct']:+.2f}%)")
        if fg.get("crypto"):
            lines.append(f"\nCrypto F&G: {fg['crypto']['score']} ({fg['crypto']['rating']})")
        crypto_ctx = "\n".join(lines)
    crypto_ed = generate_editorial(EDITORIAL_CRYPTO, crypto_articles, crypto_ctx)

    log.info("--- dev editorial ---")
    dev_ed = generate_editorial(EDITORIAL_DEV, dev_articles)

    # Merge sector analysis from AI with signal sectors
    if invest_ed.get("sector_analysis"):
        signal["sectors"] = invest_ed["sector_analysis"]

    # Phase 5
    log.info("=== Phase 5: Build JSON ===")
    return {
        "date": TODAY,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "market_data": market,
        "crypto_prices": crypto_prices,
        "fear_greed": fg,
        "kospi_signal": signal,
        "total_articles": total,
        "tabs": {
            "invest": {
                "briefing": invest_ed.get("briefing", ""),
                "key_insights": invest_ed.get("key_insights", []),
                "forex_commentary": invest_ed.get("forex_commentary", ""),
                "commodity_commentary": invest_ed.get("commodity_commentary", ""),
                "outlook": invest_ed.get("outlook", ""),
                "trends": invest_ed.get("trends", []),
                "articles": [article_out(a) for a in invest_articles],
            },
            "ai_industry": {
                "briefing": ai_ed.get("briefing", ""),
                "quotes": ai_ed.get("quotes", []),
                "trends": ai_ed.get("trends", []),
                "articles": [article_out(a) for a in ai_articles],
            },
            "crypto": {
                "briefing": crypto_ed.get("briefing", ""),
                "key_events": crypto_ed.get("key_events", []),
                "trends": crypto_ed.get("trends", []),
                "articles": [article_out(a) for a in crypto_articles],
            },
            "ai_dev": {
                "briefing": dev_ed.get("briefing", ""),
                "highlights": dev_ed.get("highlights", []),
                "trends": dev_ed.get("trends", []),
                "articles": [article_out(a) for a in dev_articles],
            },
        },
    }


def update_index():
    dates = sorted([f.stem for f in DATA_DIR.glob("*.json") if f.stem != "index"], reverse=True)
    with open(DATA_DIR / "index.json", "w", encoding="utf-8") as f:
        json.dump({"dates": dates, "latest": dates[0] if dates else None}, f, indent=2)
    log.info("Index: %d dates", len(dates))


def main():
    if not API_KEY:
        log.error("OPENROUTER_API_KEY not set!")
        sys.exit(1)

    log.info("=" * 60)
    log.info("nydad-bot Unified Digest v1.0")
    log.info("Fast: %s | Quality: %s", MODEL_FAST, MODEL_QUALITY)
    log.info("Date: %s (KST) | Window: %dh%s", TODAY, AGE_HOURS,
             " [MONDAY WEEKEND RECAP]" if IS_MONDAY else "")
    log.info("=" * 60)

    digest = build_digest()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{digest['date']}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(digest, f, ensure_ascii=False, indent=2)
    log.info("Saved: %s (%d articles)", path.name, digest["total_articles"])

    update_index()
    log.info("=" * 60)
    log.info("Done!")


if __name__ == "__main__":
    main()
