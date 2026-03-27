#!/usr/bin/env python3
"""
nydad-bot — Unified Daily Digest v2.0
5-tab architecture: Investment | Crypto | AI Industry | AI Dev | KBO
Dual model: Gemini Flash (batch summaries) + Claude Sonnet (editorial)

Pipeline:
  Phase 0 — Market data (yfinance + CoinGecko + CNN/Crypto F&G)
  Phase 1 — Correlation + Foreign Flow + Investment Signal
  Phase 2 — KBO Data (standings, games, news)
  Phase 3 — News collection (40+ RSS feeds across 5 tabs)
  Phase 4 — Batch summarization (Gemini Flash)
  Phase 5 — Editorial generation per tab (Claude Sonnet)
  Phase 6 — JSON output + Index + Cleanup
"""

import os, sys, json, hashlib, logging, math, re, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Optional: KBO data collection (may not be ready yet)
try:
    from kbo_collect import fetch_standings as fetch_kbo_standings, fetch_games_today as fetch_kbo_games_today, fetch_kbo_news
    HAS_KBO = True
except ImportError:
    HAS_KBO = False

# Optional: Domestic market analysis (may not be ready yet)
try:
    from domestic_analysis import (
        fetch_correlation_data as calculate_correlations,
        fetch_foreign_flow,
    )
    HAS_DOMESTIC = True
except ImportError:
    HAS_DOMESTIC = False

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
# Computed at runtime in main() to avoid stale module-load values
NOW_KST = None
TODAY = None
IS_MONDAY = False
AGE_HOURS = 36

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("nydad-bot")

HEADERS = {"User-Agent": "NydadBot/2.0 (github.com/nydad/nydad-bot)"}

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

# Correlation pair tickers for domestic analysis
CORRELATION_PAIRS = [
    {"us": "NVDA", "kr": "005930.KS", "label_us": "NVDA", "label_kr": "삼성전자"},
    {"us": "MU", "kr": "000660.KS", "label_us": "MU", "label_kr": "SK하이닉스"},
    {"us": "AAPL", "kr": "066570.KS", "label_us": "AAPL", "label_kr": "LG전자"},
    {"us": "TSLA", "kr": "373220.KS", "label_us": "TSLA", "label_kr": "LG에너지솔루션"},
]

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
    {"name": "AI Frontier", "url": "https://aifrontier.kr/rss.xml"},
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
    {"name": "AI Frontier", "url": "https://aifrontier.kr/rss.xml"},
]

KBO_FEEDS = [
    {"name": "스포츠조선", "url": "https://sports.chosun.com/rss/baseball.xml"},
    {"name": "스포탈코리아", "url": "https://www.sportalkorea.com/rss/baseball.xml"},
    {"name": "엠스플뉴스", "url": "https://www.msn.com/ko-kr/sports/kbo/feed"},
    {"name": "OSEN 야구", "url": "https://www.osen.co.kr/rss/baseball.xml"},
    {"name": "MK스포츠", "url": "https://www.mk.co.kr/rss/50400001/"},
]


# ===========================================================================
# Phase 0: Market Data
# ===========================================================================
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
                if math.isnan(cur):
                    continue
                prev = float(close.iloc[-2]) if len(close) >= 2 else cur
                if math.isnan(prev):
                    prev = cur
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


# ===========================================================================
# Phase 1: Correlation + Foreign Flow + Investment Signal
# ===========================================================================

def _calculate_correlations_builtin(market: dict) -> list[dict]:
    """Built-in fallback: compute correlations via yfinance if domestic_analysis
    module is not available. Returns list of correlation pair dicts."""
    if not yf:
        return []
    correlations = []
    for pair in CORRELATION_PAIRS:
        try:
            df = yf.download([pair["us"], pair["kr"]], period="30d", interval="1d",
                             progress=False, threads=True, timeout=20)
            if df.empty:
                continue
            close_us = df["Close"][pair["us"]].dropna()
            close_kr = df["Close"][pair["kr"]].dropna()
            # Align dates
            idx = close_us.index.intersection(close_kr.index)
            if len(idx) < 5:
                continue
            us_ret = close_us.loc[idx].pct_change().dropna()
            kr_ret = close_kr.loc[idx].pct_change().dropna()
            # Align again after pct_change
            common = us_ret.index.intersection(kr_ret.index)
            if len(common) < 5:
                continue
            us_vals = us_ret.loc[common]
            kr_vals = kr_ret.loc[common]
            # Manual Pearson correlation (avoid numpy dependency)
            n = len(common)
            mean_us = sum(us_vals) / n
            mean_kr = sum(kr_vals) / n
            cov = sum((u - mean_us) * (k - mean_kr) for u, k in zip(us_vals, kr_vals)) / n
            std_us = (sum((u - mean_us) ** 2 for u in us_vals) / n) ** 0.5
            std_kr = (sum((k - mean_kr) ** 2 for k in kr_vals) / n) ** 0.5
            corr = cov / (std_us * std_kr) if std_us > 0 and std_kr > 0 else 0
            correlations.append({
                "us_ticker": pair["label_us"],
                "kr_ticker": pair["label_kr"],
                "coefficient": round(corr, 3),
                "period_days": len(common),
                "interpretation": (
                    "강한 양의 상관" if corr > 0.7 else
                    "양의 상관" if corr > 0.3 else
                    "약한 상관" if corr > -0.3 else
                    "음의 상관" if corr > -0.7 else
                    "강한 음의 상관"
                ),
            })
        except Exception as e:
            log.warning("  Correlation %s-%s: %s", pair["label_us"], pair["label_kr"], e)
    return correlations


def _fetch_foreign_flow_builtin() -> dict:
    """Built-in fallback: fetch foreign investor flow from KRX/proxy APIs.
    Returns a dict with flow data or empty dict on failure."""
    flow = {}
    # Try to get foreign investor net buy/sell from a public proxy
    try:
        # KRX provides foreign investor data; we use a lightweight proxy approach
        r = requests.get(
            "https://data.krx.co.kr/comm/bldAttend498/getJsonData.cmd",
            params={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT00703",
                "trdDd": datetime.now(KST).strftime("%Y%m%d"),
            },
            headers={
                **HEADERS,
                "Referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd",
            },
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            items = data.get("output", data.get("OutBlock_1", []))
            if items and isinstance(items, list):
                for item in items[:5]:
                    name = item.get("ISU_ABBRV", item.get("ISU_NM", ""))
                    net_buy = item.get("FRGN_NET_BUY_QTY", item.get("FORN_NET_BUY_QTY", "0"))
                    if name:
                        flow[name] = {
                            "net_buy_qty": int(str(net_buy).replace(",", "") or 0),
                        }
    except Exception as e:
        log.warning("Foreign flow (KRX): %s", e)

    if not flow:
        # Return a placeholder structure so the pipeline can continue
        flow = {"status": "unavailable", "note": "외국인 수급 데이터 수집 실패 — KRX API 접근 불가"}

    return flow


# Geopolitical risk keywords for scanning news headlines
GEO_RISK_KEYWORDS = [
    # War / Military conflict
    "war", "military", "strike", "attack", "missile", "bomb", "invasion",
    "conflict", "troops", "army", "navy", "airforce", "escalat",
    "전쟁", "군사", "공격", "미사일", "폭격", "침공", "공습", "확전",
    # Iran specifically
    "iran", "tehran", "strait of hormuz", "persian gulf", "irgc",
    "이란", "호르무즈", "페르시아만",
    # Middle East / geopolitics
    "middle east", "israel", "hamas", "hezbollah", "syria", "yemen", "houthi",
    "중동", "이스라엘", "하마스", "헤즈볼라", "시리아", "예멘", "후티",
    # US-China / North Korea / Taiwan
    "taiwan strait", "south china sea", "north korea", "icbm", "nuclear",
    "대만", "남중국해", "북한", "핵",
    # Russia / Ukraine
    "russia", "ukraine", "nato", "러시아", "우크라이나", "나토",
    # Trade war / sanctions
    "sanction", "tariff", "trade war", "embargo", "blacklist",
    "제재", "관세", "무역전쟁", "금수",
]


def _scan_geopolitical_risk(articles: list[dict]) -> dict:
    """Scan news headlines for geopolitical risk signals."""
    risk_hits = []
    for a in articles:
        text = (a.get("title", "") + " " + a.get("description", "")).lower()
        for kw in GEO_RISK_KEYWORDS:
            if kw in text:
                risk_hits.append({"keyword": kw, "title": a["title"][:80], "source": a["source"]})
                break  # one hit per article

    risk_level = "low"
    if len(risk_hits) >= 8:
        risk_level = "critical"
    elif len(risk_hits) >= 5:
        risk_level = "high"
    elif len(risk_hits) >= 2:
        risk_level = "elevated"

    return {
        "level": risk_level,
        "hit_count": len(risk_hits),
        "top_hits": risk_hits[:5],
    }


def calculate_investment_signal(market: dict, invest_articles: list[dict] = None,
                                correlations: list[dict] = None,
                                foreign_flow: dict = None) -> dict:
    """Multi-factor composite long/short signal.

    10 base factors + correlation/flow enrichment:
      1. VIX (공포지수)
      2. US Futures (S&P 선물)
      3. USD/KRW (환율)
      4. S&P 500 종가
      5. KOSPI 추세
      6. WTI 유가 변동
      7. 금 가격 (안전자산)
      8. 필라델피아 반도체 지수 (한국 수출 프록시)
      9. 달러인덱스 (글로벌 유동성)
     10. 지정학 리스크 (뉴스 헤드라인 스캔)
    """
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

    # 6. WTI Oil
    wti = find("commodities", "WTI")
    if wti:
        if wti["change_pct"] > 2.0:
            factors.append({"name": "유가 급등 (지정학 우려)", "signal": "bearish",
                            "detail": f"WTI ${wti['price']:.1f} ({wti['change_pct']:+.1f}%)"})
        elif wti["change_pct"] > 0.5:
            factors.append({"name": "유가 상승", "signal": "neutral",
                            "detail": f"WTI ${wti['price']:.1f} ({wti['change_pct']:+.1f}%)"})
        elif wti["change_pct"] < -2.0:
            factors.append({"name": "유가 급락 (수요 우려)", "signal": "bearish",
                            "detail": f"WTI ${wti['price']:.1f} ({wti['change_pct']:+.1f}%)"})
        elif wti["change_pct"] < -0.5:
            factors.append({"name": "유가 하락 (비용 완화)", "signal": "bullish",
                            "detail": f"WTI ${wti['price']:.1f} ({wti['change_pct']:+.1f}%)"})
        else:
            factors.append({"name": "유가 안정", "signal": "neutral",
                            "detail": f"WTI ${wti['price']:.1f}"})

    # 7. Gold
    gold = find("commodities", "금")
    if gold:
        if gold["change_pct"] > 1.0:
            factors.append({"name": "금 급등 (안전자산 선호)", "signal": "bearish",
                            "detail": f"Gold ${gold['price']:.0f} ({gold['change_pct']:+.1f}%)"})
        elif gold["change_pct"] < -1.0:
            factors.append({"name": "금 하락 (위험자산 선호)", "signal": "bullish",
                            "detail": f"Gold ${gold['price']:.0f} ({gold['change_pct']:+.1f}%)"})
        else:
            factors.append({"name": "금 보합", "signal": "neutral",
                            "detail": f"Gold ${gold['price']:.0f}"})

    # 8. Philadelphia Semiconductor (SOX)
    sox = find("us_indices", "반도체")
    if sox:
        if sox["change_pct"] > 0.5:
            factors.append({"name": "SOX 반도체 강세", "signal": "bullish", "detail": f"SOX {sox['change_pct']:+.2f}%"})
        elif sox["change_pct"] < -0.5:
            factors.append({"name": "SOX 반도체 약세", "signal": "bearish", "detail": f"SOX {sox['change_pct']:+.2f}%"})
        else:
            factors.append({"name": "SOX 반도체 보합", "signal": "neutral", "detail": f"SOX {sox['change_pct']:+.2f}%"})

    # 9. Dollar Index
    dxy = find("forex", "달러인덱스")
    if dxy:
        if dxy["change_pct"] > 0.3:
            factors.append({"name": "달러 강세 (EM 자금유출 우려)", "signal": "bearish",
                            "detail": f"DXY {dxy['price']:.1f} ({dxy['change_pct']:+.2f}%)"})
        elif dxy["change_pct"] < -0.3:
            factors.append({"name": "달러 약세 (EM 자금유입 기대)", "signal": "bullish",
                            "detail": f"DXY {dxy['price']:.1f} ({dxy['change_pct']:+.2f}%)"})
        else:
            factors.append({"name": "달러 보합", "signal": "neutral",
                            "detail": f"DXY {dxy['price']:.1f}"})

    # 10. Geopolitical risk
    geo_risk = {"level": "low", "hit_count": 0, "top_hits": []}
    if invest_articles:
        geo_risk = _scan_geopolitical_risk(invest_articles)
        if geo_risk["level"] == "critical":
            factors.append({"name": "지정학 리스크 심각", "signal": "bearish",
                            "detail": f"{geo_risk['hit_count']}건 위험 뉴스 감지"})
        elif geo_risk["level"] == "high":
            factors.append({"name": "지정학 리스크 높음", "signal": "bearish",
                            "detail": f"{geo_risk['hit_count']}건 위험 뉴스"})
        elif geo_risk["level"] == "elevated":
            factors.append({"name": "지정학 리스크 주의", "signal": "neutral",
                            "detail": f"{geo_risk['hit_count']}건 관련 뉴스"})
        else:
            factors.append({"name": "지정학 리스크 낮음", "signal": "bullish",
                            "detail": "주요 리스크 뉴스 없음"})

    if not factors:
        return {
            "direction": "neutral", "long_pct": 50, "short_pct": 50,
            "confidence": 0, "summary": "", "factors": [],
            "correlations": correlations or [], "foreign_flow": foreign_flow or {},
            "key_insight": "", "sectors": [], "geo_risk": geo_risk,
        }

    bull = sum(1 for f in factors if f["signal"] == "bullish")
    bear = sum(1 for f in factors if f["signal"] == "bearish")
    total = len(factors)

    # Direction logic with critical geo risk override
    if geo_risk["level"] == "critical" and bear >= 3:
        direction = "short"
    elif bull >= total * 0.6:
        direction = "long"
    elif bear >= total * 0.6:
        direction = "short"
    elif bull > bear:
        direction = "long"
    elif bear > bull:
        direction = "short"
    else:
        direction = "neutral"

    confidence = round(max(bull, bear) / total, 2) if total else 0

    # Compute long/short percentages
    if direction == "long":
        long_pct = round(50 + confidence * 30)
        short_pct = 100 - long_pct
    elif direction == "short":
        short_pct = round(50 + confidence * 30)
        long_pct = 100 - short_pct
    else:
        long_pct = 50
        short_pct = 50

    # Sector recommendations
    sectors = []
    oil_surge = any("유가 급등" in f["name"] for f in factors)
    oil_drop = any("유가 하락" in f["name"] or "비용 완화" in f["name"] for f in factors)
    geo_high = geo_risk["level"] in ("high", "critical")
    sox_bull = any("SOX" in f["name"] and f["signal"] == "bullish" for f in factors)
    krw_strong = any("원화 강세" in f["name"] for f in factors)
    krw_weak = any("원화 약세" in f["name"] for f in factors)
    gold_up = any("금 급등" in f["name"] for f in factors)

    if direction == "long":
        if sox_bull:
            sectors.append({"name": "반도체", "direction": "overweight",
                            "reason": "SOX 강세 연동 + 외국인 수급 유입 기대"})
        sectors.append({"name": "2차전지", "direction": "overweight", "reason": "성장주 랠리 환경"})
        if krw_strong:
            sectors.append({"name": "내수/유통", "direction": "overweight",
                            "reason": "원화 강세 시 내수 소비 수혜"})
        elif krw_weak:
            sectors.append({"name": "수출주/자동차", "direction": "overweight",
                            "reason": "원화 약세 시 수출 경쟁력 강화"})
        if oil_drop:
            sectors.append({"name": "항공/운송", "direction": "overweight",
                            "reason": "유가 하락 시 비용 절감 수혜"})
    elif direction == "short":
        sectors.append({"name": "방어주/유틸리티", "direction": "overweight",
                        "reason": "하락장 방어 + 배당 매력"})
        sectors.append({"name": "통신", "direction": "overweight", "reason": "변동성 장세 방어 섹터"})
        if oil_surge or geo_high:
            sectors.append({"name": "에너지/정유", "direction": "overweight",
                            "reason": "유가 상승 수혜 + 지정학 프리미엄"})
            sectors.append({"name": "방산", "direction": "overweight",
                            "reason": "지정학 긴장 시 방산주 수혜"})
        if gold_up:
            sectors.append({"name": "금 ETF", "direction": "overweight",
                            "reason": "안전자산 랠리 지속 기대"})
        if not oil_surge and not geo_high and not gold_up:
            sectors.append({"name": "금/원자재 ETF", "direction": "overweight",
                            "reason": "안전자산 선호 구간"})
    else:
        sectors = [
            {"name": "배당주", "direction": "neutral", "reason": "박스권 장세에서 배당 수익 확보"},
            {"name": "바이오", "direction": "neutral", "reason": "개별 모멘텀 중심 접근"},
        ]

    # Build a concise key_insight from correlations + flow
    key_insight_parts = []
    if correlations:
        for c in correlations[:2]:
            key_insight_parts.append(
                f"{c['us_ticker']}↔{c['kr_ticker']} 상관계수 {c['coefficient']:.2f}"
            )
    if foreign_flow and foreign_flow.get("status") != "unavailable":
        flow_items = [k for k in foreign_flow if k not in ("status", "note")]
        if flow_items:
            key_insight_parts.append(f"외국인 주요 종목 수급 {len(flow_items)}건 추적 중")

    key_insight = " | ".join(key_insight_parts) if key_insight_parts else ""

    # Build summary
    bull_names = [f["name"] for f in factors if f["signal"] == "bullish"]
    bear_names = [f["name"] for f in factors if f["signal"] == "bearish"]
    summary_parts = []
    if bull_names:
        summary_parts.append(f"호재: {', '.join(bull_names[:3])}")
    if bear_names:
        summary_parts.append(f"악재: {', '.join(bear_names[:3])}")
    summary = f"{direction.upper()} ({confidence:.0%} 신뢰도). " + ". ".join(summary_parts)

    return {
        "direction": direction,
        "long_pct": long_pct,
        "short_pct": short_pct,
        "confidence": confidence,
        "summary": summary,
        "factors": factors,
        "correlations": correlations or [],
        "foreign_flow": foreign_flow or {},
        "key_insight": key_insight,
        "sectors": sectors,
        "geo_risk": geo_risk,
    }


# ===========================================================================
# Phase 2: KBO Data
# ===========================================================================
def fetch_kbo_data() -> dict:
    """Collect KBO standings, today's games, and news.
    Uses kbo_collect module if available, otherwise returns empty structure."""
    log.info("=== Phase 2: KBO Data ===")
    kbo = {"standings": [], "games_today": [], "news": []}

    if HAS_KBO:
        try:
            kbo["standings"] = fetch_kbo_standings()
            log.info("  KBO standings: %d teams", len(kbo["standings"]))
        except Exception as e:
            log.warning("  KBO standings failed: %s", e)

        try:
            kbo["games_today"] = fetch_kbo_games_today()
            log.info("  KBO games today: %d", len(kbo["games_today"]))
        except Exception as e:
            log.warning("  KBO games today failed: %s", e)

        try:
            kbo["news"] = fetch_kbo_news()
            log.info("  KBO news: %d articles", len(kbo["news"]))
        except Exception as e:
            log.warning("  KBO news failed: %s", e)
    else:
        log.info("  kbo_collect module not available — skipping structured KBO data")

    return kbo


# ===========================================================================
# Phase 3: News Collection (RSS)
# ===========================================================================
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
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_rss, f, cutoff): f for f in feeds}
        for future in as_completed(futures):
            try:
                articles.extend(future.result())
            except Exception as e:
                log.warning("Feed failed: %s", e)
    # deduplicate
    seen = set()
    unique = []
    for a in articles:
        key = hashlib.sha256(a["url"].lower().split("?")[0].rstrip("/").encode()).hexdigest()[:16]
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
        dl = trafilatura.fetch_url(url, no_ssl=True)
        if dl and len(dl) < 500_000:
            t = trafilatura.extract(dl, include_comments=False, include_tables=False, deduplicate=True)
            if t:
                return t[:2500]
    except Exception:
        pass
    return ""


# ===========================================================================
# OpenRouter API
# ===========================================================================
def _call_api(model, system, user, max_tokens=4096):
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/nydad/nydad-bot",
        "X-Title": "Nydad Bot",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.2,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }
    for attempt in range(1, RETRY + 2):  # 3 total attempts
        try:
            resp = requests.post("https://openrouter.ai/api/v1/chat/completions",
                                 headers=headers, json=payload, timeout=180)
            # Handle rate limits
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 10))
                log.warning("Rate limited (429), waiting %ds...", retry_after)
                time.sleep(min(retry_after, 60))
                continue
            if resp.status_code >= 500:
                delay = RETRY_DELAY * (2 ** (attempt - 1))
                log.warning("Server error %d, retrying in %ds...", resp.status_code, delay)
                time.sleep(delay)
                continue
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
        except json.JSONDecodeError as e:
            log.warning("JSON parse error (attempt %d): %s", attempt, e)
        except Exception as e:
            log.warning("API fail (attempt %d): %s", attempt, e)
        if attempt <= RETRY:
            time.sleep(RETRY_DELAY * (2 ** (attempt - 1)))
    return None


# ===========================================================================
# Phase 4: Batch Summarization (Gemini Flash)
# ===========================================================================
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

    "kbo": """You are a Korean baseball news analyst. For EACH article, provide:
- "summary": 2-3 sentence Korean summary. Keep player names and stats in original form.
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
        user = f"Summarize {len(batch)} articles.\n\n" + "\n".join(parts)
        ai = _call_api(MODEL_FAST, SUMMARY_PROMPTS.get(tab_id, SUMMARY_PROMPTS["invest"]), user)
        ai_list = ai.get("articles", []) if ai else []
        # Validate response shape
        ai_list = [item for item in ai_list if isinstance(item, dict) and "summary" in item]
        for j, article in enumerate(batch):
            if j < len(ai_list):
                item = ai_list[j]
                enriched.append({
                    **article,
                    "summary": item.get("summary", article.get("description", "")[:300]),
                    "importance": item.get("importance", "medium"),
                    "tags": item.get("tags", []),
                })
            else:
                enriched.append({
                    **article,
                    "summary": article.get("description", "")[:300],
                    "importance": "medium",
                    "tags": [],
                })
    imp = {"high": 0, "medium": 1, "low": 2}
    enriched.sort(key=lambda a: imp.get(a.get("importance"), 2))
    return enriched


# ===========================================================================
# Phase 5: Editorial Generation (Claude Sonnet)
# ===========================================================================

EDITORIAL_INVEST = """You are a senior financial analyst writing a morning briefing for Korean investors.

Given market data + investment signal + correlation data + foreign flow + news, generate IN KOREAN:

1. "briefing": 6-8 sentences. Professional market overview covering US close, futures, key drivers.
   반드시 구체적 방향성과 수치를 제시. 뻔한 말 금지.
   Include correlation insights (e.g., NVDA↔삼성 상관계수 0.85 → 삼성전자 동반 상승 예상).
   Include foreign investor flow if available.
   {monday_note}
2. "key_insights": 3-5 items, each: {{"title":"...(max 20 chars)", "detail":"1-2 sentences", "type":"bullish/bearish/neutral/alert"}}
3. "correlations": Summarize the US-KR stock correlation data into 2-3 bullet points.
   Each: {{"pair":"NVDA↔삼성전자", "coefficient":0.85, "implication":"1 sentence Korean"}}
4. "foreign_flow": 2-3 sentences about foreign investor flow trends and their implications.
5. "forex_commentary": 2-3 sentences about USD/KRW.
6. "commodity_commentary": 2-3 sentences about oil/gold.
7. "outlook": 2-3 sentences. Today's outlook + key events.
   반드시 구체적 방향성과 수치를 제시. 뻔한 말 금지.
8. "sector_analysis": Validate/refine the provided sector recommendations. Return 3 items: {{"name":"sector", "direction":"overweight/underweight/neutral", "reason":"1 sentence"}}
9. "trends": 3-5 short Korean keywords.

Return JSON with all 9 fields. Korean text, keep numbers/tickers/names in English.
핵심 원칙: 뻔한 관망론 금지. 구체적 수치, 종목명, 방향성을 반드시 포함."""

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

EDITORIAL_KBO = """You are a Korean baseball analyst writing daily briefings in KOREAN.

Given KBO standings, today's games, and news articles, generate:
1. "briefing": 4-6 sentences covering key games, standings changes, notable performances.
   Include specific scores, player stats, and team movements where available.
2. "trends": 2-3 Korean trend keywords (e.g., "삼성 연승가도", "SSG 투수력 부활").

Return JSON with "briefing" (string) and "trends" (list of strings). Korean text."""


def generate_invest_editorial(market, signal, fg, articles, correlations, foreign_flow):
    monday = "Include weekend recap in briefing." if IS_MONDAY else ""
    prompt = EDITORIAL_INVEST.replace("{monday_note}", monday)

    parts = ["=== MARKET DATA ==="]
    for cat, items in market.items():
        if items:
            parts.append(f"[{cat}]")
            for i in items:
                s = "+" if i["change"] >= 0 else ""
                parts.append(f"  {i['name']}: {i['price']} ({s}{i['change']}, {s}{i['change_pct']}%)")

    parts.append(f"\n=== INVESTMENT SIGNAL: {signal['direction'].upper()} "
                 f"(conf: {signal['confidence']}, long {signal['long_pct']}% / short {signal['short_pct']}%) ===")
    for f in signal["factors"]:
        parts.append(f"  {f['name']}: {f['signal']} ({f['detail']})")
    parts.append("Sectors: " + ", ".join(s["name"] for s in signal.get("sectors", [])))

    # Correlation data
    if correlations:
        parts.append("\n=== CORRELATION DATA (30-day rolling) ===")
        for c in correlations:
            parts.append(f"  {c['us_ticker']} ↔ {c['kr_ticker']}: "
                         f"r={c['coefficient']:.3f} ({c['interpretation']}), "
                         f"period={c['period_days']}d")

    # Foreign flow data
    if foreign_flow and foreign_flow.get("status") != "unavailable":
        parts.append("\n=== FOREIGN INVESTOR FLOW ===")
        for name, data in foreign_flow.items():
            if name in ("status", "note"):
                continue
            if isinstance(data, dict):
                net = data.get("net_buy_qty", 0)
                direction = "순매수" if net > 0 else "순매도"
                parts.append(f"  {name}: {direction} {abs(net):,}주")
    elif foreign_flow:
        parts.append(f"\n=== FOREIGN FLOW: {foreign_flow.get('note', 'N/A')} ===")

    if fg.get("us"):
        parts.append(f"\n=== US F&G: {fg['us']['score']} ({fg['us']['rating']}) ===")

    parts.append("\n=== NEWS ===")
    for a in articles[:20]:
        parts.append(f"  [{a['source']}] {a['title']}\n    {a.get('summary', '')[:200]}")

    user = f"Today: {NOW_KST.strftime('%Y-%m-%d %A')}\nWindow: {AGE_HOURS}h\n\n" + "\n".join(parts)
    return _call_api(MODEL_QUALITY, prompt, user, 4000) or {}


def generate_editorial(prompt, articles, extra_context=""):
    parts = []
    if extra_context:
        parts.append(extra_context)
    parts.append("=== ARTICLES ===")
    for a in articles[:20]:
        parts.append(f"[{a['source']}] {a['title']}\n  {a.get('summary', '')[:200]}")
    user = f"Today: {NOW_KST.strftime('%Y-%m-%d %A')}\n{len(articles)} articles\n\n" + "\n".join(parts)
    return _call_api(MODEL_QUALITY, prompt, user, 2500) or {}


def generate_kbo_editorial(kbo_data, kbo_articles):
    """Generate KBO editorial using standings, games, and news."""
    parts = []

    if kbo_data.get("standings"):
        parts.append("=== KBO STANDINGS ===")
        for team in kbo_data["standings"]:
            if isinstance(team, dict):
                parts.append(f"  {team.get('rank', '?')}. {team.get('team', '?')} "
                             f"- {team.get('wins', 0)}W {team.get('losses', 0)}L "
                             f"({team.get('pct', '?')})")

    if kbo_data.get("games_today"):
        parts.append("\n=== TODAY'S GAMES ===")
        for game in kbo_data["games_today"]:
            if isinstance(game, dict):
                parts.append(f"  {game.get('away', '?')} vs {game.get('home', '?')} "
                             f"({game.get('time', '?')}) "
                             f"— {game.get('status', 'scheduled')}")
                if game.get("score"):
                    parts.append(f"    Score: {game['score']}")

    if kbo_articles:
        parts.append("\n=== KBO NEWS ===")
        for a in kbo_articles[:15]:
            parts.append(f"  [{a['source']}] {a['title']}\n    {a.get('summary', '')[:200]}")

    if not parts:
        return {}

    user = f"Today: {NOW_KST.strftime('%Y-%m-%d %A')}\n\n" + "\n".join(parts)
    return _call_api(MODEL_QUALITY, EDITORIAL_KBO, user, 2000) or {}


# ===========================================================================
# Phase 6: Build Digest + Output
# ===========================================================================
def article_out(a):
    return {
        "title": a["title"], "url": a["url"], "source": a["source"],
        "published": a["published"], "summary": a.get("summary", ""),
        "importance": a.get("importance", "medium"), "tags": a.get("tags", []),
    }


def build_digest():
    # ------------------------------------------------------------------
    # Phase 0: Market Data + Crypto + Fear & Greed
    # ------------------------------------------------------------------
    log.info("=" * 50)
    log.info("Phase 0: Market Data + Crypto + F&G")
    log.info("=" * 50)
    market = fetch_market_data()
    crypto_prices = fetch_crypto_prices()
    fg = fetch_fear_greed()

    # ------------------------------------------------------------------
    # Phase 1: Correlation + Foreign Flow + Investment Signal
    # ------------------------------------------------------------------
    log.info("=" * 50)
    log.info("Phase 1: Correlation + Foreign Flow + Signal")
    log.info("=" * 50)

    # Fetch invest news first (needed for geo risk scan in signal)
    log.info("--- Fetching invest news for geo risk scan ---")
    invest_raw = fetch_tab_feeds(INVEST_FEEDS, "invest")

    # Correlations
    log.info("--- Computing correlations ---")
    if HAS_DOMESTIC:
        try:
            correlations = calculate_correlations(CORRELATION_PAIRS)
            log.info("  Correlations (domestic_analysis): %d pairs", len(correlations))
        except Exception as e:
            log.warning("  domestic_analysis.calculate_correlations failed: %s", e)
            correlations = _calculate_correlations_builtin(market)
    else:
        correlations = _calculate_correlations_builtin(market)
    log.info("  Correlations: %d pairs computed", len(correlations))
    for c in correlations:
        log.info("    %s ↔ %s: r=%.3f (%s)",
                 c["us_ticker"], c["kr_ticker"], c["coefficient"], c["interpretation"])

    # Foreign flow
    log.info("--- Fetching foreign investor flow ---")
    if HAS_DOMESTIC:
        try:
            foreign_flow = fetch_foreign_flow()
            log.info("  Foreign flow (domestic_analysis): OK")
        except Exception as e:
            log.warning("  domestic_analysis.fetch_foreign_flow failed: %s", e)
            foreign_flow = _fetch_foreign_flow_builtin()
    else:
        foreign_flow = _fetch_foreign_flow_builtin()

    # Investment signal (composite)
    log.info("--- Computing investment signal ---")
    signal = calculate_investment_signal(market, invest_raw, correlations, foreign_flow)
    log.info("  Direction: %s (conf: %s, long %d%% / short %d%%)",
             signal["direction"], signal["confidence"],
             signal["long_pct"], signal["short_pct"])
    log.info("  Geo risk: %s (%d hits)",
             signal.get("geo_risk", {}).get("level", "?"),
             signal.get("geo_risk", {}).get("hit_count", 0))
    if signal.get("key_insight"):
        log.info("  Key insight: %s", signal["key_insight"])

    # ------------------------------------------------------------------
    # Phase 2: KBO Data
    # ------------------------------------------------------------------
    log.info("=" * 50)
    log.info("Phase 2: KBO Data")
    log.info("=" * 50)
    kbo_data = fetch_kbo_data()

    # ------------------------------------------------------------------
    # Phase 3: News Collection (all 5 tabs)
    # ------------------------------------------------------------------
    log.info("=" * 50)
    log.info("Phase 3: News Collection")
    log.info("=" * 50)
    # invest_raw already fetched above for geo risk scan
    ai_raw = fetch_tab_feeds(AI_INDUSTRY_FEEDS, "ai_industry")
    crypto_raw = fetch_tab_feeds(CRYPTO_FEEDS, "crypto")
    dev_raw = fetch_tab_feeds(AI_DEV_FEEDS, "ai_dev")
    kbo_raw = fetch_tab_feeds(KBO_FEEDS, "kbo")

    # Merge KBO news from structured source + RSS
    if kbo_data.get("news"):
        # Deduplicate: KBO module news + RSS feeds
        existing_urls = {a["url"].lower().split("?")[0].rstrip("/") for a in kbo_raw}
        for article in kbo_data["news"]:
            url_key = article.get("url", "").lower().split("?")[0].rstrip("/")
            if url_key and url_key not in existing_urls:
                kbo_raw.append(article)
                existing_urls.add(url_key)
        log.info("  KBO merged total: %d articles", len(kbo_raw))

    # ------------------------------------------------------------------
    # Phase 4: Batch Summarization (Gemini Flash)
    # ------------------------------------------------------------------
    log.info("=" * 50)
    log.info("Phase 4: Summarization (%s)", MODEL_FAST)
    log.info("=" * 50)
    log.info("--- invest ---")
    invest_articles = summarize_tab(invest_raw, "invest")
    log.info("--- ai_industry ---")
    ai_articles = summarize_tab(ai_raw, "ai_industry")
    log.info("--- crypto ---")
    crypto_articles = summarize_tab(crypto_raw, "crypto")
    log.info("--- ai_dev ---")
    dev_articles = summarize_tab(dev_raw, "ai_dev")
    log.info("--- kbo ---")
    kbo_articles = summarize_tab(kbo_raw, "kbo")

    total = (len(invest_articles) + len(ai_articles) + len(crypto_articles)
             + len(dev_articles) + len(kbo_articles))
    log.info("Phase 4 done: %d total articles", total)

    # ------------------------------------------------------------------
    # Phase 5: Editorial Generation (Claude Sonnet) — 5 tabs
    # ------------------------------------------------------------------
    log.info("=" * 50)
    log.info("Phase 5: Editorial (%s)", MODEL_QUALITY)
    log.info("=" * 50)

    crypto_ctx = ""
    if crypto_prices:
        lines = ["=== CRYPTO PRICES ==="]
        for c in crypto_prices[:10]:
            lines.append(f"  #{c['rank']} {c['symbol']}: ${c['price']:,.2f} ({c['change_pct']:+.2f}%)")
        if fg.get("crypto"):
            lines.append(f"\nCrypto F&G: {fg['crypto']['score']} ({fg['crypto']['rating']})")
        crypto_ctx = "\n".join(lines)

    log.info("--- editorial generation (parallel) ---")
    with ThreadPoolExecutor(max_workers=5) as executor:
        invest_ed_future = executor.submit(generate_invest_editorial, market, signal, fg, invest_articles,
                                           correlations, foreign_flow)
        ai_ed_future = executor.submit(generate_editorial, EDITORIAL_AI, ai_articles)
        crypto_ed_future = executor.submit(generate_editorial, EDITORIAL_CRYPTO, crypto_articles, crypto_ctx)
        dev_ed_future = executor.submit(generate_editorial, EDITORIAL_DEV, dev_articles)
        kbo_ed_future = executor.submit(generate_kbo_editorial, kbo_data, kbo_articles)

    invest_ed = invest_ed_future.result() or {}
    ai_ed = ai_ed_future.result() or {}
    crypto_ed = crypto_ed_future.result() or {}
    dev_ed = dev_ed_future.result() or {}
    kbo_ed = kbo_ed_future.result() or {}

    # Merge sector analysis from AI editorial with signal sectors
    if invest_ed.get("sector_analysis"):
        signal["sectors"] = invest_ed["sector_analysis"]

    # ------------------------------------------------------------------
    # Phase 5.5: Previous Signal Review (오답노트)
    # ------------------------------------------------------------------
    prev_review = None
    try:
        yesterday = (NOW_KST - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_file = DATA_DIR / f"{yesterday}.json"
        if prev_file.exists():
            with open(prev_file, "r", encoding="utf-8") as pf:
                prev_data = json.load(pf)
            prev_sig = prev_data.get("investment_signal", prev_data.get("kospi_signal", {}))
            prev_dir = prev_sig.get("direction", "")
            # Check actual KOSPI result
            kospi_items = market.get("kr_indices", [])
            kospi_actual = None
            for item in kospi_items:
                if "KOSPI" in item["name"] and "200" not in item["name"]:
                    kospi_actual = item
                    break
            if prev_dir and kospi_actual:
                actual_change = kospi_actual.get("change_pct", 0)
                actual_dir = "long" if actual_change > 0 else "short"
                correct = prev_dir == actual_dir
                predicted_str = f"{prev_dir.upper()} {prev_sig.get('long_pct', '?')}% / {prev_sig.get('short_pct', '?')}%"
                actual_str = f"KOSPI {actual_change:+.2f}% ({'상승' if actual_change > 0 else '하락'})"

                # Generate reason if wrong
                reason = ""
                if not correct:
                    reason_parts = []
                    if prev_dir == "long" and actual_change < 0:
                        reason_parts.append("롱 예측이었으나 실제 하락")
                    elif prev_dir == "short" and actual_change > 0:
                        reason_parts.append("숏 예측이었으나 실제 상승")
                    # Check what factors might have been wrong
                    for f in prev_sig.get("factors", []):
                        if f.get("signal") == ("bullish" if prev_dir == "long" else "bearish"):
                            reason_parts.append(f"'{f['name']}' 시그널이 기대와 다르게 작용")
                            break
                    reason = ". ".join(reason_parts) if reason_parts else "예상과 반대 방향으로 시장 전개"
                else:
                    reason = "예측 방향과 실제 방향 일치. 인사이트가 유효했음."

                prev_review = {
                    "date": yesterday,
                    "predicted": predicted_str,
                    "actual": actual_str,
                    "correct": correct,
                    "reason": reason,
                }
                log.info("  Previous signal review: %s -> %s (%s)",
                         predicted_str, actual_str, "CORRECT" if correct else "WRONG")
    except Exception as e:
        log.warning("  Previous signal review failed: %s", e)

    # ------------------------------------------------------------------
    # Phase 6: Build JSON
    # ------------------------------------------------------------------
    log.info("=" * 50)
    log.info("Phase 6: Build JSON")
    log.info("=" * 50)

    result = {
        "date": TODAY,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "market_data": market,
        "crypto_prices": crypto_prices,
        "fear_greed": fg,
        "investment_signal": {
            "direction": signal["direction"],
            "long_pct": signal["long_pct"],
            "short_pct": signal["short_pct"],
            "confidence": signal["confidence"],
            "summary": signal.get("summary", ""),
            "factors": signal["factors"],
            "correlations": signal.get("correlations", []),
            "foreign_flow": signal.get("foreign_flow", {}),
            "key_insight": signal.get("key_insight", ""),
        },
        "total_articles": total,
        "tabs": {
            "invest": {
                "briefing": invest_ed.get("briefing", ""),
                "key_insights": invest_ed.get("key_insights", []),
                "correlations": invest_ed.get("correlations", []),
                "foreign_flow": invest_ed.get("foreign_flow", ""),
                "forex_commentary": invest_ed.get("forex_commentary", ""),
                "commodity_commentary": invest_ed.get("commodity_commentary", ""),
                "outlook": invest_ed.get("outlook", ""),
                "trends": invest_ed.get("trends", []),
                "articles": [article_out(a) for a in invest_articles],
            },
            "crypto": {
                "briefing": crypto_ed.get("briefing", ""),
                "key_events": crypto_ed.get("key_events", []),
                "trends": crypto_ed.get("trends", []),
                "articles": [article_out(a) for a in crypto_articles],
            },
            "ai_industry": {
                "briefing": ai_ed.get("briefing", ""),
                "quotes": ai_ed.get("quotes", []),
                "trends": ai_ed.get("trends", []),
                "articles": [article_out(a) for a in ai_articles],
            },
            "ai_dev": {
                "briefing": dev_ed.get("briefing", ""),
                "highlights": dev_ed.get("highlights", []),
                "trends": dev_ed.get("trends", []),
                "articles": [article_out(a) for a in dev_articles],
            },
            "kbo": {
                "standings": kbo_data.get("standings", []),
                "games_today": kbo_data.get("games_today", []),
                "briefing": kbo_ed.get("briefing", ""),
                "trends": kbo_ed.get("trends", []),
                "articles": [article_out(a) for a in kbo_articles],
            },
        },
    }

    # Add previous signal review if available
    if prev_review:
        result["prev_signal_review"] = prev_review

    return result


def update_index():
    dates = sorted(
        [f.stem for f in DATA_DIR.glob("*.json") if f.stem not in ("index", "live")],
        reverse=True,
    )
    with open(DATA_DIR / "index.json", "w", encoding="utf-8") as f:
        json.dump({"dates": dates, "latest": dates[0] if dates else None}, f, indent=2)
    log.info("Index: %d dates", len(dates))


def cleanup_old_data(keep_days=30):
    """Remove data files older than keep_days."""
    cutoff = datetime.now(KST) - timedelta(days=keep_days)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    removed = 0
    for f in DATA_DIR.glob("*.json"):
        if f.stem not in ("index", "live") and f.stem < cutoff_str:
            f.unlink()
            removed += 1
    if removed:
        log.info("Cleaned up %d old data files (>%d days)", removed, keep_days)


# ===========================================================================
# Main
# ===========================================================================
def main():
    global NOW_KST, TODAY, IS_MONDAY, AGE_HOURS

    if not API_KEY:
        log.error("OPENROUTER_API_KEY not set!")
        sys.exit(1)

    # Compute time at runtime (not module load)
    NOW_KST = datetime.now(KST)
    TODAY = NOW_KST.strftime("%Y-%m-%d")
    IS_MONDAY = NOW_KST.weekday() == 0
    AGE_HOURS = 72 if IS_MONDAY else 36

    log.info("=" * 60)
    log.info("nydad-bot Unified Digest v2.0 (5-tab)")
    log.info("Fast: %s | Quality: %s", MODEL_FAST, MODEL_QUALITY)
    log.info("Date: %s (KST) | Window: %dh%s", TODAY, AGE_HOURS,
             " [MONDAY WEEKEND RECAP]" if IS_MONDAY else "")
    log.info("Modules: KBO=%s | Domestic=%s", HAS_KBO, HAS_DOMESTIC)
    log.info("=" * 60)

    digest = build_digest()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{digest['date']}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(digest, f, ensure_ascii=False, indent=2)
    log.info("Saved: %s (%d articles)", path.name, digest["total_articles"])

    update_index()
    cleanup_old_data(keep_days=30)

    log.info("=" * 60)
    log.info("Done! 5 tabs: invest(%d) crypto(%d) ai_industry(%d) ai_dev(%d) kbo(%d)",
             len(digest["tabs"]["invest"]["articles"]),
             len(digest["tabs"]["crypto"]["articles"]),
             len(digest["tabs"]["ai_industry"]["articles"]),
             len(digest["tabs"]["ai_dev"]["articles"]),
             len(digest["tabs"]["kbo"]["articles"]))


if __name__ == "__main__":
    main()
