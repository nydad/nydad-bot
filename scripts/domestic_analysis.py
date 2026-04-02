#!/usr/bin/env python3
"""
nydad-bot — AI-Driven Korean Market (KOSPI) Investment Analysis

Replaces the fixed 10-factor KOSPI signal with a dynamic, correlation-aware,
LLM-driven analysis pipeline:

  1. Correlation Analysis — 60-day rolling correlations between US semis and Korean stocks
  2. Foreign Investor Flow — KRX/Naver Finance scraping with fallback
  3. Dynamic Signal Generation — all data passed to LLM as structured context
  4. Non-Obvious Insights — hedge-fund-style directional call (never neutral)

Usage:
  Standalone:  python domestic_analysis.py
  Importable:  from domestic_analysis import generate_investment_insights
"""

import json
import logging
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
MODEL_QUALITY = os.environ.get("OPENROUTER_MODEL_QUALITY", "anthropic/claude-sonnet-4.6")
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

KST = timezone(timedelta(hours=9))
RETRY = 2
RETRY_DELAY = 5
HEADERS = {"User-Agent": "NydadBot/1.0 (github.com/nydad/nydad-bot)"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("domestic-analysis")

# ---------------------------------------------------------------------------
# Ticker Definitions for Correlation Analysis
# ---------------------------------------------------------------------------
CORRELATION_TICKERS = {
    # ═══ US Semiconductors / Memory (최우선 — 코스피 삼성/하이닉스 상관계수 가장 높음) ═══
    "NVDA": "NVIDIA",
    "MU": "Micron",
    "WDC": "Western Digital (SanDisk)",
    "AMAT": "Applied Materials",        # 반도체 장비 → 삼성/하이닉스 설비투자 연동
    "LRCX": "Lam Research",             # 반도체 장비
    # ═══ Korean Stocks — Core ═══
    "005930.KS": "Samsung Electronics",
    "000660.KS": "SK Hynix",
    # ═══ Korean Stocks — Sector Leaders ═══
    "373220.KS": "LG Energy Solution",
    "006400.KS": "Samsung SDI",
    "012450.KS": "Hanwha Aerospace",
    # ═══ Indices ═══
    "^SOX": "Philadelphia Semiconductor Index",
    "^KS11": "KOSPI",
    "^IXIC": "NASDAQ Composite",
    "^DJI": "Dow Jones",
    # ═══ US Sector Leaders — 2nd Battery/EV (3종목) ═══
    "TSLA": "Tesla",
    "ALB": "Albemarle (Lithium)",
    "ENPH": "Enphase Energy (Solar/Clean Energy)",
    # ═══ US Sector Leaders — Robotics/Automation (3종목) ═══
    "ISRG": "Intuitive Surgical",
    "ROK": "Rockwell Automation",
    "ABB": "ABB Ltd (Industrial Automation)",
    # ═══ US Sector Leaders — Defense/Aerospace/Space (3종목) ═══
    "LMT": "Lockheed Martin",
    "RTX": "RTX Corp (Raytheon)",
    "RKLB": "Rocket Lab",
    # ═══ FX ═══
    "KRW=X": "USD/KRW",
    "DX-Y.NYB": "US Dollar Index",
    # ═══ Commodities ═══
    "CL=F": "WTI Crude Oil",
    "GC=F": "Gold",
}

# Key correlation pairs to track
# Priority: Memory/SOX → 삼성/하이닉스 (상관관계 가장 높음, 코스피 시가총액 1,2위)
CORRELATION_PAIRS = [
    # ═══ Memory Sector (최우선 — 코스피 지수 영향 최대, 시총 1,2위) ═══
    ("MU", "000660.KS", "Micron <-> SK Hynix"),
    ("MU", "005930.KS", "Micron <-> Samsung"),
    ("WDC", "000660.KS", "WDC(SanDisk) <-> SK Hynix"),
    ("WDC", "005930.KS", "WDC(SanDisk) <-> Samsung"),
    ("AMAT", "000660.KS", "AMAT(장비) <-> SK Hynix"),
    ("LRCX", "005930.KS", "Lam Research(장비) <-> Samsung"),
    # SOX Index (반도체 전체)
    ("^SOX", "005930.KS", "SOX <-> Samsung"),
    ("^SOX", "000660.KS", "SOX <-> SK Hynix"),
    ("^SOX", "^KS11", "SOX <-> KOSPI"),
    # NVIDIA (AI capex, 참고용 — 한국 반도체와 직접 상관 약함)
    ("NVDA", "005930.KS", "NVDA <-> Samsung"),
    ("NVDA", "000660.KS", "NVDA <-> SK Hynix"),
    # ═══ 2nd Battery / EV Sector ═══
    ("TSLA", "373220.KS", "Tesla <-> LG Energy"),
    ("TSLA", "006400.KS", "Tesla <-> Samsung SDI"),
    ("ALB", "373220.KS", "ALB(Lithium) <-> LG Energy"),
    ("ALB", "006400.KS", "ALB(Lithium) <-> Samsung SDI"),
    ("ENPH", "373220.KS", "Enphase(Clean Energy) <-> LG Energy"),
    # ═══ Defense / Aerospace / Space ═══
    ("LMT", "012450.KS", "Lockheed Martin <-> Hanwha Aerospace"),
    ("RTX", "012450.KS", "RTX <-> Hanwha Aerospace"),
    ("RKLB", "012450.KS", "Rocket Lab <-> Hanwha Aerospace"),
    # ═══ Robotics / Automation ═══
    ("ISRG", "^KS11", "ISRG(Robotics) <-> KOSPI"),
    ("ROK", "^KS11", "Rockwell(Automation) <-> KOSPI"),
    ("ABB", "^KS11", "ABB(Industrial) <-> KOSPI"),
    # Broad Market
    ("^IXIC", "^KS11", "NASDAQ <-> KOSPI"),
    ("^DJI", "^KS11", "Dow <-> KOSPI"),
    # FX & Commodities
    ("KRW=X", "^KS11", "USD/KRW <-> KOSPI"),
    ("DX-Y.NYB", "^KS11", "DXY <-> KOSPI"),
    ("CL=F", "^KS11", "WTI <-> KOSPI"),
    ("GC=F", "^KS11", "Gold <-> KOSPI"),
]

# Additional market tickers for context (VIX, futures, etc.)
CONTEXT_TICKERS = {
    "^VIX": "VIX",
    "^GSPC": "S&P 500",
    "ES=F": "S&P 500 Futures",
    "NQ=F": "NASDAQ Futures",
    "^KQ11": "KOSDAQ",
    "^TNX": "US 10Y Treasury Yield",
}


# ---------------------------------------------------------------------------
# Phase 1: Correlation Analysis
# ---------------------------------------------------------------------------
def fetch_correlation_data() -> dict:
    """Fetch 60-day historical data and calculate rolling correlations.

    Returns:
        dict with keys:
            - prices: dict of ticker -> {current, prev_close, change_pct}
            - correlations: list of {pair, coefficient, implied_move, strength}
            - top_correlations: top 3 by absolute coefficient
            - raw_returns: dict of ticker -> latest daily return %
    """
    if not yf or not pd or not np:
        log.warning("yfinance/pandas/numpy not installed, skipping correlation analysis")
        return {"prices": {}, "correlations": [], "top_correlations": [], "raw_returns": {}}

    log.info("=== Correlation Analysis: Fetching 60-day data ===")

    all_tickers = list(CORRELATION_TICKERS.keys()) + list(CONTEXT_TICKERS.keys())
    result = {
        "prices": {},
        "correlations": [],
        "top_correlations": [],
        "raw_returns": {},
    }

    try:
        df = yf.download(
            all_tickers,
            period="90d",  # fetch extra to ensure 60 trading days
            interval="1d",
            progress=False,
            threads=True,
            timeout=30,
        )
        if df.empty:
            log.warning("Empty dataframe from yfinance")
            return result
    except Exception as e:
        log.error("yfinance download failed: %s", e)
        return result

    # Extract close prices
    try:
        close = df["Close"] if len(all_tickers) > 1 else df[["Close"]]
    except KeyError:
        log.error("No 'Close' column in downloaded data")
        return result

    # Calculate daily returns
    returns = close.pct_change().dropna()

    # Keep only last 60 trading days
    returns = returns.tail(60)
    close = close.tail(61)  # need 61 for returns of 60

    # Extract current prices and changes
    for ticker in all_tickers:
        try:
            col = close[ticker].dropna()
            if len(col) < 2:
                continue
            current = float(col.iloc[-1])
            prev = float(col.iloc[-2])
            if math.isnan(current) or math.isnan(prev):
                continue
            chg_pct = ((current - prev) / prev) * 100 if prev != 0 else 0.0
            name = CORRELATION_TICKERS.get(ticker, CONTEXT_TICKERS.get(ticker, ticker))
            result["prices"][ticker] = {
                "name": name,
                "current": round(current, 2),
                "prev_close": round(prev, 2),
                "change_pct": round(chg_pct, 2),
            }
        except Exception:
            continue

    # Calculate daily returns for context
    for ticker in all_tickers:
        try:
            ret_col = returns[ticker].dropna()
            if len(ret_col) > 0:
                result["raw_returns"][ticker] = round(float(ret_col.iloc[-1]) * 100, 2)
        except Exception:
            continue

    # Calculate 20-day rolling correlations for each pair
    log.info("Calculating 20-day rolling correlations...")
    for ticker_a, ticker_b, pair_name in CORRELATION_PAIRS:
        try:
            if ticker_a not in returns.columns or ticker_b not in returns.columns:
                continue

            ret_a = returns[ticker_a].dropna()
            ret_b = returns[ticker_b].dropna()

            # Align the two series
            common = ret_a.index.intersection(ret_b.index)
            if len(common) < 20:
                continue

            ret_a = ret_a.loc[common]
            ret_b = ret_b.loc[common]

            # 20-day rolling correlation
            rolling_corr = ret_a.rolling(window=20).corr(ret_b)
            latest_corr = rolling_corr.dropna()
            if len(latest_corr) == 0:
                continue

            corr_value = float(latest_corr.iloc[-1])
            if math.isnan(corr_value):
                continue

            # Calculate implied move for Korean asset based on US asset's latest return
            implied_move = None
            # If ticker_a is a US asset and ticker_b is Korean, calculate implied move
            us_tickers = {"NVDA", "MU", "WDC", "AMAT", "LRCX",
                         "TSLA", "ALB", "ENPH", "ISRG", "ROK", "ABB",
                         "LMT", "RTX", "RKLB",
                         "^SOX", "^IXIC", "^DJI", "DX-Y.NYB", "CL=F", "GC=F"}
            kr_tickers = {"005930.KS", "000660.KS", "^KS11",
                          "373220.KS", "006400.KS", "012450.KS"}

            if ticker_a in us_tickers and ticker_b in kr_tickers:
                us_return = result["raw_returns"].get(ticker_a)
                if us_return is not None:
                    # Simple implied move: correlation * US return * (kr_vol / us_vol)
                    kr_std = float(ret_b.tail(20).std()) if len(ret_b) >= 20 else float(ret_b.std())
                    us_std = float(ret_a.tail(20).std()) if len(ret_a) >= 20 else float(ret_a.std())
                    vol_ratio = kr_std / us_std if us_std > 0 else 1.0
                    implied_move = round(corr_value * us_return * vol_ratio, 2)
            elif ticker_a in kr_tickers and ticker_b in us_tickers:
                us_return = result["raw_returns"].get(ticker_b)
                if us_return is not None:
                    kr_std = float(ret_a.tail(20).std()) if len(ret_a) >= 20 else float(ret_a.std())
                    us_std = float(ret_b.tail(20).std()) if len(ret_b) >= 20 else float(ret_b.std())
                    vol_ratio = kr_std / us_std if us_std > 0 else 1.0
                    implied_move = round(corr_value * us_return * vol_ratio, 2)

            # Determine strength
            abs_corr = abs(corr_value)
            if abs_corr >= 0.7:
                strength = "strong"
            elif abs_corr >= 0.4:
                strength = "moderate"
            else:
                strength = "weak"

            entry = {
                "pair": pair_name,
                "ticker_a": ticker_a,
                "ticker_b": ticker_b,
                "coefficient": round(corr_value, 4),
                "strength": strength,
                "implied_move": implied_move,
            }
            result["correlations"].append(entry)

        except Exception as e:
            log.warning("Correlation error for %s: %s", pair_name, e)
            continue

    # Sort by absolute coefficient and pick top 3
    sorted_corrs = sorted(result["correlations"], key=lambda x: abs(x["coefficient"]), reverse=True)
    result["top_correlations"] = sorted_corrs[:3]

    log.info(
        "Correlation analysis complete: %d pairs calculated, top corr: %s",
        len(result["correlations"]),
        result["top_correlations"][0]["pair"] if result["top_correlations"] else "N/A",
    )

    return result


# ---------------------------------------------------------------------------
# Phase 2: Foreign Investor Flow
# ---------------------------------------------------------------------------
def fetch_foreign_flow() -> dict:
    """Fetch foreign investor flow data from Naver Finance / KRX.

    Returns:
        dict with keys:
            - net_amount: net buy/sell in billion KRW (positive = net buy)
            - consecutive_days: number of consecutive buy or sell days
            - direction: "buy" or "sell"
            - institutional: net institutional flow
            - retail: net retail flow
            - source: where the data came from
            - details: list of individual data points
    """
    log.info("=== Foreign Investor Flow ===")

    result = {
        "net_amount": None,
        "consecutive_days": None,
        "direction": None,
        "institutional": None,
        "retail": None,
        "source": "unavailable",
        "details": [],
    }

    # Attempt 1: KRX (가장 신뢰할 수 있는 소스, 금액 기반)
    try:
        result = _fetch_krx_foreign_flow()
        if result.get("net_amount") is not None:
            log.info("Foreign flow from KRX: %s", result["direction"])
            return result
    except Exception as e:
        log.warning("KRX foreign flow failed: %s", e)

    # Attempt 2: Naver Finance (fallback)
    try:
        result = _fetch_naver_foreign_flow()
        if result.get("net_amount") is not None:
            log.info(
                "Foreign flow from Naver: %s %s (%d consecutive days)",
                result["direction"],
                abs(result["net_amount"]),
                result.get("consecutive_days", 0),
            )
            return result
    except Exception as e:
        log.warning("Naver Finance foreign flow failed: %s", e)

    # Attempt 3: Fallback — estimate from ETF flows
    try:
        result = _estimate_foreign_flow_from_etf()
        if result.get("net_amount") is not None:
            log.info("Foreign flow estimated from ETF: %s", result["direction"])
            return result
    except Exception as e:
        log.warning("ETF-based foreign flow estimation failed: %s", e)

    log.warning("All foreign flow sources failed, returning empty data")
    return result


def _fetch_naver_foreign_flow() -> dict:
    """Scrape foreign investor flow from Naver Finance."""
    url = "https://finance.naver.com/sise/investorDealTrendDay.naver"
    params = {"bizdate": "", "sosession": ""}

    headers = {
        **HEADERS,
        "Referer": "https://finance.naver.com/sise/sise_dealer.naver",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    }

    resp = requests.get(url, params=params, headers=headers, timeout=15)
    resp.raise_for_status()
    content = resp.text

    # Parse the HTML table for investor trend data
    # Naver Finance returns data in HTML table format
    result = {
        "net_amount": None,
        "consecutive_days": None,
        "direction": None,
        "institutional": None,
        "retail": None,
        "source": "naver_finance",
        "details": [],
    }

    # Extract numbers from the investor trend table
    # Pattern: look for rows with date and amounts
    # Foreign investor net amounts are typically in the 3rd-4th columns
    rows = re.findall(
        r'<td[^>]*class="number_1"[^>]*>([\-\+]?[\d,]+)</td>',
        content,
    )

    if not rows:
        # Alternative pattern for Naver
        rows = re.findall(
            r'<td[^>]*>([\-\+]?[\d,]+)</td>',
            content,
        )

    if rows and len(rows) >= 3:
        # Parse as best we can: foreign, institutional, retail columns
        try:
            amounts = []
            for r in rows:
                cleaned = r.replace(",", "").replace("+", "")
                if cleaned.lstrip("-").isdigit():
                    amounts.append(int(cleaned))

            if len(amounts) >= 3:
                # Typical order in Naver: individual, foreign, institutional
                foreign_net = amounts[1] if len(amounts) > 1 else 0
                institutional_net = amounts[2] if len(amounts) > 2 else 0
                retail_net = amounts[0] if len(amounts) > 0 else 0

                # Amounts are in millions KRW, convert to billions
                result["net_amount"] = round(foreign_net / 100, 1)  # approximate
                result["direction"] = "buy" if foreign_net > 0 else "sell"
                result["institutional"] = round(institutional_net / 100, 1)
                result["retail"] = round(retail_net / 100, 1)

                # Count consecutive days (simplified: check last 5 entries)
                consecutive = 1
                sign = 1 if foreign_net > 0 else -1
                for i in range(3, min(len(amounts), 15), 3):
                    if len(amounts) > i + 1:
                        next_val = amounts[i + 1]
                        if (next_val > 0 and sign > 0) or (next_val < 0 and sign < 0):
                            consecutive += 1
                        else:
                            break
                result["consecutive_days"] = consecutive
                return result
        except (ValueError, IndexError):
            pass

    return result


def _fetch_krx_foreign_flow() -> dict:
    """Attempt to fetch foreign flow from KRX open data."""
    result = {
        "net_amount": None,
        "consecutive_days": None,
        "direction": None,
        "institutional": None,
        "retail": None,
        "source": "krx",
        "details": [],
    }

    today = datetime.now(KST)
    # Fall back to previous business day on weekends
    if today.weekday() >= 5:  # Saturday=5, Sunday=6
        today -= timedelta(days=(today.weekday() - 4))
    trd_date = today.strftime("%Y%m%d")

    # KRX KOSPI investor trading trend
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    payload = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT02203",
        "locale": "ko_KR",
        "trdDd": trd_date,
        "mktId": "STK",  # KOSPI
        "csvxls_is498": "false",
    }

    headers = {
        **HEADERS,
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020203",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }

    resp = requests.post(url, data=payload, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    items = data.get("output", [])
    if not items:
        return result

    # Find foreign and institutional rows
    for item in items:
        name = item.get("INVST_TP_NM", "")
        # NETBVSAMT = 순매수/매도 금액 (원), NETBVSQTY = 주수
        # 금액이 더 정확한 수급 강도 지표 (삼성전자 1주 vs 소형주 1주 가치 다름)
        net_krw = item.get("NETBVSAMT", item.get("NETBVSQTY", "0")).replace(",", "")
        try:
            net_amount = int(net_krw)
        except ValueError:
            net_amount = 0

        if "외국인" in name:
            result["net_amount"] = round(net_amount / 100_000_000, 1)  # 억원
            result["net_amount_unit"] = "억원"
            result["direction"] = "buy" if net_amount > 0 else "sell"
        elif "기관" in name:
            result["institutional"] = round(net_amount / 100_000_000, 1)  # 억원
        elif "개인" in name:
            result["retail"] = round(net_amount / 100_000_000, 1)  # 억원

    if result["net_amount"] is not None:
        result["consecutive_days"] = 1  # KRX single day, no consecutive tracking here

    return result


def _estimate_foreign_flow_from_etf() -> dict:
    """Fallback: estimate foreign flow direction from Korea-related ETF volumes.

    Uses EWY (iShares MSCI South Korea ETF) as a proxy. Rising volume + positive
    price action suggests foreign inflows; the reverse suggests outflows.
    """
    result = {
        "net_amount": None,
        "consecutive_days": None,
        "direction": None,
        "institutional": None,
        "retail": None,
        "source": "etf_proxy",
        "details": [],
    }

    if not yf or not pd:
        return result

    try:
        ewy = yf.download("EWY", period="10d", interval="1d", progress=False, timeout=15)
        if ewy.empty or len(ewy) < 2:
            return result

        close = ewy["Close"].dropna()
        volume = ewy["Volume"].dropna()

        if len(close) < 2 or len(volume) < 2:
            return result

        current_price = float(close.iloc[-1])
        prev_price = float(close.iloc[-2])
        current_vol = float(volume.iloc[-1])
        avg_vol = float(volume.tail(5).mean())

        price_change = ((current_price - prev_price) / prev_price) * 100
        vol_ratio = current_vol / avg_vol if avg_vol > 0 else 1.0

        # Heuristic: positive price + above-avg volume = inflow
        # 주의: 애매한 경우 "unknown"으로 처리 (기존: 기본 sell → short 바이어스 유발)
        if price_change > 0.3 and vol_ratio > 1.1:
            direction = "buy"
            estimated_net = round(price_change * vol_ratio * 100, 0)  # rough proxy in billions
        elif price_change < -0.3 and vol_ratio > 1.1:
            direction = "sell"
            estimated_net = round(price_change * vol_ratio * 100, 0)
        elif abs(price_change) > 0.3:
            direction = "buy" if price_change > 0 else "sell"
            estimated_net = round(price_change * 50, 0)
        else:
            # 변동폭 ±0.3% 이내 = 방향 판단 불가 → unknown 처리
            direction = "unknown"
            estimated_net = 0

        # Count consecutive direction days
        consecutive = 0
        for i in range(len(close) - 1, 0, -1):
            day_chg = float(close.iloc[i]) - float(close.iloc[i - 1])
            if (direction == "buy" and day_chg > 0) or (direction == "sell" and day_chg <= 0):
                consecutive += 1
            else:
                break

        if direction == "unknown":
            # 방향 판단 불가 — neutral factor로 처리, short 바이어스 방지
            result["source"] = "etf_proxy_inconclusive"
            result["details"].append({
                "proxy": "EWY",
                "price_change_pct": round(price_change, 2),
                "volume_ratio": round(vol_ratio, 2),
                "note": "변동폭 미미하여 방향 판단 불가",
            })
        else:
            result["net_amount"] = estimated_net
            result["direction"] = direction
            result["consecutive_days"] = max(consecutive, 1)
            result["details"].append({
                "proxy": "EWY",
                "price_change_pct": round(price_change, 2),
                "volume_ratio": round(vol_ratio, 2),
            })

    except Exception as e:
        log.warning("EWY proxy estimation error: %s", e)

    return result


# ---------------------------------------------------------------------------
# Phase 2b: KOSPI200 Night Futures from News Headlines
# ---------------------------------------------------------------------------
def extract_night_futures_from_news(articles: list) -> dict:
    """Extract KOSPI200 night futures data from news headlines.

    KOSPI200 야간선물은 yfinance에서 제공되지 않으므로,
    뉴스 헤드라인에서 파싱하여 추출합니다.
    """
    result = {
        "found": False,
        "change_pct": None,
        "close_price": None,
        "headline": None,
        "source": None,
    }

    if not articles:
        return result

    # 야간선물 관련 키워드 패턴
    # 실제 헤드라인 예시:
    #   "코스피200 야간선물, 4.32% 급락 '털썩'…코스피 시초가 '하락' 예상"
    #   "[속보] 코스피200 야간선물 6% 가까이 급반등…장중 850선 회복"
    #   "야간선물 1.5% 하락 마감"
    patterns = [
        re.compile(r"야간선물[,\s]*(\d+\.?\d*)%", re.IGNORECASE),
        re.compile(r"야간선물[,\s]+([+-]?\d+\.?\d*)%", re.IGNORECASE),
        re.compile(r"야간.*?선물[,\s]*(\d+\.?\d*)%", re.IGNORECASE),
        re.compile(r"코스피200\s*야간선물[,\s]*(\d+\.?\d*)%", re.IGNORECASE),
        re.compile(r"코스피.*?야간.*?(\d+\.?\d*)%", re.IGNORECASE),
        re.compile(r"KOSPI.*?200.*?futures?.*?(\d+\.?\d*)%", re.IGNORECASE),
        re.compile(r"night.*?futures?.*?(\d+\.?\d*)%", re.IGNORECASE),
    ]

    # 방향 키워드 (퍼센트가 부호 없이 나올 때 방향 판단)
    bearish_words = {"급락", "하락", "떨어", "밀려", "빠져", "폭락", "약세", "decline", "drop", "fall", "down"}
    bullish_words = {"급등", "상승", "반등", "올라", "뛰어", "폭등", "강세", "rally", "surge", "rise", "up", "회복"}

    # 야간선물 종가/포인트 패턴
    price_patterns = [
        re.compile(r"(\d{3,4})선\s*(?:회복|돌파|붕괴|이탈)", re.IGNORECASE),
        re.compile(r"야간선물.*?(\d{3,4}\.?\d*)\s*(?:pt|포인트|에)", re.IGNORECASE),
    ]

    for a in articles:
        text = a.get("title", "") + " " + a.get("description", a.get("summary", ""))

        for pat in patterns:
            m = pat.search(text)
            if m:
                try:
                    pct = float(m.group(1))

                    # 부호 판단: 숫자에 부호가 없으면 주변 키워드로 방향 결정
                    text_lower = text.lower()
                    is_bearish = any(w in text_lower for w in bearish_words)
                    is_bullish = any(w in text_lower for w in bullish_words)
                    if is_bearish and not is_bullish:
                        pct = -abs(pct)
                    elif is_bullish and not is_bearish:
                        pct = abs(pct)
                    # 양쪽 다 있거나 없으면 원래 부호 유지

                    result["found"] = True
                    result["change_pct"] = pct
                    result["headline"] = a.get("title", "")[:100]
                    result["source"] = a.get("source", "")
                    log.info("KOSPI200 night futures from news: %+.2f%% [%s]", pct, result["source"])

                    # 종가/포인트도 찾기
                    for ppat in price_patterns:
                        pm = ppat.search(text)
                        if pm:
                            result["close_price"] = float(pm.group(1).replace(",", ""))
                            break

                    return result
                except (ValueError, IndexError):
                    continue

    return result


# ---------------------------------------------------------------------------
# Phase 3: Build Analysis Context
# ---------------------------------------------------------------------------
def build_analysis_context(
    market_data: dict,
    correlations: dict,
    foreign_flow: dict,
    articles: list = None,
) -> str:
    """Build a structured text context for the LLM from all data sources.

    Args:
        market_data: correlation data dict (includes prices and correlations)
        correlations: same as market_data (correlation analysis output)
        foreign_flow: foreign investor flow dict
        articles: list of news article dicts (optional)

    Returns:
        Formatted context string for LLM prompt
    """
    sections = []
    now = datetime.now(KST)
    sections.append(f"=== ANALYSIS TIMESTAMP: {now.strftime('%Y-%m-%d %A %H:%M KST')} ===")
    sections.append("")
    sections.append("=== TIME CHAIN (CRITICAL — read this first) ===")
    sections.append(f"  NOW: {now.strftime('%Y-%m-%d %H:%M')} KST (Korean time)")
    sections.append("  KOSPI: CLOSED (last session ended yesterday ~15:30 KST)")
    sections.append("  US market: CLOSED (ended ~06:00 KST today = yesterday US time)")
    sections.append("  KOSPI opens: TODAY 09:00 KST (about 2 hours from now)")
    sections.append("")
    sections.append("  TIMELINE:")
    sections.append("    Yesterday ~15:30 KST: KOSPI closed → this data is 15+ hours old")
    sections.append("    Yesterday ~23:30 KST: US market opened")
    sections.append("    Today ~06:00 KST: US market closed → this is the LATEST data")
    sections.append("    Today ~06:00 KST: US market closed, KOSPI200 night futures (야간선물) settled → MOST RELEVANT")
    sections.append("    Today 09:00 KST: KOSPI will open → THIS is what we're predicting")
    sections.append("")
    sections.append("  ⚠️ ALL news headlines below are ALREADY PRICED INTO the US close and overnight futures.")
    sections.append("     Do NOT count news + futures as separate bearish/bullish factors — that's double-counting.")
    sections.append("     The futures price IS the market's reaction to the news.")

    # --- KOSPI200 Night Futures (야간선물) from news ---
    if articles:
        night_futures = extract_night_futures_from_news(articles)
        if night_futures["found"]:
            sections.append("\n=== KOSPI200 NIGHT FUTURES (야간선물) — #1 LEADING INDICATOR ===")
            sections.append(f"  Change: {night_futures['change_pct']:+.2f}%")
            if night_futures["close_price"]:
                sections.append(f"  Close: {night_futures['close_price']:.2f}")
            sections.append(f"  Source: [{night_futures['source']}] {night_futures['headline']}")
            sections.append("  ⚠️ This is the SINGLE MOST IMPORTANT data point for today's KOSPI open.")
        else:
            sections.append("\n=== KOSPI200 NIGHT FUTURES (야간선물) ===")
            sections.append("  Data not found in news headlines. Using US futures (ES=F, NQ=F) as proxy.")

    # --- Market Prices ---
    sections.append("\n=== MARKET PRICES & CHANGES ===")
    prices = correlations.get("prices", {})

    # Group prices by category
    categories = {
        "US Indices": ["^GSPC", "^IXIC", "^DJI", "^SOX"],
        "US Futures": ["ES=F", "NQ=F"],
        "Volatility": ["^VIX"],
        "US Semiconductors/Memory": ["NVDA", "MU", "WDC", "AMAT", "LRCX"],
        "US EV/Battery": ["TSLA", "ALB", "ENPH"],
        "US Defense/Space": ["LMT", "RTX", "RKLB"],
        "US Robotics": ["ISRG", "ROK", "ABB"],
        "Korean Market": ["^KS11", "^KQ11", "005930.KS", "000660.KS",
                          "373220.KS", "006400.KS", "012450.KS"],
        "FX": ["KRW=X", "DX-Y.NYB"],
        "Commodities": ["CL=F", "GC=F"],
        "Bonds": ["^TNX"],
    }

    for cat_name, tickers in categories.items():
        cat_entries = []
        for t in tickers:
            p = prices.get(t)
            if p:
                sign = "+" if p["change_pct"] >= 0 else ""
                cat_entries.append(
                    f"  {p['name']} ({t}): {p['current']} ({sign}{p['change_pct']}%)"
                )
        if cat_entries:
            sections.append(f"[{cat_name}]")
            sections.extend(cat_entries)

    # --- Correlation Data (강도순 정렬, 약한 상관은 필터) ---
    sections.append("\n=== CORRELATION ANALYSIS (20-day rolling, lag-1) ===")
    sections.append("  ※ r > 0.6 = 신뢰 가능, r 0.3~0.6 = 참고, r < 0.3 = 무시 권장")
    strong_corrs = []
    weak_corrs = []
    for corr in correlations.get("correlations", []):
        implied = f", implied move: {corr['implied_move']:+.2f}%" if corr.get("implied_move") is not None else ""
        abs_corr = abs(corr['coefficient'])
        if abs_corr >= 0.5:
            tag = "★" if abs_corr >= 0.7 else ""
            strong_corrs.append(
                f"  {tag}{corr['pair']}: r={corr['coefficient']:+.4f} ({corr['strength']}){implied}"
            )
        else:
            weak_corrs.append(
                f"  {corr['pair']}: r={corr['coefficient']:+.4f} (약함 — 방향 근거로 사용 비권장){implied}"
            )
    if strong_corrs:
        sections.append("[HIGH CONFIDENCE PAIRS]")
        sections.extend(strong_corrs)
    if weak_corrs:
        sections.append("[LOW CONFIDENCE PAIRS — 참고만]")
        sections.extend(weak_corrs)

    if correlations.get("top_correlations"):
        sections.append("\nTOP 3 STRONGEST CORRELATIONS (핵심 판단 근거):")
        for i, tc in enumerate(correlations["top_correlations"], 1):
            implied = f", implied move: {tc['implied_move']:+.2f}%" if tc.get("implied_move") is not None else ""
            sections.append(
                f"  #{i} {tc['pair']}: r={tc['coefficient']:+.4f}{implied}"
            )

    # --- Foreign Flow ---
    sections.append("\n=== FOREIGN INVESTOR FLOW ===")
    if foreign_flow.get("net_amount") is not None:
        sections.append(f"  Direction: {foreign_flow['direction'].upper()}")
        unit = foreign_flow.get("net_amount_unit", "억원")
        sections.append(f"  Net amount: {foreign_flow['net_amount']} {unit}")
        if foreign_flow.get("consecutive_days"):
            sections.append(f"  Consecutive days: {foreign_flow['consecutive_days']}")
        if foreign_flow.get("institutional") is not None:
            sections.append(f"  Institutional net: {foreign_flow['institutional']} {unit}")
        if foreign_flow.get("retail") is not None:
            sections.append(f"  Retail net: {foreign_flow['retail']} {unit}")
        sections.append(f"  Source: {foreign_flow.get('source', 'unknown')}")
    else:
        sections.append("  Data unavailable — consider this as neutral/unknown factor")

    # --- VIX Context ---
    vix_data = prices.get("^VIX")
    if vix_data:
        vix_val = vix_data["current"]
        if vix_val < 15:
            vix_regime = "extreme complacency"
        elif vix_val < 20:
            vix_regime = "low volatility"
        elif vix_val < 25:
            vix_regime = "moderate caution"
        elif vix_val < 30:
            vix_regime = "elevated fear"
        else:
            vix_regime = "extreme fear / panic"
        sections.append(f"\n=== VOLATILITY REGIME ===")
        sections.append(f"  VIX: {vix_val} ({vix_regime}), change: {vix_data['change_pct']:+.2f}%")

    # --- FX Detail ---
    krw = prices.get("KRW=X")
    dxy = prices.get("DX-Y.NYB")
    if krw or dxy:
        sections.append("\n=== FX DETAIL ===")
        if krw:
            sections.append(f"  USD/KRW: {krw['current']} ({krw['change_pct']:+.2f}%)")
        if dxy:
            sections.append(f"  Dollar Index: {dxy['current']} ({dxy['change_pct']:+.2f}%)")

    # --- Oil & Gold (참고용 — KOSPI 방향 예측력 없음, 백테스트 확인) ---
    oil = prices.get("CL=F")
    gold = prices.get("GC=F")
    if oil or gold:
        sections.append("\n=== COMMODITIES (참고용 — KOSPI 방향 예측력 없음) ===")
        if oil:
            sections.append(f"  WTI Crude: ${oil['current']} ({oil['change_pct']:+.2f}%) [KOSPI 상관 없음]")
        if gold:
            sections.append(f"  Gold: ${gold['current']} ({gold['change_pct']:+.2f}%) [KOSPI 상관 없음]")

    # --- Recent news — 5~7AM KST 뉴스만 유효 (이전 뉴스는 이미 야간선물에 반영) ---
    if articles:
        # 오전 5시 이후 뉴스만 필터 (그 전 뉴스는 미국장/야간선물에 이미 반영)
        kst_5am = now.replace(hour=5, minute=0, second=0, microsecond=0)
        fresh_articles = []
        for a in articles:
            pub = a.get("published_parsed") or a.get("pub_date")
            # 발행 시간을 알 수 없으면 포함 (필터 누락보다 과포함이 나음)
            if pub is None:
                fresh_articles.append(a)
            else:
                fresh_articles.append(a)  # RSS에서 이미 시간 필터링됨
        fresh_articles = fresh_articles[:15]  # 최대 15개

        sections.append(f"\n=== NEWS HEADLINES ({len(fresh_articles)} articles) ===")
        sections.append("⚠️ These are post-5AM KST articles. Earlier news is ALREADY in futures prices.")
        sections.append("   Use news only to EXPLAIN price moves, not as independent directional factors.")
        for a in fresh_articles:
            source = a.get("source", "Unknown")
            title = a.get("title", "")
            sections.append(f"  [{source}] {title}")

    # --- Also load live.json if available ---
    live_path = DATA_DIR / "live.json"
    if live_path.exists():
        try:
            with open(live_path, "r", encoding="utf-8") as f:
                live_data = json.load(f)
            md = live_data.get("market_data", {})
            # Add any data not already present
            extra_cats = []
            for cat, items in md.items():
                if cat in ("us_indices", "futures", "bonds", "volatility"):
                    continue  # already covered
                for item in items:
                    if item.get("ticker") not in prices:
                        extra_cats.append(
                            f"  {item['name']} ({item['ticker']}): {item['price']} ({item['change_pct']:+.2f}%)"
                        )
            if extra_cats:
                sections.append("\n=== ADDITIONAL MARKET DATA (from live.json) ===")
                sections.extend(extra_cats[:20])
        except Exception:
            pass

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Phase 4: LLM-Driven Investment Insights
# ---------------------------------------------------------------------------
ANALYSIS_SYSTEM_PROMPT = """헤지펀드 퀀트 애널리스트. 오전 7시(장전) 한국 개인 투자자용 방향 인사이트 제공.

## 핵심 규칙
1. 반드시 LONG 또는 SHORT. 중립 금지. 51%라도 한쪽 선택.
2. 뻔한 말 금지. 구체적 수치와 상관계수 근거 필수.
3. "갭다운 예상" ≠ "숏 추천". 갭은 이미 반영된 정보. 종가 방향을 예측하라.
4. 뉴스는 이미 야간선물 가격에 반영됨 — 뉴스+선물 이중 카운트 금지.

## 판단 원칙
- NQ/SOX/ES 야간선물이 KOSPI 시가의 최강 선행지표 (NQ 상관 r≈0.85)
- 상승 시그널은 매우 강하지만 (88~95%), 하락 시그널은 단일 팩터로 약함 (59~65%)
- 하락 판단은 반드시 복수 팩터 동시 확인 필요 (SOX+VIX+환율 등)
- 갭 방향은 종가 방향과 거의 일치하나, 장중 확장은 보장 안 됨
- 금/유가는 KOSPI 방향 예측력 없음 — 참고만 하고 방향 시그널로 사용 금지
- 데이터에 상관계수와 implied move가 포함됨 — 이를 근거로 섹터별 방향 판단

## Opening outlook labels (use in summary, based on overnight futures)
- ES/NQ > +1.5%: "강한 상승 출발"
- ES/NQ +0.5~1.5%: "상승 출발"
- ES/NQ +0.1~0.5%: "약보합 상승 출발"
- ES/NQ ±0.1%: "보합 출발"
- ES/NQ -0.1~-0.5%: "약보합 하락 출발"
- ES/NQ -0.5~-1.5%: "하락 출발"
- ES/NQ < -1.5%: "강한 하락 출발"

## Response format (JSON, all text in Korean, keep it SHORT)
{
  "direction": "long" or "short",
  "long_pct": 51~85,
  "short_pct": 15~49,
  "confidence": 0.5~0.9,
  "summary": "2문장 이내. 시가 전망 + 핵심 드라이버 1개. 짧고 임팩트있게.",
  "factors": [{"name": "팩터명", "signal": "bullish/bearish", "detail": "수치"}],
  "correlations": [{"pair": "Micron <-> SK Hynix", "coefficient": 0.70, "implied_move": "+1.2%"}],
  "foreign_flow": {"net_amount": 1500, "consecutive_days": 3, "direction": "buy"},
  "key_insight": "1문장. 핵심 변수 또는 시나리오 전환 조건만.",
  "sectors": [{"name": "섹터명", "direction": "overweight/underweight", "reason": "10자 이내"}]
}"""


def generate_investment_insights(context: str) -> dict:
    """Send structured context to LLM and get directional investment insights.

    Args:
        context: formatted context string from build_analysis_context()

    Returns:
        dict with direction, confidence, summary, factors, correlations,
        foreign_flow, key_insight, sectors
    """
    log.info("=== Generating AI Investment Insights ===")

    if not API_KEY:
        log.error("OPENROUTER_API_KEY not set")
        return _fallback_analysis(context)

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/nydad/nydad-bot",
        "X-Title": "Nydad Bot",
    }

    user_prompt = f"""Below is today's market data, correlation analysis, foreign investor flow, and news headlines.
Synthesize all data and provide a directional call for KOSPI/Korean market today.

{context}

Respond in JSON format. direction must be "long" or "short" only. All text fields in KOREAN."""

    payload = {
        "model": MODEL_QUALITY,
        "messages": [
            {"role": "system", "content": ANALYSIS_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.3,
        "max_tokens": 4096,
        "response_format": {"type": "json_object"},
    }

    for attempt in range(1, RETRY + 2):
        try:
            resp = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=180,
            )

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

            # Strip markdown code fences if present
            if content.startswith("```"):
                content = "\n".join(content.split("\n")[1:])
                if content.rstrip().endswith("```"):
                    content = content.rstrip()[:-3]
                content = content.strip()

            result = json.loads(content)

            # Validate and enforce constraints
            result = _validate_insights(result)

            log.info(
                "AI analysis complete: %s (confidence: %.2f)",
                result["direction"],
                result["confidence"],
            )
            return result

        except json.JSONDecodeError as e:
            log.warning("JSON parse error (attempt %d): %s", attempt, e)
        except Exception as e:
            log.warning("API fail (attempt %d): %s", attempt, e)

        if attempt <= RETRY:
            time.sleep(RETRY_DELAY * (2 ** (attempt - 1)))

    log.error("All API attempts failed, using fallback analysis")
    return _fallback_analysis(context)


def _validate_insights(result: dict) -> dict:
    """Validate and fix the LLM response to meet constraints."""

    # Enforce direction is never neutral
    direction = result.get("direction", "long")
    if direction not in ("long", "short"):
        direction = "long"
    result["direction"] = direction

    # Enforce long_pct / short_pct ranges
    long_pct = result.get("long_pct", 55 if direction == "long" else 45)
    short_pct = result.get("short_pct", 100 - long_pct)

    if not isinstance(long_pct, (int, float)):
        long_pct = 55 if direction == "long" else 45
    if not isinstance(short_pct, (int, float)):
        short_pct = 100 - long_pct

    long_pct = int(long_pct)
    short_pct = int(short_pct)

    # Ensure they sum to 100
    if long_pct + short_pct != 100:
        short_pct = 100 - long_pct

    # Ensure direction matches percentages
    if direction == "long" and long_pct < 51:
        long_pct = 51
        short_pct = 49
    elif direction == "short" and short_pct < 51:
        short_pct = 51
        long_pct = 49

    # Clamp ranges
    long_pct = max(15, min(85, long_pct))
    short_pct = 100 - long_pct

    result["long_pct"] = long_pct
    result["short_pct"] = short_pct

    # Enforce confidence range
    confidence = result.get("confidence", 0.6)
    if not isinstance(confidence, (int, float)):
        confidence = 0.6
    result["confidence"] = max(0.5, min(0.9, float(confidence)))

    # Ensure required fields exist
    if not result.get("summary"):
        result["summary"] = "데이터 분석 결과를 확인하세요."
    if not result.get("factors"):
        result["factors"] = []
    if not result.get("correlations"):
        result["correlations"] = []
    if not result.get("foreign_flow"):
        result["foreign_flow"] = {"net_amount": None, "consecutive_days": None, "direction": None}
    if not result.get("key_insight"):
        result["key_insight"] = "추가 분석이 필요합니다."
    if not result.get("sectors"):
        result["sectors"] = []

    return result


def _fallback_analysis(context: str) -> dict:
    """Rule-based fallback when LLM is unavailable.

    Parses the context string for key numbers and makes a simple directional call.
    """
    log.info("Running fallback rule-based analysis...")

    bull_signals = 0
    bear_signals = 0
    factors = []

    # Parse VIX
    vix_match = re.search(r"VIX.*?:\s*([\d.]+)", context)
    if vix_match:
        vix = float(vix_match.group(1))
        if vix < 18:
            bull_signals += 1
            factors.append({"name": "VIX 안정권", "signal": "bullish", "detail": f"VIX {vix:.1f}"})
        elif vix > 28:
            bear_signals += 1
            factors.append({"name": "VIX 공포 구간", "signal": "bearish", "detail": f"VIX {vix:.1f}"})

    # Parse S&P 500 Futures
    spf_match = re.search(r"S&P 500 Futures.*?\(([\+\-][\d.]+)%\)", context)
    spf_chg = float(spf_match.group(1)) if spf_match else 0.0
    if spf_match:
        if spf_chg > 0.3:
            bull_signals += 1
            factors.append({"name": "미 선물 강세", "signal": "bullish", "detail": f"{spf_chg:+.2f}%"})
        elif spf_chg < -0.3:
            bear_signals += 1
            factors.append({"name": "미 선물 약세", "signal": "bearish", "detail": f"{spf_chg:+.2f}%"})

    # Parse SOX
    sox_match = re.search(r"Philadelphia Semiconductor.*?\(([\+\-][\d.]+)%\)", context)
    if sox_match:
        sox_chg = float(sox_match.group(1))
        if sox_chg > 0.5:
            bull_signals += 1
            factors.append({"name": "SOX 반도체 강세", "signal": "bullish", "detail": f"SOX {sox_chg:+.2f}%"})
        elif sox_chg < -0.5:
            bear_signals += 1
            factors.append({"name": "SOX 반도체 약세", "signal": "bearish", "detail": f"SOX {sox_chg:+.2f}%"})

    # Parse USD/KRW
    krw_match = re.search(r"USD/KRW.*?\(([\+\-][\d.]+)%\)", context)
    if krw_match:
        krw_chg = float(krw_match.group(1))
        if krw_chg < -0.2:
            bull_signals += 1
            factors.append({"name": "원화 강세", "signal": "bullish", "detail": f"KRW {krw_chg:+.2f}%"})
        elif krw_chg > 0.2:
            bear_signals += 1
            factors.append({"name": "원화 약세", "signal": "bearish", "detail": f"KRW {krw_chg:+.2f}%"})

    # Parse foreign flow direction
    if "Direction: BUY" in context.upper():
        bull_signals += 1
        factors.append({"name": "외국인 순매수", "signal": "bullish", "detail": "외국인 매수 전환"})
    elif "Direction: SELL" in context.upper():
        bear_signals += 1
        factors.append({"name": "외국인 순매도", "signal": "bearish", "detail": "외국인 매도 지속"})

    # Determine direction — pick a side, but no bias on ties
    if bull_signals > bear_signals:
        direction = "long"
        long_pct = min(85, 51 + (bull_signals - bear_signals) * 5)
    elif bear_signals > bull_signals:
        direction = "short"
        long_pct = max(15, 49 - (bear_signals - bull_signals) * 5)
    else:
        # 동점: 선물 방향으로 tiebreak (line 1054의 spf_chg 재사용)
        if spf_chg > 0:
            direction = "long"
        elif spf_chg < 0:
            direction = "short"
        else:
            direction = "long"  # 최후의 기본값
        long_pct = 51  # 동점이므로 최소 확신

    short_pct = 100 - long_pct
    confidence = round(max(bull_signals, bear_signals) / max(len(factors), 1) * 0.9, 2)
    confidence = max(0.5, min(0.9, confidence))

    return {
        "direction": direction,
        "long_pct": long_pct,
        "short_pct": short_pct,
        "confidence": confidence,
        "summary": f"규칙 기반 분석: 강세 신호 {bull_signals}개, 약세 신호 {bear_signals}개 감지. API 미연결로 룰 기반 시그널 생성.",
        "factors": factors,
        "correlations": [],
        "foreign_flow": {"net_amount": None, "consecutive_days": None, "direction": None},
        "key_insight": f"AI 분석 불가 — 룰 기반 시그널: {'강세' if direction == 'long' else '약세'} 우위 ({max(bull_signals, bear_signals)}:{min(bull_signals, bear_signals)})",
        "sectors": [],
        "_fallback": True,
    }


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------
def run_full_analysis(articles: list = None) -> dict:
    """Run the complete analysis pipeline.

    Args:
        articles: optional list of news article dicts to include in context

    Returns:
        Complete analysis result dict
    """
    log.info("=" * 60)
    log.info("Starting AI-driven Korean market analysis")
    log.info("=" * 60)

    # Phase 1 & 2: Correlation data + Foreign flow (parallel — independent I/O)
    with ThreadPoolExecutor(max_workers=2) as pool:
        corr_future = pool.submit(fetch_correlation_data)
        flow_future = pool.submit(fetch_foreign_flow)
        correlations = corr_future.result()
        foreign_flow = flow_future.result()

    # Phase 3: Build context
    context = build_analysis_context(
        market_data=correlations,
        correlations=correlations,
        foreign_flow=foreign_flow,
        articles=articles,
    )

    log.info("Context built: %d characters", len(context))

    # Phase 4: Generate insights
    insights = generate_investment_insights(context)

    # Merge raw data into the result for downstream consumers
    insights["_raw"] = {
        "correlation_data": {
            "prices": correlations.get("prices", {}),
            "top_correlations": correlations.get("top_correlations", []),
            "pair_count": len(correlations.get("correlations", [])),
        },
        "foreign_flow": foreign_flow,
        "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return insights


def main():
    """Standalone execution for testing."""
    print("=" * 60)
    print("  Korean Market AI Analysis — Standalone Test")
    print("=" * 60)
    print()

    result = run_full_analysis()

    # Pretty print the result
    print("\n" + "=" * 60)
    print("  ANALYSIS RESULT")
    print("=" * 60)
    print(f"\n  Direction:  {result['direction'].upper()}")
    print(f"  Long/Short: {result['long_pct']}% / {result['short_pct']}%")
    print(f"  Confidence: {result['confidence']:.2f}")
    print(f"\n  Summary: {result.get('summary', 'N/A')}")
    print(f"\n  Key Insight: {result.get('key_insight', 'N/A')}")

    if result.get("factors"):
        print("\n  Factors:")
        for f in result["factors"]:
            print(f"    - {f['name']}: {f['signal']} ({f.get('detail', '')})")

    if result.get("correlations"):
        print("\n  Correlations:")
        for c in result["correlations"]:
            print(f"    - {c['pair']}: r={c.get('coefficient', 'N/A')}, implied={c.get('implied_move', 'N/A')}")

    ff = result.get("foreign_flow", {})
    if ff.get("net_amount") is not None:
        print(f"\n  Foreign Flow: {ff['direction']} {ff['net_amount']}B KRW ({ff.get('consecutive_days', '?')} days)")

    if result.get("sectors"):
        print("\n  Sector Recommendations:")
        for s in result["sectors"]:
            print(f"    - {s['name']}: {s['direction']} ({s.get('reason', '')})")

    # Save to file for inspection
    output_path = DATA_DIR / "domestic_analysis.json"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        # Remove raw prices dict for cleaner output (keep top_correlations)
        output = {k: v for k, v in result.items() if k != "_raw"}
        output["_meta"] = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "model": MODEL_QUALITY,
            "correlation_pairs": result.get("_raw", {}).get("correlation_data", {}).get("pair_count", 0),
        }
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n  Saved to: {output_path}")
    print()

    return result


if __name__ == "__main__":
    main()
