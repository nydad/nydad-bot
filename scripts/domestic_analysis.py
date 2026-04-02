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

    # Attempt 3: KRX retry (already tried as #1, skip)
    except Exception as e:
        log.warning("KRX foreign flow failed: %s", e)

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
    sections.append(f"=== ANALYSIS DATE: {now.strftime('%Y-%m-%d %A %H:%M KST')} ===\n")

    # --- Market Prices ---
    sections.append("=== MARKET PRICES & CHANGES ===")
    sections.append("⚠️ 선행 지표 우선 원칙:")
    sections.append("   1. 야간선물(ES=F, NQ=F)이 코스피 시가 예측의 최강 선행 지표 (r=0.78~0.85)")
    sections.append("   2. S&P 종가, 전일 KOSPI 종가는 후행 데이터 — 이미 야간선물에 반영됨, 별도 팩터 X")
    sections.append("   3. 뉴스는 이미 가격에 반영됨 — '가격이 왜 움직였는지' 설명용으로만 사용")
    sections.append("   4. 투자자가 보는 시점의 선행 정보만 의사결정에 반영하세요")
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

    # --- Correlation Data ---
    sections.append("\n=== CORRELATION ANALYSIS (20-day rolling) ===")
    for corr in correlations.get("correlations", []):
        implied = f", implied move: {corr['implied_move']:+.2f}%" if corr.get("implied_move") is not None else ""
        sections.append(
            f"  {corr['pair']}: r={corr['coefficient']:+.4f} ({corr['strength']}){implied}"
        )

    if correlations.get("top_correlations"):
        sections.append("\nTOP 3 STRONGEST CORRELATIONS:")
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
            sections.append(f"  Institutional net: {foreign_flow['institutional']} billion KRW")
        if foreign_flow.get("retail") is not None:
            sections.append(f"  Retail net: {foreign_flow['retail']} billion KRW")
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

    # --- Oil & Gold ---
    oil = prices.get("CL=F")
    gold = prices.get("GC=F")
    if oil or gold:
        sections.append("\n=== COMMODITIES DETAIL ===")
        if oil:
            sections.append(f"  WTI Crude: ${oil['current']} ({oil['change_pct']:+.2f}%)")
        if gold:
            sections.append(f"  Gold: ${gold['current']} ({gold['change_pct']:+.2f}%)")

    # --- Recent news headlines ---
    if articles:
        sections.append(f"\n=== RECENT NEWS HEADLINES ({len(articles)} articles) ===")
        sections.append("⚠️ 주의: 이 뉴스들은 대부분 이미 야간선물/나스닥/환율 가격에 반영되어 있습니다.")
        sections.append("   뉴스 내용과 가격 변동을 별도 팩터로 이중 카운트하지 마세요.")
        sections.append("   뉴스는 '가격이 왜 움직였는지' 설명하는 용도로만 사용하세요.")
        for a in articles[:25]:
            source = a.get("source", "Unknown")
            title = a.get("title", "")
            summary = a.get("summary", a.get("description", ""))[:150]
            sections.append(f"  [{source}] {title}")
            if summary:
                sections.append(f"    -> {summary}")

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
ANALYSIS_SYSTEM_PROMPT = """You are a hedge fund quant analyst. Provide actionable directional insight for a Korean retail investor checking markets at 7 AM KST (pre-market).

## CRITICAL RULE: "Expected gap-down" ≠ "SHORT recommendation"
This analysis is generated at 7 AM KST, BEFORE the Korean market opens.
- An expected gap-down is ALREADY PRICED IN. SHORT means betting on FURTHER decline beyond the gap.
- If a gap-down open is expected but intraday reversal is likely → LONG is correct.
- If a gap-up open is expected but selling pressure is likely → SHORT is correct.
- **You are predicting the CLOSING direction**, not the opening direction.
- Overnight futures decline = "likely gap-down open", NOT "short is favorable".

## Morning watch-point: 11 AM 60-min candle (backtested)
- Bullish 11 AM candle → afternoon rally 71% of the time (significant signal)
- Bearish 11 AM candle → afternoon decline only 42% (reversal frequent — low reliability)
- Mention as a **scenario pivot condition**, NOT as directional evidence.

## Core rules
1. You MUST pick LONG or SHORT. Neutral is forbidden. Pick a side even at 51%.
2. NO platitudes ("markets are uncertain", "exercise caution", "volatility ahead"). Banned.
3. Include specific numbers. Not "may rise" but "SOX +2.3% + MU +1.8% implies Samsung +1.5%".
4. Use correlation data actively:
   - MU/WDC ↔ SK Hynix has the highest lag-1 correlation among Korean single stocks (r ≈ 0.65~0.80 lagged)
   - SOX index is the broadest semiconductor sentiment proxy (r ≈ 0.75 lagged to Samsung)
   - Prioritize pairs with high implied moves
5. Use foreign investor flow data as a key directional input when available.
6. key_insight must be non-obvious — derived from THIS specific data combination only.

## Sector correlation framework (lagged 1-day, backtested)
Recommend sectors based on US leader performance:
- **Memory/Semis**: WDC(r=0.80), MU(r=0.74), LRCX(r=0.72), SOX(r=0.75) → Samsung, SK Hynix
- **2nd Battery/EV**: TSLA(r=0.69), SQM(r=0.69), ALB → LG Energy, Samsung SDI
- **Defense/Space**: LMT, RTX, RKLB → Hanwha Aerospace, KAI
- **Power Grid**: NRG(r=0.72), VST(r=0.70) → HD Hyundai Electric
- **Robotics**: ISRG(r=0.47), ROK → Korean robotics stocks
- Each sector transmits with ~1 day lag from US close to Korean open.

## Opening outlook expression guide (for summary field)
Use these opening outlook phrases in the summary based on overnight data:
- ES/NQ futures > +1%: "강한 상승 출발 예상" (strong gap-up expected)
- ES/NQ futures +0.3~1%: "상승 출발 예상" (gap-up expected)
- ES/NQ futures -0.3~+0.3%: "보합 출발 예상" (flat open expected)
- ES/NQ futures -0.3~-1%: "하락 출발 예상" (gap-down expected)
- ES/NQ futures < -1%: "강한 하락 출발 예상" (strong gap-down expected)
Then separately state your CLOSING direction prediction (which may differ from the opening).

## Response format (JSON)
All text fields (summary, detail, key_insight, reason) must be in KOREAN.
{
  "direction": "long" or "short" (NEVER "neutral"),
  "long_pct": integer 51~85,
  "short_pct": integer 15~49 (long_pct + short_pct = 100),
  "confidence": float 0.5~0.9,
  "summary": "3 sentences IN KOREAN. Closing direction forecast + evidence + sectors. Numbers required.",
  "factors": [
    {"name": "factor name in Korean", "signal": "bullish/bearish", "detail": "specific numbers in Korean"}
  ],
  "correlations": [
    {"pair": "Micron <-> SK Hynix", "coefficient": 0.65, "implied_move": "+1.2%"}
  ],
  "foreign_flow": {"net_amount": 1500, "consecutive_days": 3, "direction": "buy"},
  "key_insight": "1 sentence IN KOREAN — 11AM candle watch-point + non-obvious edge.",
  "sectors": [
    {"name": "sector name in Korean", "direction": "overweight/underweight", "reason": "1 sentence in Korean based on US leader"}
  ]
}

## Decision framework (priority order)
1. **Overnight futures are the #1 signal**: ES=F↔KOSPI open r=0.78, SOX↔KOSPI open r=0.85
2. KOSPI open→close same sign ~70% (30% intraday reversals — do NOT assume gap = close)
3. Yesterday's news is ALREADY reflected in overnight futures prices. Do NOT double-count news + futures as separate factors.
4. MU↔SK Hynix actual lagged correlation ≈ 0.65~0.80 (strong next-day predictor)
5. SOX↔Korean semis > 0.6 + SOX up → semiconductor LONG signal
6. Foreign 3+ consecutive days net buy + KRW strengthening → strong LONG
7. VIX > 25 + KRW weakening + foreign selling → SHORT
8. Correlation breakdown (r < 0.3) = decoupling → focus on Korea-specific variables
9. DXY up + KRW down simultaneously = EM capital outflow risk
10. Geopolitical risk: Iran mention alone = low weight, only actual military conflict = high weight"""


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
    if spf_match:
        spf_chg = float(spf_match.group(1))
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

    # Determine direction — always pick a side
    if bull_signals >= bear_signals:
        direction = "long"
        long_pct = min(85, 51 + (bull_signals - bear_signals) * 5)
    else:
        direction = "short"
        long_pct = max(15, 49 - (bear_signals - bull_signals) * 5)

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
