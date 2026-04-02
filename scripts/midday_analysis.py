#!/usr/bin/env python3
"""
nydad-bot — Midday KOSPI Analysis (오후 시황 예측)

Runs at 12:00 KST (03:00 UTC), AFTER the morning session (9:00-12:00).
Appends `midday_signal` to the existing daily JSON file.

Data sources:
  1. KOSPI morning session data (9:00-12:00) — intraday via yfinance
  2. 11:00 AM candle direction (30min/1hr) — the "11시 캔들 법칙"
  3. Korean morning news (domestic RSS feeds)
  4. Foreign investor flow (KRX real-time data)
  5. Morning 7AM signal (for comparison/update)

Output: midday_signal dict appended to data/YYYY-MM-DD.json
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

try:
    import pytz
except ImportError:
    pytz = None

try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    import feedparser
except ImportError:
    feedparser = None

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("midday")

HEADERS = {"User-Agent": "NydadBot/2.0 (github.com/nydad/nydad-bot)"}

# Korean domestic news RSS feeds for morning news
KR_NEWS_FEEDS = [
    ("한경", "https://www.hankyung.com/feed/finance"),
    ("매경", "https://www.mk.co.kr/rss/50200011/"),
    ("이데일리", "https://rss.edaily.co.kr/edaily/economy/Stock/"),
    ("연합인포맥스", "https://news.einfomax.co.kr/rss/S1N2.xml"),
]


# ---------------------------------------------------------------------------
# Phase 1: KOSPI Intraday Data + 11AM Candle
# ---------------------------------------------------------------------------
def fetch_morning_session() -> dict:
    """Fetch KOSPI morning session data including 11AM candle."""
    log.info("=== Phase 1: Morning Session Data ===")
    result = {
        "kospi_current": None,
        "kospi_open": None,
        "kospi_change_pct": None,
        "morning_high": None,
        "morning_low": None,
        "candle_11am": None,  # The special sauce
        "morning_trend": None,
        "volume_ratio": None,
    }

    if not yf:
        log.warning("yfinance not installed")
        return result

    try:
        # Fetch 30-minute intraday data for today
        kospi = yf.download(
            "^KS11",
            period="1d",
            interval="30m",
            progress=False,
            timeout=15,
        )

        if kospi.empty:
            log.warning("No intraday KOSPI data available")
            return result

        # Open, Close, High, Low 모두 사용 (Open 누락 시 Close로 대체하지 않음)
        opens = kospi["Open"].dropna()
        close = kospi["Close"].dropna()
        high = kospi["High"].dropna()
        low = kospi["Low"].dropna()
        volume = kospi["Volume"].dropna()

        if len(close) < 2 or len(opens) < 1:
            log.warning("Insufficient intraday data points: close=%d, open=%d", len(close), len(opens))
            return result

        # 실제 시가 = 첫 번째 Open (Close가 아님!)
        result["kospi_open"] = round(float(opens.iloc[0]), 2)
        result["kospi_current"] = round(float(close.iloc[-1]), 2)
        result["morning_high"] = round(float(high.max()), 2)
        result["morning_low"] = round(float(low.min()), 2)

        # 시초가 대비 변동률
        open_price = float(opens.iloc[0])
        current = float(close.iloc[-1])
        result["kospi_change_pct"] = round(((current - open_price) / open_price) * 100, 2) if open_price > 0 else 0

        # Morning trend
        changes = close.pct_change().dropna()
        up_candles = sum(1 for c in changes if c > 0)
        down_candles = sum(1 for c in changes if c <= 0)
        total_candles = up_candles + down_candles
        if total_candles > 0:
            up_ratio = up_candles / total_candles
            if up_ratio >= 0.7:
                result["morning_trend"] = "strong_up"
            elif up_ratio >= 0.55:
                result["morning_trend"] = "mild_up"
            elif up_ratio <= 0.3:
                result["morning_trend"] = "strong_down"
            elif up_ratio <= 0.45:
                result["morning_trend"] = "mild_down"
            else:
                result["morning_trend"] = "choppy"

        # 11AM Candle (핵심!)
        # yfinance KRX 데이터는 Asia/Seoul 타임존으로 반환됨
        # tz-aware인 경우 KST로 변환, tz-naive인 경우 KST로 가정
        for i, ts in enumerate(opens.index):
            # 타임존 처리: tz-aware → KST 변환, tz-naive → 그대로 (KRX는 KST)
            if hasattr(ts, 'tz') and ts.tz is not None and pytz:
                kst_tz = pytz.timezone("Asia/Seoul")
                ts_local = ts.astimezone(kst_tz)
                hour, minute = ts_local.hour, ts_local.minute
            else:
                hour = ts.hour if hasattr(ts, 'hour') else 0
                minute = ts.minute if hasattr(ts, 'minute') else 0

            if hour == 11 and minute == 0:
                # 11시 30분봉: Open/Close/High/Low 모두 사용
                candle_open = float(opens.iloc[i])
                candle_close = float(close.iloc[i])
                candle_high = float(high.iloc[i])
                candle_low = float(low.iloc[i])
                candle_body = candle_close - candle_open

                result["candle_11am"] = {
                    "open": round(candle_open, 2),
                    "close": round(candle_close, 2),
                    "high": round(candle_high, 2),
                    "low": round(candle_low, 2),
                    "direction": "양봉" if candle_body > 0 else "음봉" if candle_body < 0 else "보합",
                    "body_pct": round(abs(candle_body) / candle_open * 100, 3) if candle_open > 0 else 0,
                    "signal": "bullish" if candle_body > 0 else "bearish" if candle_body < 0 else "neutral",
                }
                log.info("11AM Candle: %s (body %.3f%%)",
                         result["candle_11am"]["direction"],
                         result["candle_11am"]["body_pct"])
                break

        # Volume ratio — skip extra yfinance call, use intraday volume only
        if len(volume) >= 2:
            result["morning_volume"] = int(volume.sum())

        log.info("KOSPI morning: %.2f → %.2f (%+.2f%%), trend=%s",
                 result["kospi_open"] or 0, result["kospi_current"] or 0,
                 result["kospi_change_pct"] or 0, result["morning_trend"])

    except Exception as e:
        log.error("Morning session fetch failed: %s", e)

    return result


# ---------------------------------------------------------------------------
# Phase 2: Foreign Investor Flow (Live)
# ---------------------------------------------------------------------------
def fetch_live_foreign_flow() -> dict:
    """Fetch live foreign investor flow from KRX during trading hours."""
    log.info("=== Phase 2: Live Foreign Flow ===")
    result = {
        "net_amount": None,
        "direction": None,
        "source": "unavailable",
    }

    today = datetime.now(KST)
    if today.weekday() >= 5:
        today -= timedelta(days=(today.weekday() - 4))
    trd_date = today.strftime("%Y%m%d")

    try:
        url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        payload = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT02203",
            "locale": "ko_KR",
            "trdDd": trd_date,
            "mktId": "STK",
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

        for item in data.get("output", []):
            name = item.get("INVST_TP_NM", "")
            net_val = item.get("NETBVSQTY", "0").replace(",", "")
            try:
                net_amount = int(net_val)
            except ValueError:
                continue
            if "외국인" in name:
                result["net_amount"] = round(net_amount / 1_000_000, 1)
                result["direction"] = "buy" if net_amount > 0 else "sell" if net_amount < 0 else "neutral"
                result["source"] = "krx_live"
                log.info("Foreign flow: %s %.1f million shares",
                         result["direction"], abs(result["net_amount"]))
            elif "기관" in name:
                result["institutional"] = round(int(net_val) / 1_000_000, 1)

    except Exception as e:
        log.warning("KRX live flow failed: %s", e)

    return result


# ---------------------------------------------------------------------------
# Phase 3: Korean Morning News
# ---------------------------------------------------------------------------
def fetch_morning_news() -> list:
    """Fetch Korean domestic news headlines from morning (9AM-12PM)."""
    log.info("=== Phase 3: Morning News ===")
    articles = []

    if not feedparser:
        log.warning("feedparser not installed")
        return articles

    now = datetime.now(KST)
    cutoff = now - timedelta(hours=6)  # last 6 hours = since ~6AM

    for source_name, feed_url in KR_NEWS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:10]:
                title = entry.get("title", "").strip()
                if not title:
                    continue

                # Check publish time
                published = entry.get("published_parsed") or entry.get("updated_parsed")
                if published:
                    pub_dt = datetime(*published[:6], tzinfo=KST)
                    if pub_dt < cutoff:
                        continue

                articles.append({
                    "source": source_name,
                    "title": title,
                    "description": entry.get("summary", entry.get("description", ""))[:200],
                })
        except Exception as e:
            log.warning("RSS feed %s failed: %s", source_name, e)

    log.info("Morning news: %d articles from %d feeds", len(articles), len(KR_NEWS_FEEDS))
    return articles[:20]  # cap at 20


# ---------------------------------------------------------------------------
# Phase 4: AI Midday Analysis
# ---------------------------------------------------------------------------
MIDDAY_SYSTEM_PROMPT = """You are a hedge fund quant trader. It is now noon (12:10 KST). The Korean morning session (9:00-12:00) has ended.

## Role: Afternoon market forecast
Synthesize morning data (KOSPI 9:00-12:00 price action, 11 AM candle, foreign investor flow, domestic news) to predict the **afternoon closing direction**.

## 11 AM 60-min candle (backtested)
- Bullish 11 AM candle → afternoon rally 71%, full-day up 68% (significant signal)
- Bearish 11 AM candle → afternoon decline only 42% (58% reversal — low reliability)
- Conclusion: **trust bullish candles, but bearish candles frequently reverse**
- Strong candle (body > 0.1%) increases reliability to 61%

## Difference from 7 AM pre-market analysis
- The 7 AM analysis was based on overnight futures + US close data.
- Now the market has actually opened and we have real intraday data.
- Compare the 7 AM prediction with actual morning price action. Update the afternoon forecast.

## Backtested gap statistics (2026-01~03)
- Gap up > 0.3%: close > prev close 100% of the time (N=28) — gap direction holds
- Gap down < -0.3%: close < prev close 71% (N=21) — less reliable
- BUT intraday direction (close vs open) is 52% — gap holds but does NOT extend
- 삼성전자 > +1% → KOSPI next day up 85% (N=26) — Samsung leads index

## Rules
1. MUST pick LONG or SHORT. Neutral forbidden.
2. NO platitudes. Specific numbers required.
3. Foreign investor flow is the KEY variable for intraday direction changes.
4. Do NOT simply extrapolate morning trend. Morning decline + bullish 11 AM candle = afternoon reversal possible.
5. If morning gap was large (>1%), the gap direction has 87~100% chance of matching close direction — strong confidence warranted.

## JSON response format
ALL text fields must be in KOREAN (한국어).
{
  "direction": "long" or "short",
  "long_pct": 51~85,
  "short_pct": 15~49,
  "confidence": 0.5~0.9,
  "summary": "3 sentences IN KOREAN. Afternoon forecast with 11AM candle + foreign flow + direction.",
  "candle_11am_interpretation": "1 sentence IN KOREAN. 11AM candle interpretation.",
  "morning_review": "1 sentence IN KOREAN. 7AM prediction vs actual morning comparison.",
  "afternoon_catalyst": "1 sentence IN KOREAN. Key afternoon pivot variable.",
  "factors": [
    {"name": "factor name in Korean", "signal": "bullish/bearish", "detail": "specific numbers in Korean"}
  ]
}"""


def generate_midday_insight(context: str) -> dict:
    """Generate AI midday analysis."""
    log.info("=== Phase 4: AI Midday Insight ===")

    if not API_KEY:
        log.error("OPENROUTER_API_KEY not set")
        return _midday_fallback(context)

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/nydad/nydad-bot",
        "X-Title": "Nydad Bot Midday",
    }

    user_prompt = f"""아래는 오늘 오전 세션(9:00-12:00)의 데이터입니다.
이를 종합하여 오후 장 마감까지의 방향을 예측하세요.

{context}

위 데이터를 분석하여 JSON 형식으로 응답하세요."""

    payload = {
        "model": MODEL_QUALITY,
        "messages": [
            {"role": "system", "content": MIDDAY_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.3,
        "max_tokens": 2048,
        "response_format": {"type": "json_object"},
    }

    for attempt in range(3):
        try:
            resp = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=120,
            )
            if resp.status_code == 429:
                time.sleep(min(int(resp.headers.get("Retry-After", 10)), 30))
                continue
            if resp.status_code >= 500:
                time.sleep(5 * (2 ** attempt))
                continue
            resp.raise_for_status()

            content = resp.json()["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                content = "\n".join(content.split("\n")[1:])
                if content.rstrip().endswith("```"):
                    content = content.rstrip()[:-3]
                content = content.strip()

            result = json.loads(content)

            # Validate & reconcile direction with percentages
            direction = result.get("direction", "long")
            if direction not in ("long", "short"):
                direction = "long"
            result["direction"] = direction
            long_pct = max(15, min(85, int(result.get("long_pct", 55))))
            # direction과 percentage 정합성 보장
            if direction == "long" and long_pct < 51:
                long_pct = 51
            elif direction == "short" and long_pct > 49:
                long_pct = 49
            result["long_pct"] = long_pct
            result["short_pct"] = 100 - long_pct
            result["confidence"] = max(0.5, min(0.9, float(result.get("confidence", 0.6))))

            log.info("Midday AI: %s (conf: %.2f)", result["direction"], result["confidence"])
            return result

        except json.JSONDecodeError as e:
            log.warning("JSON parse error (attempt %d): %s", attempt + 1, e)
        except Exception as e:
            log.warning("API fail (attempt %d): %s", attempt + 1, e)

    return _midday_fallback(context)


def _midday_fallback(context: str) -> dict:
    """Rule-based fallback for midday analysis."""
    bull = 0
    bear = 0
    factors = []

    # Parse 11AM candle
    if "양봉" in context:
        bull += 2  # 11시 캔들은 가중치 높음
        factors.append({"name": "11시 캔들 양봉", "signal": "bullish", "detail": "오후 상승 확률 높음"})
    elif "음봉" in context:
        bear += 2
        factors.append({"name": "11시 캔들 음봉", "signal": "bearish", "detail": "오후 추가 하락 확률 높음"})

    # Parse morning trend
    if "strong_up" in context:
        bull += 1
        factors.append({"name": "오전 강한 상승", "signal": "bullish", "detail": "상승 모멘텀 지속"})
    elif "strong_down" in context:
        bear += 1
        factors.append({"name": "오전 강한 하락", "signal": "bearish", "detail": "하락 모멘텀 지속"})

    # Parse foreign flow (한국어 context이므로 한국어로 검색)
    if "순매수" in context:
        bull += 1
        factors.append({"name": "외국인 순매수", "signal": "bullish", "detail": "장중 외국인 매수 유입"})
    elif "순매도" in context:
        bear += 1
        factors.append({"name": "외국인 순매도", "signal": "bearish", "detail": "장중 외국인 매도 이탈"})

    direction = "long" if bull >= bear else "short"
    long_pct = min(85, 51 + (bull - bear) * 5) if direction == "long" else max(15, 49 - (bear - bull) * 5)

    return {
        "direction": direction,
        "long_pct": long_pct,
        "short_pct": 100 - long_pct,
        "confidence": round(max(bull, bear) / max(bull + bear, 1) * 0.8, 2),
        "summary": f"룰 기반 오후 전망: 강세 {bull}개 / 약세 {bear}개",
        "candle_11am_interpretation": "AI 미응답 — 11시 캔들 데이터 참고",
        "morning_review": "AI 미응답",
        "afternoon_catalyst": "AI 미응답 — 외국인 수급 동향 주시",
        "factors": factors,
        "_fallback": True,
    }


# ---------------------------------------------------------------------------
# Build Context
# ---------------------------------------------------------------------------
def build_midday_context(session: dict, flow: dict, news: list, morning_signal: dict) -> str:
    """Build context string for midday AI analysis."""
    sections = []
    now = datetime.now(KST)
    sections.append(f"=== 오후 시황 분석: {now.strftime('%Y-%m-%d %H:%M KST')} ===\n")

    # Morning session data
    sections.append("=== KOSPI 오전 세션 (9:00-12:00) ===")
    if session.get("kospi_open"):
        sections.append(f"  시가: {session['kospi_open']}")
        sections.append(f"  현재가: {session['kospi_current']}")
        sections.append(f"  오전 등락: {session['kospi_change_pct']:+.2f}%")
        sections.append(f"  오전 고가: {session['morning_high']}")
        sections.append(f"  오전 저가: {session['morning_low']}")
        sections.append(f"  오전 추세: {session['morning_trend']}")
        if session.get("volume_ratio"):
            sections.append(f"  거래량 비율: {session['volume_ratio']:.2f}x (vs 5일 평균)")

    # 11AM Candle (핵심!)
    sections.append("\n=== 11시 캔들 (핵심 지표!) ===")
    candle = session.get("candle_11am")
    if candle:
        sections.append(f"  방향: {candle['direction']}")
        sections.append(f"  시가: {candle['open']} → 종가: {candle['close']}")
        sections.append(f"  고가: {candle['high']} / 저가: {candle['low']}")
        sections.append(f"  몸통 크기: {candle['body_pct']:.3f}%")
        sections.append(f"  시그널: {candle['signal']}")
        sections.append("  ⚠️ 11시 캔들 방향은 장마감까지의 방향성과 높은 상관관계가 있습니다!")
    else:
        sections.append("  데이터 미수집 — 시장 미개장 또는 데이터 지연")

    # Foreign flow
    sections.append("\n=== 외국인 수급 (장중) ===")
    if flow.get("net_amount") is not None:
        dir_kr = "순매수" if flow["direction"] == "buy" else "순매도"
        sections.append(f"  방향: {dir_kr}")
        sections.append(f"  규모: {abs(flow['net_amount']):.1f} 백만주")
        if flow.get("institutional") is not None:
            inst_dir = "순매수" if flow["institutional"] > 0 else "순매도"
            sections.append(f"  기관: {inst_dir} {abs(flow['institutional']):.1f} 백만주")
    else:
        sections.append("  수급 데이터 미수집")

    # Morning news
    if news:
        sections.append(f"\n=== 오전 국내 뉴스 ({len(news)}건) ===")
        for a in news[:15]:
            sections.append(f"  [{a['source']}] {a['title']}")
            if a.get("description"):
                sections.append(f"    → {a['description'][:120]}")

    # Morning 7AM signal for comparison
    if morning_signal:
        sections.append("\n=== 오전 7시 장전 시황 (비교용) ===")
        sections.append(f"  예측 방향: {morning_signal.get('direction', '?').upper()}")
        sections.append(f"  Long/Short: {morning_signal.get('long_pct', 50)}% / {morning_signal.get('short_pct', 50)}%")
        sections.append(f"  요약: {morning_signal.get('summary', 'N/A')[:200]}")
        sections.append(f"  핵심 인사이트: {morning_signal.get('key_insight', 'N/A')[:200]}")

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log.info("=" * 60)
    log.info("  Midday KOSPI Analysis — 오후 시황 예측")
    log.info("=" * 60)

    now = datetime.now(KST)
    today_str = now.strftime("%Y-%m-%d")
    daily_path = DATA_DIR / f"{today_str}.json"

    # Load existing daily data (from 7AM run)
    daily_data = {}
    if daily_path.exists():
        try:
            daily_data = json.loads(daily_path.read_text(encoding="utf-8"))
            log.info("Loaded existing daily data: %s", daily_path.name)
        except Exception as e:
            log.warning("Could not load daily data: %s", e)

    morning_signal = daily_data.get("investment_signal") or daily_data.get("kospi_signal") or {}

    # Phase 1: Morning session data + 11AM candle
    session = fetch_morning_session()

    # Phase 2: Live foreign flow
    flow = fetch_live_foreign_flow()

    # Phase 3: Morning news
    news = fetch_morning_news()

    # Build context
    context = build_midday_context(session, flow, news, morning_signal)
    log.info("Context: %d chars", len(context))

    # Phase 4: AI analysis
    midday = generate_midday_insight(context)

    # Add raw data to the result
    midday["_raw"] = {
        "session": session,
        "foreign_flow": flow,
        "news_count": len(news),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # Append to daily JSON
    daily_data["midday_signal"] = midday
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(daily_path, "w", encoding="utf-8") as f:
        json.dump(daily_data, f, ensure_ascii=False, indent=2)

    log.info("Saved midday_signal to %s", daily_path.name)

    # Print summary
    print(f"\n{'='*60}")
    print(f"  오후 시황 예측: {midday['direction'].upper()}")
    print(f"  Long/Short: {midday['long_pct']}% / {midday['short_pct']}%")
    print(f"  Confidence: {midday['confidence']:.2f}")
    if midday.get("summary"):
        print(f"\n  {midday['summary']}")
    if midday.get("candle_11am_interpretation"):
        print(f"\n  11시 캔들: {midday['candle_11am_interpretation']}")
    if midday.get("morning_review"):
        print(f"  오전 리뷰: {midday['morning_review']}")
    if midday.get("afternoon_catalyst"):
        print(f"  오후 변수: {midday['afternoon_catalyst']}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
