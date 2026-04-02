#!/usr/bin/env python3
"""
Backtest: 11AM Candle Direction vs Closing Direction
Tests the hypothesis that KOSPI 11:00 AM candle direction predicts closing direction.

Tests both 30-minute and 60-minute candles.
"""
import sys
from datetime import datetime, timedelta, timezone

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("pip install yfinance pandas")
    sys.exit(1)


def backtest_candle(interval: str, label: str):
    """Run backtest for a specific candle interval."""
    print(f"\n{'='*65}")
    print(f"  {label} 기반 — 11시 캔들 vs 장마감 방향 백테스트")
    print(f"{'='*65}")

    # Fetch 30 trading days of intraday data
    # yfinance max for 30m is 60 days, for 60m is 730 days
    kospi = yf.download(
        "^KS11",
        period="60d",
        interval=interval,
        progress=False,
        timeout=30,
    )

    if kospi.empty:
        print("  데이터 없음")
        return

    # Also fetch daily data for actual open/close
    daily = yf.download("^KS11", period="60d", interval="1d", progress=False, timeout=15)

    print(f"  데이터: {len(kospi)} intraday bars, {len(daily)} daily bars\n")

    # Convert index to KST for matching
    kospi_copy = kospi.copy()
    if kospi_copy.index.tz is not None:
        kospi_copy.index = kospi_copy.index.tz_convert("Asia/Seoul").tz_localize(None)
    kospi_copy["date"] = kospi_copy.index.date

    daily_copy = daily.copy()
    if daily_copy.index.tz is not None:
        daily_copy.index = daily_copy.index.tz_convert("Asia/Seoul").tz_localize(None)
    else:
        daily_copy.index = daily_copy.index.tz_localize(None) if hasattr(daily_copy.index, 'tz') else daily_copy.index

    results = []

    for date, group in kospi_copy.groupby("date"):
        # Find 11:00 KST candle
        candle_11 = None
        for idx, row in group.iterrows():
            h = idx.hour
            m = idx.minute
            if h == 11 and m == 0:
                candle_11 = row
                break

        if candle_11 is None:
            continue

        # 11AM candle direction (Open vs Close of that candle)
        c_open = float(candle_11["Open"])
        c_close = float(candle_11["Close"])
        if c_open == 0:
            continue
        candle_return = (c_close - c_open) / c_open * 100
        candle_dir = "양봉" if candle_return > 0.02 else "음봉" if candle_return < -0.02 else "보합"

        # Day's open and close (from daily data)
        date_pd = pd.Timestamp(date)
        daily_match = daily_copy.loc[daily_copy.index.date == date]
        if daily_match.empty:
            continue

        day_open = float(daily_match["Open"].iloc[0])
        day_close = float(daily_match["Close"].iloc[0])
        if day_open == 0:
            continue
        day_return = (day_close - day_open) / day_open * 100

        # Closing direction from 11AM onward
        # Find bars after 11AM candle to get afternoon movement
        afternoon = group[group.index.hour >= 11]
        if len(afternoon) < 2:
            continue
        price_at_11_close = float(afternoon["Close"].iloc[0])  # 11시 캔들 종가
        price_at_market_close = float(afternoon["Close"].iloc[-1])  # 장마감 종가
        if price_at_11_close == 0:
            continue
        afternoon_return = (price_at_market_close - price_at_11_close) / price_at_11_close * 100

        # Full day direction (시초가 대비 종가)
        day_dir = "상승" if day_return > 0.05 else "하락" if day_return < -0.05 else "보합"

        # 11시 이후 방향
        pm_dir = "상승" if afternoon_return > 0.05 else "하락" if afternoon_return < -0.05 else "보합"

        # Match check
        candle_bullish = candle_return > 0.02
        candle_bearish = candle_return < -0.02
        pm_up = afternoon_return > 0.05
        pm_down = afternoon_return < -0.05

        match_pm = (candle_bullish and pm_up) or (candle_bearish and pm_down)
        match_day = (candle_bullish and day_return > 0.05) or (candle_bearish and day_return < -0.05)

        results.append({
            "date": str(date),
            "candle_dir": candle_dir,
            "candle_pct": round(candle_return, 3),
            "pm_dir": pm_dir,
            "pm_pct": round(afternoon_return, 3),
            "day_dir": day_dir,
            "day_pct": round(day_return, 3),
            "match_pm": match_pm,
            "match_day": match_day,
            "candle_neutral": not candle_bullish and not candle_bearish,
        })

    if not results:
        print("  분석 가능한 데이터 없음")
        return

    # Print results table
    print(f"  {'날짜':>12} | {'11시캔들':>6} | {'캔들%':>7} | {'오후방향':>6} | {'오후%':>7} | {'종일방향':>6} | {'종일%':>7} | {'오후일치':>6} | {'종일일치':>6}")
    print("  " + "-" * 95)

    for r in results[-30:]:  # last 30 trading days
        pm_mark = "O" if r["match_pm"] else ("—" if r["candle_neutral"] else "X")
        day_mark = "O" if r["match_day"] else ("—" if r["candle_neutral"] else "X")
        print(f"  {r['date']:>12} | {r['candle_dir']:>6} | {r['candle_pct']:>+7.3f}% | {r['pm_dir']:>6} | {r['pm_pct']:>+7.3f}% | {r['day_dir']:>6} | {r['day_pct']:>+7.3f}% | {pm_mark:>6} | {day_mark:>6}")

    # Statistics
    directional = [r for r in results if not r["candle_neutral"]]
    total = len(directional)
    if total == 0:
        print("\n  방향성 있는 캔들 없음")
        return

    pm_matches = sum(1 for r in directional if r["match_pm"])
    day_matches = sum(1 for r in directional if r["match_day"])
    neutral_count = sum(1 for r in results if r["candle_neutral"])

    print(f"\n  {'='*50}")
    print(f"  총 분석일: {len(results)}일 (방향성 캔들: {total}일, 보합: {neutral_count}일)")
    print(f"  11시 캔들 → 오후 방향 일치: {pm_matches}/{total} = {pm_matches/total*100:.1f}%")
    print(f"  11시 캔들 → 종일 방향 일치: {day_matches}/{total} = {day_matches/total*100:.1f}%")

    # Breakdown by candle direction
    bulls = [r for r in directional if r["candle_pct"] > 0.02]
    bears = [r for r in directional if r["candle_pct"] < -0.02]

    if bulls:
        bull_pm = sum(1 for r in bulls if r["match_pm"])
        bull_day = sum(1 for r in bulls if r["match_day"])
        print(f"\n  양봉 ({len(bulls)}일): 오후 일치 {bull_pm}/{len(bulls)} = {bull_pm/len(bulls)*100:.1f}%, 종일 일치 {bull_day}/{len(bulls)} = {bull_day/len(bulls)*100:.1f}%")

    if bears:
        bear_pm = sum(1 for r in bears if r["match_pm"])
        bear_day = sum(1 for r in bears if r["match_day"])
        print(f"  음봉 ({len(bears)}일): 오후 일치 {bear_pm}/{len(bears)} = {bear_pm/len(bears)*100:.1f}%, 종일 일치 {bear_day}/{len(bears)} = {bear_day/len(bears)*100:.1f}%")

    # Strong candles (body > 0.1%)
    strong = [r for r in directional if abs(r["candle_pct"]) > 0.1]
    if strong:
        strong_pm = sum(1 for r in strong if r["match_pm"])
        strong_day = sum(1 for r in strong if r["match_day"])
        print(f"\n  강한 캔들 (|body|>0.1%, {len(strong)}일):")
        print(f"    오후 일치: {strong_pm}/{len(strong)} = {strong_pm/len(strong)*100:.1f}%")
        print(f"    종일 일치: {strong_day}/{len(strong)} = {strong_day/len(strong)*100:.1f}%")


if __name__ == "__main__":
    backtest_candle("30m", "KOSPI 11시 30분봉")
    backtest_candle("60m", "KOSPI 11시 60분봉")
    print()
