#!/usr/bin/env python3
"""
Backtest: Analyze historical signal data to verify short-bias fix.
Reads existing data/*.json files and simulates the old vs new signal logic.
"""
import json
import sys
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def analyze_stored_signals():
    """Analyze stored daily signals from data/*.json"""
    files = sorted(DATA_DIR.glob("2026-*.json"))
    if not files:
        print("No data files found")
        return

    print(f"{'='*60}")
    print(f"  Signal Balance Backtest — {len(files)} days")
    print(f"{'='*60}\n")

    directions = {"long": 0, "short": 0, "neutral": 0}
    daily_results = []

    for f in files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue

        date = f.stem

        # Check investment_signal or kospi_signal
        signal = data.get("investment_signal") or data.get("kospi_signal") or {}
        direction = signal.get("direction", "unknown")
        confidence = signal.get("confidence", 0)
        long_pct = signal.get("long_pct", 50)
        short_pct = signal.get("short_pct", 50)

        directions[direction] = directions.get(direction, 0) + 1
        daily_results.append({
            "date": date,
            "direction": direction,
            "long_pct": long_pct,
            "short_pct": short_pct,
            "confidence": confidence,
        })

        # Check factors for dedup analysis
        factors = signal.get("factors", [])
        bull_count = sum(1 for f in factors if f.get("signal") == "bullish")
        bear_count = sum(1 for f in factors if f.get("signal") == "bearish")

        # Check geo risk
        geo = signal.get("geo_risk", {})
        geo_level = geo.get("level", "low")
        geo_hits = geo.get("hit_count", 0)

        print(f"  {date}: {direction.upper():>7} (L{long_pct}/S{short_pct}, conf={confidence:.2f}) "
              f"bull={bull_count} bear={bear_count} geo={geo_level}({geo_hits})")

        # Simulate new dedup logic
        RISK_OFF = {"미 선물 약세", "미 증시 약세 마감", "KOSPI 하락 흐름",
                    "원화 약세", "SOX 반도체 약세", "달러 강세 (EM 자금유출 우려)"}
        NEWS = {"지정학 리스크 심각", "지정학 리스크 높음", "지정학 리스크 주의"}

        roff_bear = sum(1 for f in factors if f.get("name") in RISK_OFF and f.get("signal") == "bearish")
        news_bear = sum(1 for f in factors if f.get("name") in NEWS and f.get("signal") == "bearish")
        dedup = max(0, roff_bear - 2) + (max(0, news_bear - 1) if roff_bear > 0 and news_bear > 0 else 0)

        new_bear = max(0, bear_count - dedup)
        new_total = bull_count + new_bear + sum(1 for f in factors if f.get("signal") == "neutral")

        if bull_count >= new_total * 0.6:
            new_dir = "long"
        elif new_bear >= new_total * 0.6:
            new_dir = "short"
        elif bull_count > new_bear:
            new_dir = "long"
        elif new_bear > bull_count:
            new_dir = "short"
        else:
            new_dir = "neutral"

        print(f"           → NEW: {new_dir.upper():>7} (dedup removed {dedup} bear factors, "
              f"effective bull={bull_count} bear={new_bear})")

    print(f"\n{'='*60}")
    print(f"  SUMMARY (Original)")
    print(f"{'='*60}")
    for d, c in sorted(directions.items(), key=lambda x: -x[1]):
        bar = "█" * c
        print(f"  {d.upper():>7}: {c:2d} days  {bar}")

    total = sum(directions.values())
    if total > 0:
        short_pct = directions.get("short", 0) / total * 100
        long_pct = directions.get("long", 0) / total * 100
        print(f"\n  Short bias: {short_pct:.0f}% short vs {long_pct:.0f}% long")
        if short_pct > 70:
            print("  ⚠️  SEVERE SHORT BIAS DETECTED")
        elif short_pct > 60:
            print("  ⚠️  Moderate short bias")
        else:
            print("  ✓ Reasonably balanced")


if __name__ == "__main__":
    analyze_stored_signals()
