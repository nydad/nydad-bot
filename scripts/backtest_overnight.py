from __future__ import annotations

import os
import sys
import tempfile
from typing import Iterable

import numpy as np
import pandas as pd
import yfinance as yf

PERIOD = "60d"
INTERVAL = "1d"
WINDOW = 30

KOSPI_TICKER = "^KS11"
PROXIES = {
    "ES=F": "S&P futures proxy",
    "NQ=F": "Nasdaq futures proxy",
    "^SOX": "PHLX Semiconductor Index",
}

PROXY_ENV_VARS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)


def clear_dead_proxy_env() -> None:
    # This environment injects a loopback proxy that blocks Yahoo requests.
    for name in PROXY_ENV_VARS:
        os.environ.pop(name, None)


def configure_yfinance_cache() -> None:
    cache_dir = os.path.join(tempfile.gettempdir(), "py-yfinance-codex")
    os.makedirs(cache_dir, exist_ok=True)
    yf.set_tz_cache_location(cache_dir)


def download_daily_ohlc(ticker: str) -> pd.DataFrame:
    df = yf.download(
        ticker,
        period=PERIOD,
        interval=INTERVAL,
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if df.empty:
        raise RuntimeError(f"No data returned for {ticker}.")

    if isinstance(df.columns, pd.MultiIndex):
        if df.columns.get_level_values(-1).nunique() == 1:
            df.columns = df.columns.get_level_values(0)
        else:
            raise RuntimeError(f"Unexpected multi-index columns returned for {ticker}.")

    required = {"Open", "Close"}
    missing = required.difference(df.columns)
    if missing:
        raise RuntimeError(f"{ticker} is missing required columns: {sorted(missing)}")

    df = df.loc[:, ["Open", "Close"]].copy()
    index = pd.DatetimeIndex(pd.to_datetime(df.index))
    if index.tz is not None:
        index = index.tz_localize(None)
    df.index = index.normalize()
    df.index.name = "Date"
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


def build_kospi_features(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["PrevClose"] = data["Close"].shift(1)
    data["OpenGapPct"] = data["Open"] / data["PrevClose"] - 1.0
    data["CloseFromPrevPct"] = data["Close"] / data["PrevClose"] - 1.0
    data["OpenDirection"] = np.sign(data["OpenGapPct"])
    data["CloseDirection"] = np.sign(data["CloseFromPrevPct"])
    return data.dropna(subset=["PrevClose", "OpenGapPct", "CloseFromPrevPct"])


def build_signal_features(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["SignalPct"] = data["Close"].pct_change()
    data["SignalDirection"] = np.sign(data["SignalPct"])
    return data.dropna(subset=["SignalPct"])


def align_to_next_kospi_open(signal_df: pd.DataFrame, kospi_df: pd.DataFrame) -> pd.DataFrame:
    signal = signal_df.reset_index()[["Date", "SignalPct", "SignalDirection"]].rename(
        columns={"Date": "SignalDate"}
    )
    kospi = kospi_df.reset_index()[
        ["Date", "OpenGapPct", "OpenDirection", "CloseFromPrevPct", "CloseDirection"]
    ].rename(columns={"Date": "KospiDate"})

    aligned = pd.merge_asof(
        signal.sort_values("SignalDate"),
        kospi.sort_values("KospiDate"),
        left_on="SignalDate",
        right_on="KospiDate",
        direction="forward",
        allow_exact_matches=False,
    )
    aligned = aligned.dropna(subset=["KospiDate", "OpenGapPct"]).copy()

    # Keep the most recent U.S. session when multiple signals map to the same KOSPI open.
    aligned = aligned.sort_values(["KospiDate", "SignalDate"]).drop_duplicates(
        subset=["KospiDate"], keep="last"
    )
    return aligned.sort_values("KospiDate").reset_index(drop=True)


def take_window(df: pd.DataFrame, label: str, window: int = WINDOW) -> pd.DataFrame:
    if df.empty:
        raise RuntimeError(f"No aligned observations available for {label}.")
    if len(df) < window:
        print(
            f"Warning: only {len(df)} observations available for {label}; using all available rows.",
            file=sys.stderr,
        )
    return df.tail(min(window, len(df))).copy()


def direction_hit_rate(a: Iterable[float], b: Iterable[float]) -> float:
    frame = pd.DataFrame({"a": list(a), "b": list(b)}).dropna()
    if frame.empty:
        return float("nan")
    return float((np.sign(frame["a"]) == np.sign(frame["b"])).mean())


def fmt_pct(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{value * 100:.2f}%"


def fmt_float(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{value:.4f}"


def main() -> int:
    clear_dead_proxy_env()
    configure_yfinance_cache()
    kospi = build_kospi_features(download_daily_ohlc(KOSPI_TICKER))

    rows = []
    for ticker, label in PROXIES.items():
        signal = build_signal_features(download_daily_ohlc(ticker))
        aligned = align_to_next_kospi_open(signal, kospi)
        sample = take_window(aligned, f"{ticker} -> next-day {KOSPI_TICKER} open")

        rows.append(
            {
                "Pair": f"{ticker} -> next-day {KOSPI_TICKER} open",
                "Proxy": label,
                "Observations": len(sample),
                "Correlation": sample["SignalPct"].corr(sample["OpenGapPct"]),
                "Direction Hit Rate": direction_hit_rate(
                    sample["SignalPct"], sample["OpenGapPct"]
                ),
                "KOSPI Window": (
                    f"{sample['KospiDate'].min().date()} to {sample['KospiDate'].max().date()}"
                ),
            }
        )

    summary = pd.DataFrame(rows)

    kospi_window = take_window(kospi, f"{KOSPI_TICKER} open -> close")
    kospi_open_to_close_hit_rate = direction_hit_rate(
        kospi_window["OpenGapPct"], kospi_window["CloseFromPrevPct"]
    )

    print("Overnight Proxy vs. Next-Day KOSPI Open")
    print(f"Source period: {PERIOD}, interval: {INTERVAL}, window: last {WINDOW} KOSPI trading days")
    print()
    print(
        summary.to_string(
            index=False,
            formatters={
                "Correlation": fmt_float,
                "Direction Hit Rate": fmt_pct,
            },
        )
    )
    print()
    print("KOSPI Directional Follow-Through")
    print(
        pd.DataFrame(
            [
                {
                    "Metric": "KOSPI opening direction -> same-day KOSPI closing direction",
                    "Observations": len(kospi_window),
                    "Hit Rate": kospi_open_to_close_hit_rate,
                    "Window": f"{kospi_window.index.min().date()} to {kospi_window.index.max().date()}",
                }
            ]
        ).to_string(index=False, formatters={"Hit Rate": fmt_pct})
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
