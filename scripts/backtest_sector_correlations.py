from __future__ import annotations

import json
import logging
import math
import os
import sys
import tempfile
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

PERIOD = "90d"
INTERVAL = "1d"
WINDOWS = (20, 60)
ROOT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_PATH = ROOT_DIR / "data" / "sector_correlations.json"

PROXY_ENV_VARS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)

ALTERNATE_TICKERS = {
    "CIENA": ["CIEN"],
}

SECTORS = [
    {
        "name": "Memory/Semiconductor",
        "us": ["MU", "WDC", "AMAT", "LRCX", "AVGO", "QCOM", "TXN", "MRVL", "^SOX"],
        "kr": ["005930.KS", "000660.KS"],
    },
    {
        "name": "2nd Battery / EV / Clean Energy",
        "us": ["TSLA", "ALB", "SQM", "ENPH", "FSLR", "PLUG"],
        "kr": ["373220.KS", "006400.KS"],
    },
    {
        "name": "Defense / Aerospace / Space",
        "us": ["LMT", "RTX", "NOC", "BA", "RKLB", "LUNR"],
        "kr": ["012450.KS", "047810.KS"],
    },
    {
        "name": "Robotics / Automation",
        "us": ["ISRG", "ROK", "ABB", "PATH"],
        "kr": ["^KS11"],
    },
    {
        "name": "Telecom / Optical Communication",
        "us": ["LUMN", "LITE", "COHR", "CIENA", "VIAV"],
        "kr": ["^KS11", "005930.KS"],
    },
    {
        "name": "Power Grid / Electrical Infrastructure",
        "us": ["ETRN", "VST", "CEG", "NRG", "ETN", "PWR"],
        "kr": ["^KS11", "034730.KS"],
    },
]

TICKER_LABELS = {
    "^SOX": "PHLX Semiconductor Index",
    "^KS11": "KOSPI",
    "005930.KS": "Samsung Electronics",
    "000660.KS": "SK Hynix",
    "373220.KS": "LG Energy Solution",
    "006400.KS": "Samsung SDI",
    "012450.KS": "Hanwha Aerospace",
    "047810.KS": "Korea Aerospace",
    "034730.KS": "HD Hyundai Electric",
}


def clear_dead_proxy_env() -> None:
    for name in PROXY_ENV_VARS:
        os.environ.pop(name, None)


def configure_yfinance_cache() -> None:
    cache_dir = os.path.join(tempfile.gettempdir(), "py-yfinance-codex")
    os.makedirs(cache_dir, exist_ok=True)
    yf.set_tz_cache_location(cache_dir)


def configure_logging() -> None:
    warnings.filterwarnings("ignore", category=FutureWarning)
    warnings.filterwarnings("ignore", category=UserWarning)
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)


def all_requested_tickers() -> list[str]:
    tickers: list[str] = []
    for sector in SECTORS:
        tickers.extend(sector["us"])
        tickers.extend(sector["kr"])
    return sorted(set(tickers))


def extract_close_series(df: pd.DataFrame, ticker: str) -> pd.Series:
    if df.empty:
        raise RuntimeError(f"No data returned for {ticker}.")

    close: Any
    if isinstance(df.columns, pd.MultiIndex):
        if "Close" in df.columns.get_level_values(0):
            close = df.xs("Close", axis=1, level=0)
        elif "Close" in df.columns.get_level_values(-1):
            close = df.xs("Close", axis=1, level=-1)
        else:
            raise RuntimeError(f"{ticker} is missing a Close column.")

        if isinstance(close, pd.DataFrame):
            if close.shape[1] != 1:
                raise RuntimeError(f"Unexpected multi-column Close series for {ticker}.")
            close = close.iloc[:, 0]
    else:
        if "Close" not in df.columns:
            raise RuntimeError(f"{ticker} is missing a Close column.")
        close = df["Close"]

    series = pd.Series(close, name="Close").astype(float).dropna()
    index = pd.DatetimeIndex(pd.to_datetime(series.index))
    if index.tz is not None:
        index = index.tz_localize(None)
    series.index = index.normalize()
    series.index.name = "Date"
    series = series[~series.index.duplicated(keep="last")].sort_index()
    if series.empty:
        raise RuntimeError(f"{ticker} returned no valid close values.")
    return series


def download_ticker_data(requested_ticker: str) -> dict[str, Any]:
    candidates = [requested_ticker, *ALTERNATE_TICKERS.get(requested_ticker, [])]
    errors: list[str] = []

    for actual_ticker in candidates:
        try:
            df = yf.download(
                actual_ticker,
                period=PERIOD,
                interval=INTERVAL,
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            close = extract_close_series(df, actual_ticker)
            returns = close.pct_change().dropna()
            returns.name = "Return"
            note = "ok"
            if actual_ticker != requested_ticker:
                note = f"used alternate ticker {actual_ticker}"

            return {
                "requested_ticker": requested_ticker,
                "downloaded_ticker": actual_ticker,
                "status": "ok",
                "note": note,
                "close": close,
                "returns": returns,
                "observations": int(len(close)),
                "first_date": iso_date(close.index.min()),
                "last_date": iso_date(close.index.max()),
            }
        except Exception as exc:
            errors.append(f"{actual_ticker}: {exc}")

    return {
        "requested_ticker": requested_ticker,
        "downloaded_ticker": None,
        "status": "error",
        "note": " | ".join(errors) if errors else "Unknown download error.",
        "close": pd.Series(dtype=float),
        "returns": pd.Series(dtype=float),
        "observations": 0,
        "first_date": None,
        "last_date": None,
    }


def align_same_day(us_returns: pd.Series, kr_returns: pd.Series) -> pd.DataFrame:
    aligned = (
        pd.concat(
            [us_returns.rename("USReturn"), kr_returns.rename("KRReturn")],
            axis=1,
            join="inner",
        )
        .dropna()
        .sort_index()
    )
    aligned.index.name = "Date"
    return aligned


def align_lagged(us_returns: pd.Series, kr_returns: pd.Series) -> pd.DataFrame:
    us_df = us_returns.rename("USReturn").sort_index().reset_index()
    kr_df = kr_returns.rename("KRReturn").sort_index().reset_index()
    us_df.columns = ["USDate", "USReturn"]
    kr_df.columns = ["KRDate", "KRReturn"]

    aligned = pd.merge_asof(
        us_df.sort_values("USDate"),
        kr_df.sort_values("KRDate"),
        left_on="USDate",
        right_on="KRDate",
        direction="forward",
        allow_exact_matches=False,
    ).dropna(subset=["KRDate", "KRReturn"])

    # When the Korean market is closed, multiple U.S. sessions can point to the same
    # next Korean trading day. Keep only the most recent U.S. session for that date.
    aligned = aligned.sort_values(["KRDate", "USDate"]).drop_duplicates(
        subset=["KRDate"], keep="last"
    )
    aligned["GapDays"] = (aligned["KRDate"] - aligned["USDate"]).dt.days
    return aligned.sort_values("KRDate").reset_index(drop=True)


def latest_rolling_correlation(frame: pd.DataFrame, left: str, right: str, window: int) -> float:
    if len(frame) < window:
        return float("nan")
    return float(frame[left].rolling(window).corr(frame[right]).iloc[-1])


def direction_accuracy(frame: pd.DataFrame, left: str, right: str, window: int) -> float:
    if len(frame) < window:
        return float("nan")
    sample = frame[[left, right]].tail(window).dropna()
    if sample.empty:
        return float("nan")
    return float((np.sign(sample[left]) == np.sign(sample[right])).mean())


def direction_accuracy_full(frame: pd.DataFrame, left: str, right: str) -> float:
    sample = frame[[left, right]].dropna()
    if sample.empty:
        return float("nan")
    return float((np.sign(sample[left]) == np.sign(sample[right])).mean())


def iso_date(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: json_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [json_value(item) for item in value]
    if isinstance(value, tuple):
        return [json_value(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return iso_date(value)
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if pd.isna(value):
        return None
    return value


def fmt_corr(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{value:+.4f}"


def fmt_pct(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{value * 100:.1f}%"


def build_pair_result(
    sector_name: str,
    us_info: dict[str, Any],
    kr_info: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "sector": sector_name,
        "us_ticker": us_info["requested_ticker"],
        "us_downloaded_ticker": us_info["downloaded_ticker"],
        "kr_ticker": kr_info["requested_ticker"],
        "kr_downloaded_ticker": kr_info["downloaded_ticker"],
        "status": "ok",
        "note": "",
        "same_day_observations": 0,
        "same_day_latest_date": None,
        "lagged_observations": 0,
        "lagged_latest_us_date": None,
        "lagged_latest_kr_date": None,
        "lagged_max_gap_days": None,
        "metrics": {},
    }

    notes: list[str] = []
    if us_info["status"] != "ok":
        result["status"] = "error"
        notes.append(f"US download failed: {us_info['note']}")
    elif us_info["note"] != "ok":
        notes.append(f"US {us_info['note']}")

    if kr_info["status"] != "ok":
        result["status"] = "error"
        notes.append(f"KR download failed: {kr_info['note']}")
    elif kr_info["note"] != "ok":
        notes.append(f"KR {kr_info['note']}")

    if result["status"] != "ok":
        result["note"] = " | ".join(notes)
        result["metrics"] = {
            "same_day_correlation_20d": float("nan"),
            "same_day_correlation_60d": float("nan"),
            "lagged_correlation_20d": float("nan"),
            "lagged_correlation_60d": float("nan"),
            "direction_accuracy_20d": float("nan"),
            "direction_accuracy_60d": float("nan"),
            "direction_accuracy_full_sample": float("nan"),
        }
        return result

    same_day = align_same_day(us_info["returns"], kr_info["returns"])
    lagged = align_lagged(us_info["returns"], kr_info["returns"])

    result["same_day_observations"] = int(len(same_day))
    result["same_day_latest_date"] = iso_date(same_day.index.max()) if not same_day.empty else None
    result["lagged_observations"] = int(len(lagged))
    result["lagged_latest_us_date"] = iso_date(lagged["USDate"].max()) if not lagged.empty else None
    result["lagged_latest_kr_date"] = iso_date(lagged["KRDate"].max()) if not lagged.empty else None
    result["lagged_max_gap_days"] = (
        int(lagged["GapDays"].max()) if not lagged.empty else None
    )
    result["metrics"] = {
        "same_day_correlation_20d": latest_rolling_correlation(same_day, "USReturn", "KRReturn", 20),
        "same_day_correlation_60d": latest_rolling_correlation(same_day, "USReturn", "KRReturn", 60),
        "lagged_correlation_20d": latest_rolling_correlation(lagged, "USReturn", "KRReturn", 20),
        "lagged_correlation_60d": latest_rolling_correlation(lagged, "USReturn", "KRReturn", 60),
        "direction_accuracy_20d": direction_accuracy(lagged, "USReturn", "KRReturn", 20),
        "direction_accuracy_60d": direction_accuracy(lagged, "USReturn", "KRReturn", 60),
        "direction_accuracy_full_sample": direction_accuracy_full(lagged, "USReturn", "KRReturn"),
    }
    result["note"] = " | ".join(notes) if notes else "ok"
    return result


def build_sector_tables(pair_results: list[dict[str, Any]]) -> list[tuple[str, pd.DataFrame]]:
    tables: list[tuple[str, pd.DataFrame]] = []
    for sector in SECTORS:
        sector_rows = []
        for pair in pair_results:
            if pair["sector"] != sector["name"]:
                continue
            metrics = pair["metrics"]
            sector_rows.append(
                {
                    "US": pair["us_ticker"],
                    "KR": pair["kr_ticker"],
                    "SameN": pair["same_day_observations"],
                    "LagN": pair["lagged_observations"],
                    "Same20": metrics["same_day_correlation_20d"],
                    "Same60": metrics["same_day_correlation_60d"],
                    "Lag20": metrics["lagged_correlation_20d"],
                    "Lag60": metrics["lagged_correlation_60d"],
                    "Hit20": metrics["direction_accuracy_20d"],
                    "Hit60": metrics["direction_accuracy_60d"],
                    "Status": pair["note"],
                }
            )
        tables.append((sector["name"], pd.DataFrame(sector_rows)))
    return tables


def build_summary_table(pair_results: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for pair in pair_results:
        metrics = pair["metrics"]
        lag20 = metrics["lagged_correlation_20d"]
        lag60 = metrics["lagged_correlation_60d"]
        candidates = []
        if not pd.isna(lag20):
            candidates.append(("20d", lag20))
        if not pd.isna(lag60):
            candidates.append(("60d", lag60))
        if candidates:
            best_window, best_corr = max(candidates, key=lambda item: abs(item[1]))
            strength = abs(best_corr)
        else:
            best_window, best_corr, strength = None, float("nan"), float("nan")

        rows.append(
            {
                "Sector": pair["sector"],
                "US": pair["us_ticker"],
                "KR": pair["kr_ticker"],
                "BestLagWindow": best_window,
                "BestLagCorr": best_corr,
                "Strength": strength,
                "Lag20": lag20,
                "Lag60": lag60,
                "Hit20": metrics["direction_accuracy_20d"],
                "Hit60": metrics["direction_accuracy_60d"],
                "Status": pair["note"],
            }
        )

    summary = pd.DataFrame(rows)
    summary = summary.sort_values(
        by=["Strength", "BestLagCorr"],
        ascending=[False, False],
        na_position="last",
    ).reset_index(drop=True)
    return summary


def save_results_json(
    ticker_data: dict[str, dict[str, Any]],
    pair_results: list[dict[str, Any]],
    summary_table: pd.DataFrame,
) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "period": PERIOD,
        "interval": INTERVAL,
        "windows": list(WINDOWS),
        "ranking_method": "Sorted by the largest absolute lagged correlation from the 20d and 60d windows.",
        "ticker_downloads": {
            ticker: {
                "requested_ticker": info["requested_ticker"],
                "downloaded_ticker": info["downloaded_ticker"],
                "status": info["status"],
                "note": info["note"],
                "observations": info["observations"],
                "first_date": info["first_date"],
                "last_date": info["last_date"],
            }
            for ticker, info in ticker_data.items()
        },
        "pair_results": pair_results,
        "summary_ranking": summary_table.to_dict(orient="records"),
    }
    OUTPUT_PATH.write_text(json.dumps(json_value(payload), indent=2), encoding="utf-8")


def print_ticker_status(ticker_data: dict[str, dict[str, Any]]) -> None:
    rows = []
    for ticker, info in sorted(ticker_data.items()):
        rows.append(
            {
                "Ticker": ticker,
                "Used": info["downloaded_ticker"] or "-",
                "Obs": info["observations"],
                "First": info["first_date"] or "-",
                "Last": info["last_date"] or "-",
                "Status": info["note"],
            }
        )
    status_df = pd.DataFrame(rows)
    print("Ticker download status")
    print(status_df.to_string(index=False))
    print()


def print_sector_tables(pair_results: list[dict[str, Any]]) -> None:
    formatters = {
        "Same20": fmt_corr,
        "Same60": fmt_corr,
        "Lag20": fmt_corr,
        "Lag60": fmt_corr,
        "Hit20": fmt_pct,
        "Hit60": fmt_pct,
    }
    for sector_name, table in build_sector_tables(pair_results):
        print(sector_name)
        print(table.to_string(index=False, formatters=formatters))
        print()


def print_summary(summary_table: pd.DataFrame) -> None:
    print("SUMMARY: pairs ranked by lagged correlation strength")
    print(
        summary_table.to_string(
            index=False,
            formatters={
                "BestLagCorr": fmt_corr,
                "Strength": fmt_corr,
                "Lag20": fmt_corr,
                "Lag60": fmt_corr,
                "Hit20": fmt_pct,
                "Hit60": fmt_pct,
            },
        )
    )
    print()


def main() -> int:
    clear_dead_proxy_env()
    configure_yfinance_cache()
    configure_logging()

    print("Sector correlation backtest")
    print(f"Period: {PERIOD}, interval: {INTERVAL}, windows: {WINDOWS}")
    print("Method: daily close returns; lagged mapping uses U.S. day T -> next Korean trading day T+1")
    print()

    ticker_data = {ticker: download_ticker_data(ticker) for ticker in all_requested_tickers()}
    print_ticker_status(ticker_data)

    pair_results = []
    for sector in SECTORS:
        for us_ticker in sector["us"]:
            for kr_ticker in sector["kr"]:
                pair_results.append(
                    build_pair_result(
                        sector["name"],
                        ticker_data[us_ticker],
                        ticker_data[kr_ticker],
                    )
                )

    summary_table = build_summary_table(pair_results)
    print_sector_tables(pair_results)
    print_summary(summary_table)
    save_results_json(ticker_data, pair_results, summary_table)

    successful_pairs = sum(1 for pair in pair_results if pair["status"] == "ok")
    failed_pairs = len(pair_results) - successful_pairs
    print(f"Saved JSON: {OUTPUT_PATH}")
    print(f"Pairs processed: {len(pair_results)} total, {successful_pairs} successful, {failed_pairs} failed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
