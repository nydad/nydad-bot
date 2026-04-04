#!/usr/bin/env python3
"""Quick market data sync — no AI calls, ~30s runtime."""
import os, sys, json, logging, time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    yf = None
import requests

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
KST = timezone(timedelta(hours=9))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("sync")
COINGECKO_DEMO_API_KEY = os.environ.get("COINGECKO_DEMO_API_KEY", "")

TICKERS = {
    "us_indices": [("S&P 500","^GSPC"),("NASDAQ","^IXIC"),("다우존스","^DJI"),("러셀 2000","^RUT"),("필라델피아 반도체","^SOX")],
    "futures": [("S&P 500 선물","ES=F"),("나스닥 선물","NQ=F"),("다우 선물","YM=F")],
    "volatility": [("VIX","^VIX")],
    "forex": [("달러/원","KRW=X"),("달러/엔","JPY=X"),("유로/달러","EURUSD=X"),("달러인덱스","DX-Y.NYB")],
    "commodities": [("WTI 원유","CL=F"),("브렌트유","BZ=F"),("금","GC=F"),("은","SI=F")],
    "bonds": [("미국 10년물","^TNX"),("미국 2년물","^IRX")],
    "kr_indices": [("KOSPI","^KS11"),("KOSDAQ","^KQ11"),("KOSPI 200","^KS200")],
}

def main():
    log.info("Quick market sync...")
    market = {}
    if yf:
        syms = []
        smap = {}
        for cat, items in TICKERS.items():
            for name, sym in items:
                syms.append(sym)
                smap[sym] = (cat, name)
        try:
            df = yf.download(syms, period="2d", interval="1d", progress=False, threads=True, timeout=20)
            for sym, (cat, name) in smap.items():
                try:
                    close = df["Close"][sym].dropna() if len(syms) > 1 else df["Close"].dropna()
                    if len(close) < 1: continue
                    cur = float(close.iloc[-1])
                    prev = float(close.iloc[-2]) if len(close) >= 2 else cur
                    chg = cur - prev
                    pct = (chg / prev) * 100 if prev else 0
                    prec = 4 if cat in ("forex","bonds") else 2
                    data_date = str(close.index[-1].date()) if hasattr(close.index[-1], 'date') else ""
                    market.setdefault(cat, []).append({"name": name, "ticker": sym,
                        "price": round(cur, prec), "change": round(chg, prec), "change_pct": round(pct, 2),
                        "data_date": data_date})
                except Exception:
                    pass
        except Exception as e:
            log.error("Download failed: %s", e)

    # Crypto
    crypto = []
    try:
        headers = {}
        if COINGECKO_DEMO_API_KEY:
            headers["x-cg-demo-api-key"] = COINGECKO_DEMO_API_KEY
        for attempt in range(3):
            r = requests.get("https://api.coingecko.com/api/v3/coins/markets",
                params={"vs_currency":"usd","order":"market_cap_desc","per_page":15,"page":1,"sparkline":"false",
                        "price_change_percentage":"24h"}, headers=headers, timeout=15)
            if r.status_code == 429 and attempt < 2:
                retry_after = int(r.headers.get("Retry-After", 10))
                log.warning("CoinGecko rate limited, waiting %ds...", retry_after)
                time.sleep(min(retry_after, 30))
                continue
            r.raise_for_status()
            crypto = [{"name":c["name"],"symbol":c["symbol"].upper(),"price":c["current_price"],
                       "change_pct":round(c.get("price_change_percentage_24h") or 0,2),
                       "market_cap":c.get("market_cap",0),"rank":c.get("market_cap_rank",0)} for c in r.json()]
            break
    except Exception:
        pass

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out = {"synced_at": datetime.now(timezone.utc).isoformat(), "market_data": market, "crypto_prices": crypto}
    with open(DATA_DIR / "live.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    log.info("Saved live.json (%d tickers, %d coins)", sum(len(v) for v in market.values()), len(crypto))

if __name__ == "__main__":
    main()
