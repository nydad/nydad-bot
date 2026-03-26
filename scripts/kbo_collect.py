#!/usr/bin/env python3
"""
nydad-bot — KBO (Korean Baseball Organization) Data Collector

Collects three types of KBO data:
  1. Team standings from koreabaseball.com
  2. Today's game scores/schedule from koreabaseball.com
  3. KBO-related news articles from Korean sports RSS feeds

Returns structured dict with keys: standings, games_today, articles.
Designed for GitHub Actions (Ubuntu, Python 3.12). Graceful partial-data on errors.
"""

import hashlib
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KST = timezone(timedelta(hours=9))
AGE_HOURS = 36
MAX_PER_FEED = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("kbo-collect")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

# KBO official site URLs
STANDINGS_URL = "https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx"
SCHEDULE_URL = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"

# Korean sports RSS feeds for KBO news
KBO_NEWS_FEEDS = [
    {"name": "스포츠조선 야구", "url": "https://sports.chosun.com/rss/index_bs.htm"},
    {"name": "스포츠칸 야구", "url": "https://sports.khan.co.kr/rss/baseball"},
    {"name": "스포츠동아 야구", "url": "https://rss.donga.com/sportsdonga/baseball.xml"},
]

# Team name normalization map (various abbreviations -> standard name)
TEAM_NAMES = {
    "KIA": "KIA 타이거즈",
    "삼성": "삼성 라이온즈",
    "LG": "LG 트윈스",
    "두산": "두산 베어스",
    "KT": "KT 위즈",
    "SSG": "SSG 랜더스",
    "롯데": "롯데 자이언츠",
    "한화": "한화 이글스",
    "NC": "NC 다이노스",
    "키움": "키움 히어로즈",
}


# ---------------------------------------------------------------------------
# Standings
# ---------------------------------------------------------------------------
def fetch_standings() -> list[dict]:
    """Scrape KBO team standings from koreabaseball.com.

    Returns a list of dicts with keys:
      rank, team, wins, losses, draws, win_pct, games_behind, streak, last10
    """
    log.info("=== Fetching KBO Standings ===")
    try:
        resp = requests.get(STANDINGS_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error("Failed to fetch standings page: %s", e)
        return []

    soup = BeautifulSoup(resp.text, "lxml")

    # The standings table has id="cphContents_cphContents_cphContents_udpRecord"
    # or is inside a <table> with class "tData" or similar.
    # Look for the main data table in the standings section.
    table = soup.select_one("#cphContents_cphContents_cphContents_udpRecord table")
    if not table:
        # Fallback: find any table with the right structure
        tables = soup.select("table.tData, table.Record")
        table = tables[0] if tables else None
    if not table:
        # Last resort: find any table containing team-related headers
        for t in soup.find_all("table"):
            header_text = t.get_text()
            if "순위" in header_text and ("승" in header_text or "패" in header_text):
                table = t
                break

    if not table:
        log.warning("Could not locate standings table in HTML")
        return []

    rows = table.select("tbody tr")
    if not rows:
        rows = table.find_all("tr")[1:]  # skip header row

    standings = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 6:
            continue
        try:
            texts = [c.get_text(strip=True) for c in cells]
            # KBO standings table columns (typical order):
            # 순위 | 팀명 | 경기 | 승 | 패 | 무 | 승률 | 게임차 | 연속 | 최근10경기
            # Sometimes rank is in the first column, sometimes it's implicit.
            # We'll try to be flexible.

            record = {}
            # Detect if first cell is numeric (rank) or team name
            if texts[0].isdigit():
                record["rank"] = int(texts[0])
                texts = texts[1:]
            else:
                record["rank"] = len(standings) + 1

            # Team name (might contain image alt text or just text)
            team_cell = cells[1] if cells[0].get_text(strip=True).isdigit() else cells[0]
            team_name = team_cell.get_text(strip=True)
            # Try to get team name from img alt if present
            img = team_cell.find("img")
            if img and img.get("alt"):
                team_name = img["alt"].strip()
            record["team"] = team_name

            # Remaining numeric fields: games, wins, losses, draws, win_pct, games_behind, streak, last10
            nums = texts[1:]  # skip team name
            if len(nums) >= 5:
                record["games"] = _safe_int(nums[0])
                record["wins"] = _safe_int(nums[1])
                record["losses"] = _safe_int(nums[2])
                record["draws"] = _safe_int(nums[3])
                record["win_pct"] = nums[4]
                record["games_behind"] = nums[5] if len(nums) > 5 else "-"
                record["streak"] = nums[6] if len(nums) > 6 else ""
                record["last10"] = nums[7] if len(nums) > 7 else ""

            if "wins" in record:
                standings.append(record)
        except Exception as e:
            log.warning("Error parsing standings row: %s", e)
            continue

    log.info("  Parsed %d teams from standings", len(standings))
    return standings


# ---------------------------------------------------------------------------
# Today's Games
# ---------------------------------------------------------------------------
def fetch_games_today() -> list[dict]:
    """Scrape today's KBO game schedule/scores from koreabaseball.com.

    Returns a list of dicts with keys:
      home_team, away_team, home_score, away_score, status, time
    """
    log.info("=== Fetching Today's KBO Games ===")
    try:
        resp = requests.get(SCHEDULE_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error("Failed to fetch schedule page: %s", e)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    games = []

    # GameCenter uses various div structures for game cards.
    # Each game is typically in a div with class "game-cont" or similar.
    game_cards = soup.select(".game-cont, .scheduleList li, .smsScore")

    if not game_cards:
        # Alternative: look for game schedule within the Schedule section
        game_cards = soup.select("[class*='game'], [class*='Game'], [class*='score']")

    if not game_cards:
        # Try parsing the entire schedule structure
        # KBO site uses ASP.NET UpdatePanels; the game data might be in
        # script tags or specific div patterns
        game_cards = _parse_game_center_fallback(soup)

    for card in game_cards:
        try:
            game = _parse_game_card(card)
            if game:
                games.append(game)
        except Exception as e:
            log.warning("Error parsing game card: %s", e)
            continue

    # If page parsing fails completely, try the API endpoint
    if not games:
        games = _fetch_games_via_api()

    log.info("  Found %d games today", len(games))
    return games


def _parse_game_card(card) -> dict | None:
    """Parse a single game card element into a game dict."""
    text = card.get_text(" ", strip=True)
    if not text or len(text) < 3:
        return None

    game = {}

    # Look for team names within the card
    team_spans = card.select(".team, .teamName, [class*='Team']")
    score_spans = card.select(".score, [class*='Score'], [class*='point']")
    status_span = card.select_one(".state, .status, [class*='status'], [class*='State']")
    time_span = card.select_one(".time, [class*='time'], [class*='Time']")

    if len(team_spans) >= 2:
        game["away_team"] = team_spans[0].get_text(strip=True)
        game["home_team"] = team_spans[1].get_text(strip=True)
    else:
        # Try to extract team names from known team abbreviations
        found_teams = []
        for abbr in TEAM_NAMES:
            if abbr in text:
                found_teams.append(abbr)
        if len(found_teams) >= 2:
            game["away_team"] = found_teams[0]
            game["home_team"] = found_teams[1]
        else:
            return None

    if len(score_spans) >= 2:
        game["away_score"] = _safe_int(score_spans[0].get_text(strip=True))
        game["home_score"] = _safe_int(score_spans[1].get_text(strip=True))
    elif len(score_spans) == 1:
        score_text = score_spans[0].get_text(strip=True)
        parts = re.split(r"[:\-vs]", score_text)
        if len(parts) >= 2:
            game["away_score"] = _safe_int(parts[0].strip())
            game["home_score"] = _safe_int(parts[1].strip())
        else:
            game["away_score"] = None
            game["home_score"] = None
    else:
        game["away_score"] = None
        game["home_score"] = None

    if status_span:
        raw_status = status_span.get_text(strip=True)
        game["status"] = _normalize_status(raw_status)
    else:
        game["status"] = _infer_status(text)

    if time_span:
        game["time"] = time_span.get_text(strip=True)
    else:
        time_match = re.search(r"(\d{1,2}:\d{2})", text)
        game["time"] = time_match.group(1) if time_match else ""

    return game


def _parse_game_center_fallback(soup) -> list:
    """Fallback parser: look for game data in table rows or other structures."""
    results = []
    # Try finding a schedule table
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            text = row.get_text(" ", strip=True)
            # Check if row contains team names
            team_count = sum(1 for abbr in TEAM_NAMES if abbr in text)
            if team_count >= 2:
                results.append(row)
    return results


def _fetch_games_via_api() -> list[dict]:
    """Try fetching game data from KBO's internal API/AJAX endpoints."""
    now_kst = datetime.now(KST)
    date_str = now_kst.strftime("%Y%m%d")
    # KBO site uses ASP.NET AJAX; try the JSON-like endpoint
    api_url = "https://www.koreabaseball.com/ws/Schedule.asmx/GetScheduleList"
    try:
        resp = requests.post(
            api_url,
            data={"leId": "1", "srIdList": "0,9", "seasonId": str(now_kst.year),
                  "gameMonth": now_kst.strftime("%m"), "gameDay": now_kst.strftime("%d")},
            headers={**HEADERS, "X-Requested-With": "XMLHttpRequest"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json() if "json" in resp.headers.get("content-type", "") else None
            if data and isinstance(data, list):
                games = []
                for g in data:
                    games.append({
                        "away_team": g.get("away", ""),
                        "home_team": g.get("home", ""),
                        "away_score": _safe_int(g.get("awayScore", "")),
                        "home_score": _safe_int(g.get("homeScore", "")),
                        "status": _normalize_status(g.get("statusText", "")),
                        "time": g.get("gameTime", ""),
                    })
                return games
    except Exception as e:
        log.warning("KBO API fallback failed: %s", e)
    return []


# ---------------------------------------------------------------------------
# News Collection
# ---------------------------------------------------------------------------
def fetch_kbo_news() -> list[dict]:
    """Collect KBO news from Korean sports RSS feeds.

    Filters articles from the last 36 hours and deduplicates by URL hash.
    Returns a list of dicts with keys:
      title, url, source, published, description
    """
    log.info("=== Fetching KBO News ===")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=AGE_HOURS)
    all_articles = []

    for feed_cfg in KBO_NEWS_FEEDS:
        articles = _fetch_rss(feed_cfg, cutoff)
        all_articles.extend(articles)

    # Deduplicate by URL hash
    seen = set()
    unique = []
    for a in all_articles:
        url_key = a["url"].lower().split("?")[0].rstrip("/")
        h = hashlib.sha256(url_key.encode()).hexdigest()[:16]
        if h not in seen:
            seen.add(h)
            unique.append(a)

    # Sort by published date (newest first)
    unique.sort(key=lambda x: x.get("published", ""), reverse=True)

    log.info("  Total unique articles: %d", len(unique))
    return unique


def _fetch_rss(cfg: dict, cutoff: datetime) -> list[dict]:
    """Fetch and parse a single RSS feed, filtering by cutoff time."""
    name = cfg["name"]
    try:
        feed = feedparser.parse(cfg["url"], request_headers=HEADERS)
        if feed.bozo and not feed.entries:
            log.warning("  %s: feed error (bozo), no entries", name)
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
                "title": title,
                "url": link,
                "source": name,
                "published": (pub or datetime.now(timezone.utc)).isoformat(),
                "description": _clean_html(entry.get("summary", ""))[:600],
            })
        log.info("  %-20s -> %d articles", name, len(results))
        return results
    except Exception as e:
        log.error("  %s: %s", name, e)
        return []


def _parse_date(entry) -> datetime | None:
    """Extract publication date from a feedparser entry."""
    for field in ("published_parsed", "updated_parsed", "created_parsed"):
        parsed = entry.get(field)
        if parsed:
            try:
                return datetime(*parsed[:6], tzinfo=timezone.utc)
            except (ValueError, TypeError):
                pass
    return None


def _clean_html(text: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text)).strip()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe_int(val) -> int | None:
    """Convert a value to int, returning None on failure."""
    if val is None:
        return None
    try:
        cleaned = re.sub(r"[^\d\-]", "", str(val).strip())
        return int(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None


def _normalize_status(raw: str) -> str:
    """Normalize game status text to one of: 예정, 진행중, 종료, 취소, unknown."""
    raw = raw.strip()
    if not raw:
        return "예정"
    if raw in ("종료", "Final", "경기종료"):
        return "종료"
    if raw in ("예정", "경기예정") or re.match(r"^\d{1,2}:\d{2}$", raw):
        return "예정"
    if raw in ("취소", "우천취소", "Cancelled"):
        return "취소"
    if any(kw in raw for kw in ("진행", "회", "초", "말", "Live", "ing")):
        return "진행중"
    return raw  # return as-is if unrecognized


def _infer_status(text: str) -> str:
    """Infer game status from surrounding text."""
    if "종료" in text or "Final" in text:
        return "종료"
    if "취소" in text:
        return "취소"
    if any(kw in text for kw in ("회초", "회말", "진행")):
        return "진행중"
    return "예정"


# ---------------------------------------------------------------------------
# Main API
# ---------------------------------------------------------------------------
def collect_kbo_data() -> dict:
    """Collect all KBO data and return as a structured dict.

    Returns:
        {
            "collected_at": str (ISO 8601),
            "standings": [...],
            "games_today": [...],
            "articles": [...]
        }

    Gracefully handles partial failures: each section is independently
    collected, so a failure in one does not prevent others from returning data.
    """
    log.info("Starting KBO data collection")
    now_kst = datetime.now(KST)
    log.info("Current KST: %s", now_kst.strftime("%Y-%m-%d %H:%M"))

    result = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "date_kst": now_kst.strftime("%Y-%m-%d"),
        "standings": [],
        "games_today": [],
        "articles": [],
    }

    # Standings
    try:
        result["standings"] = fetch_standings()
    except Exception as e:
        log.error("Standings collection failed: %s", e)

    # Today's games
    try:
        result["games_today"] = fetch_games_today()
    except Exception as e:
        log.error("Games collection failed: %s", e)

    # News
    try:
        result["articles"] = fetch_kbo_news()
    except Exception as e:
        log.error("News collection failed: %s", e)

    log.info(
        "Collection complete: %d teams, %d games, %d articles",
        len(result["standings"]),
        len(result["games_today"]),
        len(result["articles"]),
    )
    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    data = collect_kbo_data()

    # Print summary
    print("\n" + "=" * 60)
    print("KBO Data Collection Results")
    print("=" * 60)

    if data["standings"]:
        print(f"\n[Standings] {len(data['standings'])} teams")
        print(f"  {'순위':<4} {'팀':<12} {'승':<4} {'패':<4} {'무':<4} {'승률':<6} {'게임차':<6}")
        print("  " + "-" * 46)
        for t in data["standings"]:
            print(
                f"  {t.get('rank', '-'):<4} {t.get('team', ''):<12} "
                f"{t.get('wins', '-'):<4} {t.get('losses', '-'):<4} "
                f"{t.get('draws', '-'):<4} {t.get('win_pct', '-'):<6} "
                f"{t.get('games_behind', '-'):<6}"
            )
    else:
        print("\n[Standings] No data")

    if data["games_today"]:
        print(f"\n[Games Today] {len(data['games_today'])} games")
        for g in data["games_today"]:
            away = g.get("away_team", "?")
            home = g.get("home_team", "?")
            a_score = g.get("away_score")
            h_score = g.get("home_score")
            score_str = f"{a_score} - {h_score}" if a_score is not None else "vs"
            status = g.get("status", "")
            time_str = g.get("time", "")
            print(f"  {away} {score_str} {home}  [{status}] {time_str}")
    else:
        print("\n[Games Today] No games or data unavailable")

    if data["articles"]:
        print(f"\n[News] {len(data['articles'])} articles")
        for a in data["articles"][:10]:
            print(f"  [{a['source']}] {a['title'][:60]}")
        if len(data["articles"]) > 10:
            print(f"  ... and {len(data['articles']) - 10} more")
    else:
        print("\n[News] No articles")

    # Also dump JSON to stdout for piping
    if "--json" in sys.argv:
        print("\n" + json.dumps(data, ensure_ascii=False, indent=2))

    # Save to data directory if --save flag
    if "--save" in sys.argv:
        data_dir = Path(__file__).resolve().parent.parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        outfile = data_dir / "kbo.json"
        with open(outfile, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\nSaved to {outfile}")
