"""
The Impact Board — NBA Stats Fetcher
======================================
python backend/ingest/fetch_nba_stats.py

Fetches gravity, shot quality, and leverage endpoints from NBA Stats.
These endpoints require a valid nba.com session cookie.

The script tries two approaches:
  1. Automated: visits nba.com first to get session cookies, then hits the stats endpoints
  2. Manual fallback: reads from local JSON files if the automated fetch fails

Usage:
    python backend/ingest/fetch_nba_stats.py
    python backend/ingest/fetch_nba_stats.py --season 2024-25
    python backend/ingest/fetch_nba_stats.py --season 2025-26 --manual

Files for manual fallback (place in backend/ingest/data/):
    gravity_{season}.json
    shot_quality_{season}.json
    leverage_{season}.json
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import requests

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument('--season',      default=os.getenv('NBA_SEASON', '2025-26'))
parser.add_argument('--season-type', default=os.getenv('NBA_SEASON_TYPE', 'Regular Season'))
parser.add_argument('--manual',      action='store_true', help='Skip fetch, use local JSON files')
parser.add_argument('--data-dir',    default='backend/ingest/data')
args = parser.parse_args()

SEASON      = args.season
SEASON_TYPE = args.season_type
DATA_DIR    = args.data_dir

ENDPOINTS = [
    {
        'name':     'gravity',
        'url':      'https://stats.nba.com/stats/gravityleaders',
        'params':   {'LeagueID': '00', 'Season': SEASON, 'SeasonType': SEASON_TYPE},
        'data_key': 'leaders',
        'file':     f'gravity_{SEASON}.json',
    },
    {
        'name':     'shot_quality',
        'url':      'https://stats.nba.com/stats/shotqualityleaders',
        'params':   {'LeagueID': '00', 'Season': SEASON, 'SeasonType': SEASON_TYPE, 'TeamID': '0'},
        'data_key': 'shots',
        'file':     f'shot_quality_{SEASON}.json',
    },
    {
        'name':     'leverage',
        'url':      'https://stats.nba.com/stats/leverageleaders',
        'params':   {'LeagueID': '00', 'Season': SEASON, 'SeasonType': SEASON_TYPE},
        'data_key': 'leaders',
        'file':     f'leverage_{SEASON}.json',
    },
]

# Headers that mimic the NBA app
NBA_HEADERS = {
    'User-Agent':             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Referer':                'https://www.nba.com/',
    'Origin':                 'https://www.nba.com',
    'Accept':                 'application/json, text/plain, */*',
    'Accept-Language':        'en-US,en;q=0.9',
    'Accept-Encoding':        'gzip, deflate, br',
    'Connection':             'keep-alive',
    'Host':                   'stats.nba.com',
    'x-nba-stats-origin':     'stats',
    'x-nba-stats-token':      'true',
    'sec-ch-ua':              '"Google Chrome";v="123", "Not:A-Brand";v="8"',
    'sec-ch-ua-mobile':       '?0',
    'sec-ch-ua-platform':     '"macOS"',
    'sec-fetch-dest':         'empty',
    'sec-fetch-mode':         'cors',
    'sec-fetch-site':         'same-site',
}


def get_session_with_cookies():
    """
    Visit nba.com to establish a session and get cookies,
    then return a requests.Session with those cookies set.
    """
    session = requests.Session()
    session.headers.update(NBA_HEADERS)

    print("  Getting session cookies from nba.com...")
    try:
        # Visit the main site to get session cookies
        resp = session.get('https://www.nba.com/', timeout=30)
        if resp.status_code == 200:
            cookies = dict(session.cookies)
            print(f"  ✅ Got {len(cookies)} cookies: {list(cookies.keys())}")
        else:
            print(f"  ⚠️  nba.com returned {resp.status_code}")

        time.sleep(2)

        # Also visit the stats page specifically
        resp2 = session.get('https://www.nba.com/stats/', timeout=30)
        cookies = dict(session.cookies)
        print(f"  ✅ After stats page: {len(cookies)} cookies")
        time.sleep(2)

    except Exception as e:
        print(f"  ⚠️  Cookie fetch failed: {e}")

    return session


def fetch_endpoint(session, endpoint):
    """Fetch a single endpoint, return list of rows or None."""
    name     = endpoint['name']
    url      = endpoint['url']
    params   = endpoint['params']
    data_key = endpoint['data_key']

    print(f"  Fetching {name}...")
    time.sleep(2)

    for attempt in range(3):
        try:
            resp = session.get(url, params=params, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                rows = data.get(data_key, [])
                if rows:
                    print(f"  ✅ {name}: {len(rows)} rows")
                    return rows
                else:
                    print(f"  ⚠️  {name}: 200 OK but no '{data_key}' key. Keys: {list(data.keys())}")
                    return None
            else:
                print(f"  ⚠️  {name}: HTTP {resp.status_code} (attempt {attempt+1}/3)")
                time.sleep(3 * (attempt + 1))
        except Exception as e:
            print(f"  ⚠️  {name}: {e} (attempt {attempt+1}/3)")
            time.sleep(3 * (attempt + 1))

    return None


def load_local_json(endpoint):
    """Load rows from a local JSON file."""
    path = os.path.join(DATA_DIR, endpoint['file'])
    if not os.path.exists(path):
        print(f"  ⚠️  No local file: {path}")
        return None
    with open(path) as f:
        data = json.load(f)
    rows = data if isinstance(data, list) else data.get(endpoint['data_key'], [])
    if rows:
        print(f"  ✅ {endpoint['name']}: {len(rows)} rows from local file")
    return rows or None


def save_local_json(endpoint, rows):
    """Cache fetched rows to a local file for manual re-import."""
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, endpoint['file'])
    with open(path, 'w') as f:
        json.dump(rows, f)
    print(f"  💾 Saved to {path}")


# ── DB write helpers ──────────────────────────────────────────
def safe(val, default=None):
    if val is None: return default
    try:
        import math
        if isinstance(val, float) and math.isnan(val): return default
        return val
    except: return default


def chunked_update(updates, sql, chunk_size=25):
    total = 0
    for i in range(0, len(updates), chunk_size):
        batch = updates[i:i+chunk_size]
        for attempt in range(3):
            try:
                conn = psycopg2.connect(DATABASE_URL)
                cur  = conn.cursor()
                cur.executemany(sql, batch)
                conn.commit()
                cur.close()
                conn.close()
                total += len(batch)
                break
            except Exception as e:
                if attempt < 2: time.sleep(3)
                else: print(f"  ❌ chunk failed: {e}")
    return total


def write_gravity(rows, season, season_type):
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    cur.execute("""
        ALTER TABLE player_seasons
          ADD COLUMN IF NOT EXISTS gravity_score              REAL,
          ADD COLUMN IF NOT EXISTS gravity_onball_perimeter   REAL,
          ADD COLUMN IF NOT EXISTS gravity_offball_perimeter  REAL,
          ADD COLUMN IF NOT EXISTS gravity_onball_interior    REAL,
          ADD COLUMN IF NOT EXISTS gravity_offball_interior   REAL
    """)
    conn.commit(); cur.close(); conn.close()

    updates = [(
        safe(r.get('AVGGRAVITYSCORE')),
        safe(r.get('AVGONBALLPERIMETERGRAVITYSCORE')),
        safe(r.get('AVGOFFBALLPERIMETERGRAVITYSCORE')),
        safe(r.get('AVGONBALLINTERIORGRAVITYSCORE')),
        safe(r.get('AVGOFFBALLINTERIORGRAVITYSCORE')),
        r['PLAYERID'], season, season_type,
    ) for r in rows if r.get('PLAYERID')]

    n = chunked_update(updates, """
        UPDATE player_seasons SET
            gravity_score             = %s,
            gravity_onball_perimeter  = %s,
            gravity_offball_perimeter = %s,
            gravity_onball_interior   = %s,
            gravity_offball_interior  = %s
        WHERE player_id = %s AND season = %s AND season_type = %s
    """)
    print(f"  ✅ Gravity: updated {n} players")


def write_shot_quality(rows, season, season_type):
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    cur.execute("""
        ALTER TABLE player_seasons
          ADD COLUMN IF NOT EXISTS sq_avg_shot_quality       REAL,
          ADD COLUMN IF NOT EXISTS sq_fg_pct_above_expected  REAL,
          ADD COLUMN IF NOT EXISTS sq_avg_defender_distance  REAL,
          ADD COLUMN IF NOT EXISTS sq_avg_defender_pressure  REAL,
          ADD COLUMN IF NOT EXISTS sq_avg_shooter_speed      REAL,
          ADD COLUMN IF NOT EXISTS sq_avg_made_quality       REAL,
          ADD COLUMN IF NOT EXISTS sq_avg_missed_quality     REAL
    """)
    conn.commit(); cur.close(); conn.close()

    updates = [(
        safe(r.get('AVGSHOTQUALITY')),
        safe(r.get('FGPCTABOVEEXPECTED')),
        safe(r.get('AVGDEFENDERBALLDISTANCE')),
        safe(r.get('AVGDEFENDERPRESSURESCORE')),
        safe(r.get('AVGSHOOTERSPEED')),
        safe(r.get('AVGMADESHOTQUALITY')),
        safe(r.get('AVGMISSEDSHOTQUALITY')),
        r['PLAYERID'], season, season_type,
    ) for r in rows if r.get('PLAYERID')]

    n = chunked_update(updates, """
        UPDATE player_seasons SET
            sq_avg_shot_quality      = %s,
            sq_fg_pct_above_expected = %s,
            sq_avg_defender_distance = %s,
            sq_avg_defender_pressure = %s,
            sq_avg_shooter_speed     = %s,
            sq_avg_made_quality      = %s,
            sq_avg_missed_quality    = %s
        WHERE player_id = %s AND season = %s AND season_type = %s
    """)
    print(f"  ✅ Shot Quality: updated {n} players")


def write_leverage(rows, season, season_type):
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    cur.execute("""
        ALTER TABLE player_seasons
          ADD COLUMN IF NOT EXISTS leverage_full        REAL,
          ADD COLUMN IF NOT EXISTS leverage_offense     REAL,
          ADD COLUMN IF NOT EXISTS leverage_defense     REAL,
          ADD COLUMN IF NOT EXISTS leverage_shooting    REAL,
          ADD COLUMN IF NOT EXISTS leverage_creation    REAL,
          ADD COLUMN IF NOT EXISTS leverage_turnovers   REAL,
          ADD COLUMN IF NOT EXISTS leverage_rebounds    REAL,
          ADD COLUMN IF NOT EXISTS leverage_onball_def  REAL
    """)
    conn.commit(); cur.close(); conn.close()

    updates = [(
        safe(r.get('FULL')),
        safe(r.get('OFFENSE')),
        safe(r.get('DEFENSE')),
        safe(r.get('SHOOTING')),
        safe(r.get('CREATION')),
        safe(r.get('TURNOVERS')),
        safe(r.get('REBOUNDS')),
        safe(r.get('ONBALLDEF')),
        r['PLAYERID'], season, season_type,
    ) for r in rows if r.get('PLAYERID')]

    n = chunked_update(updates, """
        UPDATE player_seasons SET
            leverage_full       = %s,
            leverage_offense    = %s,
            leverage_defense    = %s,
            leverage_shooting   = %s,
            leverage_creation   = %s,
            leverage_turnovers  = %s,
            leverage_rebounds   = %s,
            leverage_onball_def = %s
        WHERE player_id = %s AND season = %s AND season_type = %s
    """)
    print(f"  ✅ Leverage: updated {n} players")


WRITERS = {
    'gravity':     write_gravity,
    'shot_quality': write_shot_quality,
    'leverage':    write_leverage,
}


def main():
    print(f"\nThe Impact Board — NBA Stats Fetcher")
    print(f"Season: {SEASON} | Type: {SEASON_TYPE}")
    print(f"Mode: {'manual (local JSON)' if args.manual else 'automated fetch'}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    os.makedirs(DATA_DIR, exist_ok=True)

    session = None
    if not args.manual:
        session = get_session_with_cookies()

    for endpoint in ENDPOINTS:
        name = endpoint['name']
        print(f"\n── {name} ──────────────────────────────────────────────")

        rows = None
        if not args.manual and session:
            rows = fetch_endpoint(session, endpoint)
            if rows:
                save_local_json(endpoint, rows)

        if rows is None:
            print(f"  Automated fetch failed or skipped — trying local file...")
            rows = load_local_json(endpoint)

        if rows is None:
            print(f"  ❌ No data for {name} — skipping DB write")
            continue

        WRITERS[name](rows, SEASON, SEASON_TYPE)

    print(f"\n{'='*60}")
    print(f"✅ Done — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"\nNext step: python backend/ingest/compute_metrics.py")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()