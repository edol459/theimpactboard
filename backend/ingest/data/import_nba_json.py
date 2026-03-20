"""
The Impact Board — NBA Endpoint Manual Import
===============================================
python backend/ingest/import_nba_json.py

Reads locally saved JSON files from the NBA stats endpoints
(gravity, shot quality, leverage) and upserts into player_seasons.

How to get the JSON files:
  1. Open Chrome and navigate to nba.com
  2. Open DevTools → Network tab
  3. Visit each endpoint URL while logged in / with the app open
  4. Find the request, right-click → Copy → Copy Response
  5. Paste into the corresponding file below

Expected files (place in backend/ingest/data/):
  gravity_{season}.json       e.g. gravity_2025-26.json
  shot_quality_{season}.json  e.g. shot_quality_2025-26.json
  leverage_{season}.json      e.g. leverage_2025-26.json

Safe to re-run — uses upsert logic.

Usage:
    python backend/ingest/import_nba_json.py
    python backend/ingest/import_nba_json.py --season 2024-25
    python backend/ingest/import_nba_json.py --only gravity
"""

import os
import sys
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument('--season',      default=os.getenv('NBA_SEASON', '2025-26'))
parser.add_argument('--season-type', default=os.getenv('NBA_SEASON_TYPE', 'Regular Season'))
parser.add_argument('--only',        default=None,
                    help='Only import one endpoint: gravity, shot_quality, or leverage')
parser.add_argument('--data-dir',    default='backend/ingest/data',
                    help='Directory containing the JSON files')
args = parser.parse_args()

SEASON      = args.season
SEASON_TYPE = args.season_type
DATA_DIR    = args.data_dir


# ── Helpers ───────────────────────────────────────────────────
def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"  ⚠️  File not found: {path} — skipping")
        return None
    with open(path) as f:
        return json.load(f)

def safe(val, default=None):
    if val is None:
        return default
    try:
        import math
        if isinstance(val, float) and math.isnan(val):
            return default
        return val
    except:
        return default

def chunked_upsert(conn, updates, sql, chunk_size=25):
    """Run updates in chunks with reconnect to avoid Railway timeouts."""
    total = 0
    for i in range(0, len(updates), chunk_size):
        batch = updates[i:i+chunk_size]
        for attempt in range(3):
            try:
                c = psycopg2.connect(DATABASE_URL)
                cur = c.cursor()
                cur.executemany(sql, batch)
                c.commit()
                cur.close()
                c.close()
                total += len(batch)
                break
            except Exception as e:
                if attempt < 2:
                    import time; time.sleep(3)
                else:
                    print(f"  ❌ Chunk failed: {e}")
    return total


# ── Gravity ───────────────────────────────────────────────────
def import_gravity(season, season_type):
    print(f"\n── Gravity Leaders ──────────────────────────────────────")
    filename = f"gravity_{season}.json"
    data = load_json(filename)
    if not data:
        return

    # Support both raw response and pre-extracted leaders array
    rows = data if isinstance(data, list) else data.get('leaders', [])
    if not rows:
        print(f"  ❌ No rows found. Keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
        return

    print(f"  {len(rows)} players found")

    # Ensure columns exist
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
    conn.commit()
    cur.close()
    conn.close()

    updates = []
    skipped = 0
    for r in rows:
        pid = r.get('PLAYERID')
        if not pid:
            skipped += 1
            continue
        updates.append((
            safe(r.get('AVGGRAVITYSCORE')),
            safe(r.get('AVGONBALLPERIMETERGRAVITYSCORE')),
            safe(r.get('AVGOFFBALLPERIMETERGRAVITYSCORE')),
            safe(r.get('AVGONBALLINTERIORGRAVITYSCORE')),
            safe(r.get('AVGOFFBALLINTERIORGRAVITYSCORE')),
            pid, season, season_type,
        ))

    sql = """
        UPDATE player_seasons SET
            gravity_score             = %s,
            gravity_onball_perimeter  = %s,
            gravity_offball_perimeter = %s,
            gravity_onball_interior   = %s,
            gravity_offball_interior  = %s
        WHERE player_id = %s AND season = %s AND season_type = %s
    """
    updated = chunked_upsert(psycopg2.connect(DATABASE_URL), updates, sql)
    print(f"  ✅ Updated {updated} players  (skipped {skipped})")

    # Spot check
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT p.player_name, ps.gravity_score,
               ps.gravity_onball_perimeter, ps.gravity_offball_perimeter
        FROM player_seasons ps
        JOIN players p ON ps.player_id = p.player_id
        WHERE ps.season = %s AND ps.season_type = %s
          AND ps.gravity_score IS NOT NULL
        ORDER BY ps.gravity_score DESC LIMIT 10
    """, (season, season_type))
    print(f"\n  Top 10 by AVGGRAVITYSCORE:")
    print(f"  {'Player':<25} {'Gravity':>8} {'OnBall':>8} {'OffBall':>8}")
    print(f"  {'─'*55}")
    for r in cur.fetchall():
        print(f"  {r['player_name']:<25} {r['gravity_score'] or 0:>8.3f} "
              f"{r['gravity_onball_perimeter'] or 0:>8.3f} "
              f"{r['gravity_offball_perimeter'] or 0:>8.3f}")
    cur.close()
    conn.close()


# ── Shot Quality ──────────────────────────────────────────────
def import_shot_quality(season, season_type):
    print(f"\n── Shot Quality Leaders ─────────────────────────────────")
    filename = f"shot_quality_{season}.json"
    data = load_json(filename)
    if not data:
        return

    rows = data if isinstance(data, list) else data.get('shots', [])
    if not rows:
        print(f"  ❌ No rows found. Keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
        return

    print(f"  {len(rows)} players found")

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
    conn.commit()
    cur.close()
    conn.close()

    updates = []
    skipped = 0
    for r in rows:
        pid = r.get('PLAYERID')
        if not pid:
            skipped += 1
            continue
        updates.append((
            safe(r.get('AVGSHOTQUALITY')),
            safe(r.get('FGPCTABOVEEXPECTED')),
            safe(r.get('AVGDEFENDERBALLDISTANCE')),
            safe(r.get('AVGDEFENDERPRESSURESCORE')),
            safe(r.get('AVGSHOOTERSPEED')),
            safe(r.get('AVGMADESHOTQUALITY')),
            safe(r.get('AVGMISSEDSHOTQUALITY')),
            pid, season, season_type,
        ))

    sql = """
        UPDATE player_seasons SET
            sq_avg_shot_quality      = %s,
            sq_fg_pct_above_expected = %s,
            sq_avg_defender_distance = %s,
            sq_avg_defender_pressure = %s,
            sq_avg_shooter_speed     = %s,
            sq_avg_made_quality      = %s,
            sq_avg_missed_quality    = %s
        WHERE player_id = %s AND season = %s AND season_type = %s
    """
    updated = chunked_upsert(psycopg2.connect(DATABASE_URL), updates, sql)
    print(f"  ✅ Updated {updated} players  (skipped {skipped})")

    # Spot check
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT p.player_name, ps.sq_avg_shot_quality,
               ps.sq_fg_pct_above_expected, ps.sq_avg_defender_distance
        FROM player_seasons ps
        JOIN players p ON ps.player_id = p.player_id
        WHERE ps.season = %s AND ps.season_type = %s
          AND ps.sq_avg_shot_quality IS NOT NULL
        ORDER BY ps.sq_fg_pct_above_expected DESC NULLS LAST LIMIT 10
    """, (season, season_type))
    print(f"\n  Top 10 by FG% Above Expected:")
    print(f"  {'Player':<25} {'ShotQ':>7} {'FG%+Exp':>8} {'DefDist':>8}")
    print(f"  {'─'*55}")
    for r in cur.fetchall():
        print(f"  {r['player_name']:<25} {r['sq_avg_shot_quality'] or 0:>7.3f} "
              f"{r['sq_fg_pct_above_expected'] or 0:>+8.3f} "
              f"{r['sq_avg_defender_distance'] or 0:>8.1f}")
    cur.close()
    conn.close()


# ── Leverage ──────────────────────────────────────────────────
def import_leverage(season, season_type):
    print(f"\n── Leverage Leaders ─────────────────────────────────────")
    filename = f"leverage_{season}.json"
    data = load_json(filename)
    if not data:
        return

    rows = data if isinstance(data, list) else data.get('leaders', [])
    if not rows:
        print(f"  ❌ No rows found. Keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
        return

    print(f"  {len(rows)} players found")

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
    conn.commit()
    cur.close()
    conn.close()

    updates = []
    skipped = 0
    for r in rows:
        pid = r.get('PLAYERID')
        if not pid:
            skipped += 1
            continue
        updates.append((
            safe(r.get('FULL')),
            safe(r.get('OFFENSE')),
            safe(r.get('DEFENSE')),
            safe(r.get('SHOOTING')),
            safe(r.get('CREATION')),
            safe(r.get('TURNOVERS')),
            safe(r.get('REBOUNDS')),
            safe(r.get('ONBALLDEF')),
            pid, season, season_type,
        ))

    sql = """
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
    """
    updated = chunked_upsert(psycopg2.connect(DATABASE_URL), updates, sql)
    print(f"  ✅ Updated {updated} players  (skipped {skipped})")

    # Spot check
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT p.player_name, ps.leverage_full, ps.leverage_creation,
               ps.leverage_offense, ps.leverage_defense
        FROM player_seasons ps
        JOIN players p ON ps.player_id = p.player_id
        WHERE ps.season = %s AND ps.season_type = %s
          AND ps.leverage_full IS NOT NULL
        ORDER BY ps.leverage_full DESC NULLS LAST LIMIT 10
    """, (season, season_type))
    print(f"\n  Top 10 by Leverage (Full):")
    print(f"  {'Player':<25} {'Full':>7} {'Creation':>9} {'Off':>7} {'Def':>7}")
    print(f"  {'─'*60}")
    for r in cur.fetchall():
        print(f"  {r['player_name']:<25} {r['leverage_full'] or 0:>7.4f} "
              f"{r['leverage_creation'] or 0:>9.4f} "
              f"{r['leverage_offense'] or 0:>7.4f} "
              f"{r['leverage_defense'] or 0:>7.4f}")
    cur.close()
    conn.close()


# ── Main ──────────────────────────────────────────────────────
def main():
    print(f"\nThe Impact Board — NBA JSON Import")
    print(f"Season: {SEASON} | Type: {SEASON_TYPE}")
    print(f"Data dir: {DATA_DIR}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    os.makedirs(DATA_DIR, exist_ok=True)

    only = args.only
    if only == 'gravity'     or only is None: import_gravity(SEASON, SEASON_TYPE)
    if only == 'shot_quality' or only is None: import_shot_quality(SEASON, SEASON_TYPE)
    if only == 'leverage'    or only is None: import_leverage(SEASON, SEASON_TYPE)

    print(f"\n{'='*60}")
    print(f"✅ Done — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"\nNext step: python backend/ingest/compute_metrics.py")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()