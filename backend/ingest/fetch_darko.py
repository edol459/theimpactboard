"""
The Impact Board — Fetch DARKO DPM
=====================================
python backend/ingest/fetch_darko.py

Fetches from https://www.darko.app/api/active-players
Matches on nba_id directly — no name matching needed.
Updates player_seasons with darko_dpm, darko_odpm, darko_ddpm, darko_box.
NOTE: /api/active-players is current-season only — no historical backfill possible via this endpoint.

Run manually or via daily_update.py.
"""

import os
import sys
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
SEASON       = os.getenv('NBA_SEASON', '2025-26')
SEASON_TYPE  = os.getenv('NBA_SEASON_TYPE', 'Regular Season')

DARKO_URL = 'https://www.darko.app/api/active-players'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Referer': 'https://www.darko.app/',
}

if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)


def fetch_darko():
    print(f"Fetching DARKO data from {DARKO_URL}...")
    time.sleep(1.0)
    try:
        resp = requests.get(DARKO_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        print(f"  ✅ {len(data)} players returned")
        return data
    except Exception as e:
        print(f"  ❌ Failed: {e}")
        return []


def upsert_darko(players, season, season_type):
    if not players:
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    # Ensure columns exist
    for col in ['darko_dpm', 'darko_odpm', 'darko_ddpm', 'darko_box']:
        cur.execute(f"ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS {col} REAL")
    conn.commit()

    updated = 0
    skipped = 0

    for p in players:
        nba_id = p.get('nba_id')
        if not nba_id:
            continue

        dpm  = p.get('dpm')
        odpm = p.get('o_dpm')
        ddpm = p.get('d_dpm')
        box  = p.get('box_dpm')

        cur.execute("""
            UPDATE player_seasons SET
                darko_dpm  = %s,
                darko_odpm = %s,
                darko_ddpm = %s,
                darko_box  = %s
            WHERE player_id = %s
              AND season = %s
              AND season_type = %s
        """, (dpm, odpm, ddpm, box, nba_id, season, season_type))

        if cur.rowcount > 0:
            updated += 1
        else:
            skipped += 1

    conn.commit()

    # Verify
    chk = conn.cursor()
    chk.execute("""
        SELECT COUNT(*) FROM player_seasons
        WHERE darko_dpm IS NOT NULL
          AND season = %s AND season_type = %s
    """, (season, season_type))
    count = chk.fetchone()[0]
    chk.close()
    cur.close()
    conn.close()

    print(f"  ✅ Updated: {updated} players")
    print(f"  ⏭  Skipped (not in DB for this season): {skipped}")
    print(f"  📊 Total with DARKO data: {count}")


def spot_check(season, season_type):
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT p.player_name, ps.darko_dpm, ps.darko_odpm, ps.darko_ddpm, ps.darko_box
        FROM player_seasons ps
        JOIN players p ON ps.player_id = p.player_id
        WHERE ps.season = %s AND ps.season_type = %s
          AND ps.darko_dpm IS NOT NULL
        ORDER BY ps.darko_dpm DESC NULLS LAST
        LIMIT 10
    """, (season, season_type))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"\n  Top 10 DARKO DPM:")
    print(f"  {'Player':<25} {'DPM':>7} {'ODPM':>7} {'DDPM':>7} {'BOX':>7}")
    print(f"  {'─'*55}")
    for r in rows:
        print(f"  {r['player_name']:<25} "
              f"{r['darko_dpm'] or 0:>+7.2f} "
              f"{r['darko_odpm'] or 0:>+7.2f} "
              f"{r['darko_ddpm'] or 0:>+7.2f} "
              f"{r['darko_box'] or 0:>+7.2f}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--season',      default=SEASON)
    parser.add_argument('--season-type', default=SEASON_TYPE)
    args = parser.parse_args()

    season      = args.season
    season_type = args.season_type

    print(f"\nThe Impact Board — Fetch DARKO DPM")
    print(f"Season: {season} | {season_type}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    players = fetch_darko()
    if not players:
        print("❌ No data returned. Exiting.")
        sys.exit(1)

    print(f"Writing to database...")
    upsert_darko(players, season, season_type)
    spot_check(season, season_type)

    print(f"\n✅ Done — {datetime.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == '__main__':
    main()