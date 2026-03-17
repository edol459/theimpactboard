"""
The Impact Board — Fetch LEBRON
================================
python backend/ingest/fetch_lebron.py

Fetches from https://fanspo.com/bbi-role-explorer/api/lebron_dashboard_data
Matches on nba_id directly.
Updates player_seasons with lebron, o_lebron, d_lebron, war.

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
SEASON       = os.getenv('NBA_SEASON', '2024-25')
SEASON_TYPE  = os.getenv('NBA_SEASON_TYPE', 'Regular Season')

LEBRON_URL = 'https://fanspo.com/bbi-role-explorer/api/lebron_dashboard_data'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Referer': 'https://fanspo.com/',
}

if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)


def fetch_lebron(season):
    # Convert season string like "2025-26" to the year format the API expects ("2026")
    api_year = str(int(season.split('-')[0]) + 1)

    payload = {
        "seasons":            [api_year],
        "positions":          [],
        "offensiveArchetypes":[],
        "defensiveRoles":     [],
        "playerRoles":        [],
        "teams":              [],
        "seasonView":         "average",
        "minMinutes":         200,
        "minMpg":             10,
    }

    print(f"Fetching LEBRON data (season {api_year})...")
    time.sleep(1.0)
    try:
        resp = requests.post(
            LEBRON_URL,
            headers={**HEADERS, 'Content-Type': 'application/json'},
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        players = data.get('players', [])
        print(f"  ✅ {len(players)} players returned")
        return players
    except Exception as e:
        print(f"  ❌ Failed: {e}")
        return []


def upsert_lebron(players, season, season_type):
    if not players:
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    # Ensure columns exist
    for col, typ in [
        ('lebron',   'REAL'),
        ('o_lebron', 'REAL'),
        ('d_lebron', 'REAL'),
        ('war',      'REAL'),
    ]:
        cur.execute(f"ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS {col} {typ}")
    conn.commit()

    updated = 0
    skipped = 0

    for p in players:
        nba_id = p.get('nba_id')
        if not nba_id:
            continue

        try:
            nba_id = int(nba_id)
        except (ValueError, TypeError):
            continue

        lebron   = p.get('LEBRON')
        o_lebron = p.get('O-LEBRON')
        d_lebron = p.get('D-LEBRON')
        war      = p.get('WAR')

        cur.execute("""
            UPDATE player_seasons SET
                lebron   = %s,
                o_lebron = %s,
                d_lebron = %s,
                war      = %s
            WHERE player_id = %s
              AND season = %s
              AND season_type = %s
        """, (lebron, o_lebron, d_lebron, war, nba_id, season, season_type))

        if cur.rowcount > 0:
            updated += 1
        else:
            skipped += 1

    conn.commit()

    # Verify
    chk = conn.cursor()
    chk.execute("""
        SELECT COUNT(*) FROM player_seasons
        WHERE lebron IS NOT NULL
          AND season = %s AND season_type = %s
    """, (season, season_type))
    count = chk.fetchone()[0]
    chk.close()
    cur.close()
    conn.close()

    print(f"  ✅ Updated: {updated} players")
    print(f"  ⏭  Skipped (not in DB for this season): {skipped}")
    print(f"  📊 Total with LEBRON data: {count}")


def spot_check(season, season_type):
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT p.player_name, ps.lebron, ps.o_lebron, ps.d_lebron, ps.war
        FROM player_seasons ps
        JOIN players p ON ps.player_id = p.player_id
        WHERE ps.season = %s AND ps.season_type = %s
          AND ps.lebron IS NOT NULL
        ORDER BY ps.lebron DESC NULLS LAST
        LIMIT 15
    """, (season, season_type))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"\n  Top 15 LEBRON:")
    print(f"  {'Player':<25} {'LEBRON':>7} {'O-LBN':>7} {'D-LBN':>7} {'WAR':>6}")
    print(f"  {'─'*57}")
    for r in rows:
        print(f"  {r['player_name']:<25} "
              f"{r['lebron'] or 0:>+7.2f} "
              f"{r['o_lebron'] or 0:>+7.2f} "
              f"{r['d_lebron'] or 0:>+7.2f} "
              f"{r['war'] or 0:>6.2f}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--season',      default=SEASON)
    parser.add_argument('--season-type', default=SEASON_TYPE)
    args = parser.parse_args()

    season      = args.season
    season_type = args.season_type

    print(f"\nThe Impact Board — Fetch LEBRON")
    print(f"Season: {season} | {season_type}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    players = fetch_lebron(season)
    if not players:
        print("❌ No data returned. Exiting.")
        sys.exit(1)

    print(f"Writing to database...")
    upsert_lebron(players, season, season_type)
    spot_check(season, season_type)

    print(f"\n✅ Done — {datetime.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == '__main__':
    main()