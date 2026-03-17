"""
The Impact Board — Fetch Net Points (ESPN Analytics)
======================================================
python backend/ingest/fetch_net_pts.py

Fetches from https://nfl-player-metrics.s3.amazonaws.com/net-pts/nba_net_pts100_data.json
Returns all players/seasons — we filter by season year and match by name.
Updates player_seasons with net_pts100, o_net_pts100, d_net_pts100.

Run manually or via daily_update.py.
"""

import os
import sys
import time
import math
import requests
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
SEASON       = os.getenv('NBA_SEASON', '2024-25')
SEASON_TYPE  = os.getenv('NBA_SEASON_TYPE', 'Regular Season')

NET_PTS_URL = 'https://nfl-player-metrics.s3.amazonaws.com/net-pts/nba_net_pts100_data.json'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
}

if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)


def season_to_year(season):
    """Convert season string to ESPN's year convention.
    ESPN uses the starting year: '2025-26' → 2025, '2024-25' → 2024."""
    try:
        return int(season.split('-')[0])
    except:
        return None


def normalize_name(name):
    """Lowercase, strip accents, remove suffixes."""
    name = name.lower().strip()
    for suffix in [' jr.', ' sr.', ' iii', ' ii', ' iv']:
        name = name.replace(suffix, '')
    replacements = {
        'č':'c','ć':'c','š':'s','ž':'z','đ':'d',
        'á':'a','é':'e','í':'i','ó':'o','ú':'u',
        'ā':'a','ē':'e','ī':'i','ō':'o','ū':'u',
        'ő':'o','ű':'u','ö':'o','ü':'u','ä':'a',
        'ñ':'n','ç':'c','ğ':'g','ı':'i',
        "'":"'","'":"'",
    }
    for src, dst in replacements.items():
        name = name.replace(src, dst)
    return name.strip()


# Manual overrides: ESPN name → DB name
NAME_OVERRIDES = {
    'kristaps porzingis':  'Kristaps Porziņģis',
    'nikola jokic':        'Nikola Jokić',
    'luka doncic':         'Luka Dončić',
    'goga bitadze':        'Goga Bitadze',
    'alperen sengun':      'Alperen Şengün',
    'svi mykhailiuk':      'Svi Mykhailiuk',
    'bojan bogdanovic':    'Bojan Bogdanović',
    'dario saric':         'Dario Šarić',
    'ivica zubac':         'Ivica Zubac',
    'jusuf nurkic':        'Jusuf Nurkić',
    'nikola vucevic':      'Nikola Vučević',
    'bogdan bogdanovic':   'Bogdan Bogdanović',
    'moritz wagner':       'Moritz Wagner',
    'tim hardaway':        'Tim Hardaway Jr.',
    'carlton carrington':  'Carlton Carrington',
    'pacome dadiet':       'Pacôme Dadiet',
    'alexandre sarr':      'Alexandre Sarr',
}


def fetch_net_pts():
    print(f"Fetching Net Points data from S3...")
    time.sleep(1.0)
    try:
        resp = requests.get(NET_PTS_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # Handle both list and dict responses
        if isinstance(data, list):
            players = data
        elif isinstance(data, dict):
            players = data.get('players', data.get('data', []))
        else:
            players = []
        print(f"  ✅ {len(players)} total records returned")
        return players
    except Exception as e:
        print(f"  ❌ Failed: {e}")
        return []


def upsert_net_pts(all_players, season, season_type):
    if not all_players:
        return

    target_year = season_to_year(season)
    if not target_year:
        print(f"  ❌ Could not parse season year from '{season}'")
        return

    # Filter to the target season
    # Records where max_season == target_year represent that season's data
    season_players = [
        p for p in all_players
        if p.get('max_season') == target_year
        and p.get('seasonType', 'Regular Season') == season_type
    ]

    # Fallback: if no exact match, try min_season
    if not season_players:
        season_players = [
            p for p in all_players
            if p.get('min_season') == target_year
        ]

    print(f"  Filtered to {len(season_players)} players for {season} ({target_year})")

    if not season_players:
        print(f"  ⚠️  No players found for season year {target_year}")
        print(f"  Available years: {sorted(set(p.get('max_season') for p in all_players if p.get('max_season')))}")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    for col, typ in [
        ('net_pts100',   'REAL'),
        ('o_net_pts100', 'REAL'),
        ('d_net_pts100', 'REAL'),
    ]:
        cur.execute(f"ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS {col} {typ}")
    conn.commit()

    # Load DB players for name matching
    cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur2.execute("""
        SELECT p.player_id, p.player_name
        FROM players p
        JOIN player_seasons ps ON p.player_id = ps.player_id
        WHERE ps.season = %s AND ps.season_type = %s
    """, (season, season_type))
    db_players   = cur2.fetchall()
    db_by_norm   = {normalize_name(r['player_name']): r['player_id'] for r in db_players}
    db_by_lower  = {r['player_name'].lower(): r['player_id'] for r in db_players}
    cur2.close()

    updated = 0
    skipped = 0
    unmatched = []

    for p in season_players:
        full_nm = p.get('full_nm', '').strip()
        if not full_nm:
            continue

        norm = normalize_name(full_nm)
        pid  = None

        # Check manual override first
        override = NAME_OVERRIDES.get(norm)
        if override:
            pid = db_by_lower.get(override.lower()) or db_by_norm.get(normalize_name(override))

        if not pid:
            pid = db_by_norm.get(norm) or db_by_lower.get(full_nm.lower())

        # Try last/first swap
        if not pid:
            parts = norm.split()
            if len(parts) == 2:
                pid = db_by_norm.get(f"{parts[1]} {parts[0]}")

        if not pid:
            unmatched.append(full_nm)
            continue

        net   = p.get('tNet100')
        o_net = p.get('oNet100')
        d_net = p.get('dNet100')

        # Clean NaN/None
        def clean(v):
            if v is None: return None
            try:
                f = float(v)
                return None if math.isnan(f) else f
            except: return None

        cur.execute("""
            UPDATE player_seasons SET
                net_pts100   = %s,
                o_net_pts100 = %s,
                d_net_pts100 = %s
            WHERE player_id = %s
              AND season = %s
              AND season_type = %s
        """, (clean(net), clean(o_net), clean(d_net), pid, season, season_type))

        if cur.rowcount > 0:
            updated += 1
        else:
            skipped += 1

    conn.commit()

    chk = conn.cursor()
    chk.execute("""
        SELECT COUNT(*) FROM player_seasons
        WHERE net_pts100 IS NOT NULL
          AND season = %s AND season_type = %s
    """, (season, season_type))
    count = chk.fetchone()[0]
    chk.close()
    cur.close()
    conn.close()

    print(f"  ✅ Updated: {updated} players")
    print(f"  ⏭  No DB row for season: {skipped}")
    print(f"  ⚠️  Unmatched names: {len(unmatched)}")
    if unmatched:
        print(f"  First 10: {unmatched[:10]}")
    print(f"  📊 Total with Net Points data: {count}")


def spot_check(season, season_type):
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT p.player_name, p.position_group,
               ps.net_pts100, ps.o_net_pts100, ps.d_net_pts100, ps.min
        FROM player_seasons ps
        JOIN players p ON ps.player_id = p.player_id
        WHERE ps.season = %s AND ps.season_type = %s
          AND ps.net_pts100 IS NOT NULL
          AND ps.min >= 1000
        ORDER BY ps.net_pts100 DESC NULLS LAST
        LIMIT 15
    """, (season, season_type))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"\n  Top 15 Net Points per 100 (min 1000 min):")
    print(f"  {'Player':<25} {'POS':<4} {'NET':>7} {'OFF':>7} {'DEF':>7}")
    print(f"  {'─'*54}")
    for r in rows:
        print(f"  {r['player_name']:<25} {r['position_group'] or '':<4} "
              f"{r['net_pts100'] or 0:>+7.2f} "
              f"{r['o_net_pts100'] or 0:>+7.2f} "
              f"{r['d_net_pts100'] or 0:>+7.2f}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--season',      default=SEASON)
    parser.add_argument('--season-type', default=SEASON_TYPE)
    args = parser.parse_args()

    season      = args.season
    season_type = args.season_type

    print(f"\nThe Impact Board — Fetch Net Points (ESPN)")
    print(f"Season: {season} | {season_type}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    all_players = fetch_net_pts()
    if not all_players:
        print("❌ No data returned. Exiting.")
        sys.exit(1)

    print(f"Writing to database...")
    upsert_net_pts(all_players, season, season_type)
    spot_check(season, season_type)

    print(f"\n✅ Done — {datetime.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == '__main__':
    main()