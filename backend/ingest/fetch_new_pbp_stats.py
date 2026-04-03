"""
The Impact Board — Incremental PBP Stats
==========================================
python backend/ingest/fetch_new_pbp_stats.py

Fetches PBP data only for games played since the last run.
Updates bad_pass_tov (and any other PBP-derived stats).

On a typical day: 5-15 games = ~30 seconds.
On first run after fetch_bad_pass_tov.py: 0 new games.

Reads/writes bad_pass_progress.json to track processed games.
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
DELAY        = 1.8

parser = argparse.ArgumentParser()
parser.add_argument('--season',      default=os.getenv('NBA_SEASON', '2024-25'))
parser.add_argument('--season-type', default=os.getenv('NBA_SEASON_TYPE', 'Regular Season'))
args = parser.parse_args()

SEASON      = args.season
SEASON_TYPE = args.season_type

season_slug   = SEASON.replace('-', '_')
PROGRESS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'pbp_progress_{season_slug}.json')

if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)

def try_import(name):
    try:
        import importlib
        mod = importlib.import_module("nba_api.stats.endpoints")
        return getattr(mod, name)
    except (ImportError, AttributeError):
        return None

LeagueGameFinder = try_import("LeagueGameFinder")
PlayByPlayV3     = try_import("PlayByPlayV3")


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {'processed_games': []}


def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)


def get_new_game_ids(season, season_type, already_processed):
    """Get game IDs played in the last 2 days that haven't been processed."""
    print(f"Checking for new games...")
    time.sleep(DELAY)

    # Look back 2 days to catch any games we might have missed
    date_from = (datetime.now() - timedelta(days=2)).strftime('%m/%d/%Y')

    try:
        gf = LeagueGameFinder(
            season_nullable=season,
            season_type_nullable=season_type,
            league_id_nullable="00",
            date_from_nullable=date_from,
        )
        df = gf.get_data_frames()[0]
        all_ids   = set(df['GAME_ID'].unique().tolist())
        processed = set(already_processed)
        new_ids   = sorted(all_ids - processed)
        print(f"  Found {len(all_ids)} recent games, {len(new_ids)} new")
        return new_ids
    except Exception as e:
        print(f"  ❌ {e}")
        return []


def process_game_pbp(game_id):
    """
    Pull PBP for one game and extract all PBP-derived stats.
    Returns dict of dicts: {stat_name: {player_id: count}}

    Currently tracks:
      - bad_pass_tov

    Add more here as needed — each one just needs a filter condition.
    """
    time.sleep(DELAY)
    try:
        pbp = PlayByPlayV3(game_id=game_id).get_data_frames()[0]

        results = {
            'bad_pass_tov':  defaultdict(int),
            'lost_ball_tov': defaultdict(int),
        }

        for _, row in pbp.iterrows():
            if str(row.get('actionType', '')).strip() != 'Turnover':
                continue

            try:
                pid = int(float(row['personId']))
                if pid <= 0:
                    continue
            except:
                continue

            sub = str(row.get('subType', '')).strip()

            if sub == 'Bad Pass':
                results['bad_pass_tov'][pid] += 1
            elif sub == 'Lost Ball':
                results['lost_ball_tov'][pid] += 1

        # Convert defaultdicts to regular dicts
        return {k: dict(v) for k, v in results.items()}

    except Exception as e:
        return None


def update_db(stat_totals, season, season_type):
    """
    Increment PBP stat counts in player_seasons by the new-game delta.
    stat_totals: {stat_name: {player_id: new_game_count}}
    Uses += so the DB is the source of truth — no risk of overwriting season data.
    """
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    # Ensure columns exist
    for stat_name in stat_totals.keys():
        cur.execute(f"""
            ALTER TABLE player_seasons
            ADD COLUMN IF NOT EXISTS {stat_name} REAL
        """)
    conn.commit()

    total_updates = 0
    for stat_name, player_counts in stat_totals.items():
        for pid, count in player_counts.items():
            cur.execute(f"""
                UPDATE player_seasons
                SET {stat_name} = COALESCE({stat_name}, 0) + %s
                WHERE player_id = %s AND season = %s AND season_type = %s
            """, (count, pid, season, season_type))
            if cur.rowcount > 0:
                total_updates += 1

    conn.commit()
    cur.close()
    conn.close()
    return total_updates


def main():
    print(f"\nThe Impact Board — Incremental PBP Stats")
    print(f"Season: {SEASON} {SEASON_TYPE}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    # Progress file only tracks which game IDs have been processed.
    # The DB is the source of truth for accumulated totals.
    progress = load_progress()
    processed_games = progress.get('processed_games', [])

    print(f"Previously processed: {len(processed_games)} games")

    # Find new games (last 2 days, not yet processed)
    new_games = get_new_game_ids(SEASON, SEASON_TYPE, processed_games)

    if not new_games:
        print("✅ No new games to process — already up to date")
        return

    print(f"\nProcessing {len(new_games)} new games...")

    # Accumulate counts only for new games
    new_counts = {'bad_pass_tov': defaultdict(int), 'lost_ball_tov': defaultdict(int)}
    failed     = []

    for i, game_id in enumerate(new_games):
        result = process_game_pbp(game_id)

        if result is None:
            failed.append(game_id)
            print(f"  [{i+1}/{len(new_games)}] {game_id} ❌")
            continue

        for stat_name, counts in result.items():
            for pid, count in counts.items():
                new_counts[stat_name][pid] += count

        processed_games.append(game_id)
        print(f"  [{i+1}/{len(new_games)}] {game_id} ✅ "
              f"({sum(result['bad_pass_tov'].values())} bad passes, "
              f"{sum(result['lost_ball_tov'].values())} lost balls)")

    # Save updated processed game IDs
    save_progress({'processed_games': processed_games})

    # Increment DB totals by new-game counts only
    print(f"\nWriting to database...")
    updates = update_db(new_counts, SEASON, SEASON_TYPE)
    print(f"  ✅ {updates} player-stat rows updated")

    if failed:
        print(f"\n⚠️  {len(failed)} games failed: {failed}")

    print(f"\n✅ Incremental PBP update complete")
    print(f"   {len(new_games) - len(failed)} games processed")


if __name__ == "__main__":
    main()