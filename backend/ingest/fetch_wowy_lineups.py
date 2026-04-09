"""
ydkball — Fetch WoWY Lineup Data (leverage-filtered)
=====================================================
python backend/ingest/fetch_wowy_lineups.py [--season 2025-26] [--team BOS]

Pulls leverage-filtered (Medium + High + VeryHigh) 5-man lineup WoWY data
from pbpstats and upserts into the wowy_lineups table.

Run from your local machine (residential IP required — pbpstats blocks Railway).
Omit --season to fetch all seasons from player_seasons. Omit --team for all 30.

Table schema (created if missing):
    wowy_lineups (
        team_abbr   TEXT,
        season      TEXT,
        group_id    TEXT,          -- dash-separated player IDs (pbpstats EntityId)
        player_ids  TEXT[],
        player_names TEXT[],
        min         NUMERIC,
        ortg        NUMERIC,
        drtg        NUMERIC,
        net         NUMERIC,
        updated_at  TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (team_abbr, season, group_id)
    )
"""

import os, sys, time, argparse, math
import urllib.parse
import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not set."); sys.exit(1)

TEAM_IDS = {
    "ATL":1610612737,"BOS":1610612738,"BKN":1610612751,"CHA":1610612766,
    "CHI":1610612741,"CLE":1610612739,"DAL":1610612742,"DEN":1610612743,
    "DET":1610612765,"GSW":1610612744,"HOU":1610612745,"IND":1610612754,
    "LAC":1610612746,"LAL":1610612747,"MEM":1610612763,"MIA":1610612748,
    "MIL":1610612749,"MIN":1610612750,"NOP":1610612740,"NYK":1610612752,
    "OKC":1610612760,"ORL":1610612753,"PHI":1610612755,"PHX":1610612756,
    "POR":1610612757,"SAC":1610612758,"SAS":1610612759,"TOR":1610612761,
    "UTA":1610612762,"WAS":1610612764,
}

LEVERAGE = "Medium,High,VeryHigh"


def ensure_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wowy_lineups (
            team_abbr    TEXT        NOT NULL,
            season       TEXT        NOT NULL,
            group_id     TEXT        NOT NULL,
            player_ids   TEXT[]      NOT NULL,
            player_names TEXT[]      NOT NULL,
            min          NUMERIC,
            ortg         NUMERIC,
            drtg         NUMERIC,
            net          NUMERIC,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (team_abbr, season, group_id)
        )
    """)


def get_all_seasons(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT season FROM player_seasons
        ORDER BY season
    """)
    seasons = [r[0] for r in cur.fetchall()]
    cur.close()
    return seasons


def fetch_team_season(cur, team_abbr, season, retries=3):
    team_id = TEAM_IDS[team_abbr]
    params = {
        "TeamId":     team_id,
        "Season":     season,
        "SeasonType": "Regular Season",
        "Type":       "Team",
    }
    # Commas must NOT be percent-encoded — pbpstats rejects %2C
    url = "https://api.pbpstats.com/get-wowy-stats/nba?" + urllib.parse.urlencode(params)
    url += "&Leverage=" + LEVERAGE

    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            break
        except (requests.exceptions.Timeout, requests.exceptions.ReadTimeout):
            wait = 15 * (attempt + 1)
            if attempt < retries - 1:
                print(f"    timeout, retrying in {wait}s…")
                time.sleep(wait)
            else:
                print(f"    gave up after {retries} timeouts")
                return 0
        except requests.exceptions.HTTPError as e:
            wait = 20 * (attempt + 1)
            if attempt < retries - 1:
                print(f"    HTTP {e.response.status_code}, retrying in {wait}s…")
                time.sleep(wait)
            else:
                print(f"    gave up: {e}")
                return 0
    else:
        return 0

    count = 0
    for row in resp.json().get("multi_row_table_data", []):
        if not row or not row.get("EntityId") or not row.get("Minutes"):
            continue
        group_id = row["EntityId"]
        pids     = [p for p in group_id.split("-") if p.strip()]
        names    = [n.strip() for n in row.get("Name", "").split(",")]

        off_poss = row.get("OffPoss") or 0
        def_poss = row.get("DefPoss") or 0
        points   = row.get("Points") or 0
        opp_pts  = row.get("OpponentPoints") or 0

        ortg = round(points  / off_poss * 100, 1) if off_poss else None
        drtg = round(opp_pts / def_poss * 100, 1) if def_poss else None
        net  = round(ortg - drtg, 1) if ortg is not None and drtg is not None else None

        minutes = row["Minutes"]
        if isinstance(minutes, float) and (math.isnan(minutes) or math.isinf(minutes)):
            minutes = None

        cur.execute("""
            INSERT INTO wowy_lineups
                (team_abbr, season, group_id, player_ids, player_names, min, ortg, drtg, net, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (team_abbr, season, group_id) DO UPDATE SET
                player_ids   = EXCLUDED.player_ids,
                player_names = EXCLUDED.player_names,
                min          = EXCLUDED.min,
                ortg         = EXCLUDED.ortg,
                drtg         = EXCLUDED.drtg,
                net          = EXCLUDED.net,
                updated_at   = NOW()
        """, (team_abbr, season, group_id, pids, names,
              round(minutes) if minutes is not None else None,
              ortg, drtg, net))
        count += 1

    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default=None, help="e.g. 2025-26 (omit for all seasons)")
    parser.add_argument("--team",   default=None, help="e.g. BOS (omit for all 30 teams)")
    args = parser.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    ensure_table(cur)
    conn.commit()

    teams   = [args.team.upper()] if args.team else list(TEAM_IDS.keys())
    unknown = [t for t in teams if t not in TEAM_IDS]
    if unknown:
        print(f"Unknown teams: {unknown}"); sys.exit(1)

    seasons = [args.season] if args.season else get_all_seasons(conn)
    if not seasons:
        print("No seasons found in DB."); sys.exit(1)

    print(f"\nFetching WoWY lineups (leverage: {LEVERAGE})")
    print(f"Teams: {len(teams)}  |  Seasons: {len(seasons)}  |  Total: {len(teams)*len(seasons)} calls")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    total_rows = 0
    for s_idx, season in enumerate(seasons, 1):
        print(f"── Season {season} ({s_idx}/{len(seasons)}) ──")
        for t_idx, abbr in enumerate(teams, 1):
            print(f"  [{t_idx:2}/{len(teams)}] {abbr}… ", end="", flush=True)
            count = fetch_team_season(cur, abbr, season)
            conn.commit()
            print(f"{count} lineups")
            total_rows += count
            # Be polite between calls — pbpstats rate-limits aggressive clients
            if not (s_idx == len(seasons) and t_idx == len(teams)):
                time.sleep(3.0)

    cur.close()
    conn.close()

    print(f"\nDone. {total_rows} rows upserted.")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
