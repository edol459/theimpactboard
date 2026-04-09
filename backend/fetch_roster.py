"""
ydkball — Fetch Team Rosters
============================================
python backend/fetch_roster.py [--season 2025-26] [--team BOS]

Pulls current rosters from the NBA API via CommonTeamRoster
and upserts them into team_rosters. Much faster than fetch_lineups.py
since it skips the 5-man lineup data (now sourced from pbpstats).

By default fetches all 30 teams for the given season.
Pass --team to refresh a single team.
"""
import os, sys, time, argparse
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

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


def fetch_team_roster(cur, team_abbr, season):
    from nba_api.stats.endpoints import CommonTeamRoster

    team_id = TEAM_IDS[team_abbr]
    try:
        df = CommonTeamRoster(team_id=team_id, season=season).get_data_frames()[0]
        time.sleep(0.8)
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO team_rosters (team_abbr, season, player_id, player_name, number, position)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_abbr, season, player_id) DO UPDATE SET
                    player_name = EXCLUDED.player_name,
                    number      = EXCLUDED.number,
                    position    = EXCLUDED.position,
                    updated_at  = NOW()
            """, (
                team_abbr, season,
                str(int(row["PLAYER_ID"])),
                str(row["PLAYER"]),
                str(row.get("NUM", "") or ""),
                str(row.get("POSITION", "") or ""),
            ))
        print(f"  {team_abbr}: {len(df)} players")
    except Exception as e:
        print(f"  {team_abbr}: error — {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2025-26")
    parser.add_argument("--team",   default=None, help="Single team abbr, e.g. BOS")
    args = parser.parse_args()

    teams = [args.team.upper()] if args.team else list(TEAM_IDS.keys())
    unknown = [t for t in teams if t not in TEAM_IDS]
    if unknown:
        print(f"Unknown teams: {unknown}"); sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    print(f"Fetching rosters for {len(teams)} team(s), {args.season}...\n")
    for abbr in teams:
        fetch_team_roster(cur, abbr, args.season)
        conn.commit()

    cur.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
