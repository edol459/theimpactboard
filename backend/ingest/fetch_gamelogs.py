"""
ydkball — Fetch Player Game Logs
=================================
python backend/ingest/fetch_gamelogs.py [--season 2025-26] [--season-type "Regular Season"]

Fetches per-game box score data for every player via NBA API PlayerGameLogs
(Base measure type), computes ts_pct per row, and upserts into player_gamelogs.
Run after fetch_season.py in the daily local update pipeline.
"""

import os
import sys
import time
import argparse
from datetime import date
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


def get_current_season() -> str:
    today = date.today()
    y, m = today.year, today.month
    if m >= 10:
        return f"{y}-{str(y + 1)[2:]}"
    return f"{y - 1}-{str(y)[2:]}"


def fetch_gamelogs(season: str, season_type: str):
    try:
        from nba_api.stats.endpoints import PlayerGameLogs
    except ImportError:
        print("❌ nba_api not installed. Run: pip install nba_api")
        sys.exit(1)

    print(f"\nFetching game logs — {season} {season_type} ...")
    time.sleep(1)

    try:
        ep = PlayerGameLogs(
            season_nullable=season,
            season_type_nullable=season_type,
            league_id_nullable="00",
            measure_type_player_game_logs_nullable="Base",
        )
        df = ep.get_data_frames()[0]
    except Exception as e:
        print(f"❌ NBA API error: {e}")
        sys.exit(1)

    print(f"  Fetched {len(df)} game-log rows for {df['PLAYER_NAME'].nunique()} players")

    # Compute ts_pct per game row
    def compute_ts(row):
        denom = 2 * (row["FGA"] + 0.44 * row["FTA"])
        if denom > 0:
            return round(row["PTS"] / denom, 4)
        return None

    df["ts_pct_calc"] = df.apply(compute_ts, axis=1)

    rows = []
    for _, r in df.iterrows():
        rows.append({
            "player_id":   int(r["PLAYER_ID"]),
            "player_name": str(r["PLAYER_NAME"]),
            "season":      season,
            "season_type": season_type,
            "game_id":     str(r["GAME_ID"]),
            "game_date":   str(r["GAME_DATE"])[:10],  # YYYY-MM-DD
            "matchup":     str(r.get("MATCHUP", "")),
            "wl":          str(r.get("WL", "")),
            "min":         float(r["MIN"]) if r["MIN"] is not None else None,
            "pts":         float(r["PTS"]) if r["PTS"] is not None else None,
            "ast":         float(r["AST"]) if r["AST"] is not None else None,
            "reb":         float(r["REB"]) if r["REB"] is not None else None,
            "fg3m":        float(r["FG3M"]) if r["FG3M"] is not None else None,
            "fgm":         float(r["FGM"]) if r["FGM"] is not None else None,
            "fga":         float(r["FGA"]) if r["FGA"] is not None else None,
            "ftm":         float(r["FTM"]) if r["FTM"] is not None else None,
            "fta":         float(r["FTA"]) if r["FTA"] is not None else None,
            "ts_pct":      r["ts_pct_calc"],
        })

    if not rows:
        print("  No rows to upsert.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    # Filter to only players already present in the players table
    cur.execute("SELECT player_id FROM players")
    known_ids = {r[0] for r in cur.fetchall()}
    skipped = [r for r in rows if r["player_id"] not in known_ids]
    rows     = [r for r in rows if r["player_id"] in known_ids]
    if skipped:
        print(f"  ⚠️  Skipped {len(skipped)} rows for {len({r['player_id'] for r in skipped})} unknown player(s) (not in players table)")

    if not rows:
        print("  No rows to upsert after filtering.")
        cur.close(); conn.close()
        return

    upsert_sql = """
        INSERT INTO player_gamelogs
            (player_id, player_name, season, season_type,
             game_id, game_date, matchup, wl,
             min, pts, ast, reb, fg3m, fgm, fga, ftm, fta, ts_pct)
        VALUES
            (%(player_id)s, %(player_name)s, %(season)s, %(season_type)s,
             %(game_id)s, %(game_date)s, %(matchup)s, %(wl)s,
             %(min)s, %(pts)s, %(ast)s, %(reb)s, %(fg3m)s, %(fgm)s,
             %(fga)s, %(ftm)s, %(fta)s, %(ts_pct)s)
        ON CONFLICT (player_id, game_id, season_type) DO UPDATE SET
            player_name = EXCLUDED.player_name,
            game_date   = EXCLUDED.game_date,
            matchup     = EXCLUDED.matchup,
            wl          = EXCLUDED.wl,
            min         = EXCLUDED.min,
            pts         = EXCLUDED.pts,
            ast         = EXCLUDED.ast,
            reb         = EXCLUDED.reb,
            fg3m        = EXCLUDED.fg3m,
            fgm         = EXCLUDED.fgm,
            fga         = EXCLUDED.fga,
            ftm         = EXCLUDED.ftm,
            fta         = EXCLUDED.fta,
            ts_pct      = EXCLUDED.ts_pct
    """

    psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=500)
    conn.commit()
    cur.close()
    conn.close()

    print(f"  ✅ Upserted {len(rows)} game-log rows into player_gamelogs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season",      default=get_current_season())
    parser.add_argument("--season-type", default="Regular Season")
    args = parser.parse_args()

    fetch_gamelogs(args.season, args.season_type)
