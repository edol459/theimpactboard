"""
The Impact Board — Schema Creation
====================================
python backend/schema.py

Creates all tables in the PostgreSQL database.
Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    print("❌ DATABASE_URL not found in environment. Check your .env file.")
    sys.exit(1)

SCHEMA = """

-- ── Players ───────────────────────────────────────────────────
-- Master player registry — one row per player, updated each season
CREATE TABLE IF NOT EXISTS players (
    player_id        INTEGER PRIMARY KEY,
    player_name      TEXT NOT NULL,
    position         TEXT,
    position_group   TEXT,   -- normalized: G, GF, F, FC, C
    height_inches    REAL,
    weight           INTEGER,
    draft_year       INTEGER,
    draft_round      INTEGER,
    draft_number     INTEGER,
    college          TEXT,
    country          TEXT,
    is_active        BOOLEAN DEFAULT TRUE,
    updated_at       TIMESTAMP DEFAULT NOW()
);

-- ── Raw season stats ──────────────────────────────────────────
-- One row per player per season per season_type
-- Stores everything fetched from NBA API endpoints as-is
CREATE TABLE IF NOT EXISTS player_seasons (
    id               SERIAL PRIMARY KEY,
    player_id        INTEGER REFERENCES players(player_id) ON DELETE CASCADE,
    season           TEXT NOT NULL,        -- '2024-25'
    season_type      TEXT NOT NULL,        -- 'Regular Season'
    league           TEXT DEFAULT 'NBA',
    team_id          INTEGER,
    team_abbr        TEXT,

    -- Qualification
    gp               INTEGER,
    min              REAL,
    min_per_game     REAL,
    poss             REAL,

    -- Box score base (per game)
    pts              REAL,
    ast              REAL,
    reb              REAL,
    oreb             REAL,
    dreb             REAL,
    stl              REAL,
    blk              REAL,
    tov              REAL,
    pf               REAL,
    pfd              REAL,
    fgm              REAL,
    fga              REAL,
    fg_pct           REAL,
    fg3m             REAL,
    fg3a             REAL,
    fg3_pct          REAL,
    ftm              REAL,
    fta              REAL,
    ft_pct           REAL,
    plus_minus       REAL,

    -- Advanced (per game)
    off_rating       REAL,
    def_rating       REAL,
    net_rating       REAL,
    ast_pct          REAL,
    ast_to           REAL,
    ast_ratio        REAL,
    oreb_pct         REAL,
    dreb_pct         REAL,
    reb_pct          REAL,
    tm_tov_pct       REAL,
    efg_pct          REAL,
    ts_pct           REAL,
    usg_pct          REAL,
    pace             REAL,
    pie              REAL,

    -- Misc (per game)
    pts_off_tov      REAL,
    pts_2nd_chance   REAL,
    pts_fb           REAL,
    pts_paint        REAL,
    opp_pts_off_tov  REAL,
    opp_pts_paint    REAL,

    -- Scoring breakdown (per game)
    pct_uast_2pm     REAL,
    pct_uast_3pm     REAL,
    pct_uast_fgm     REAL,
    pct_ast_fgm      REAL,
    pct_pts_paint    REAL,
    pct_pts_3pt      REAL,
    pct_pts_ft       REAL,
    pct_pts_mid2     REAL,

    -- Usage (per game)
    pct_fga          REAL,
    pct_fta          REAL,
    pct_ast          REAL,
    pct_tov          REAL,

    -- Tracking: drives (season totals)
    drives           REAL,
    drive_fga        REAL,
    drive_fgm        REAL,
    drive_fg_pct     REAL,
    drive_pts        REAL,
    drive_ast        REAL,
    drive_tov        REAL,
    drive_pf         REAL,
    drive_passes     REAL,
    drive_ft_pct     REAL,

    -- Tracking: passing (season totals)
    passes_made      REAL,
    passes_received  REAL,
    ast_pts_created  REAL,
    secondary_ast    REAL,
    potential_ast    REAL,
    ft_ast           REAL,
    ast_to_pass_pct  REAL,

    -- Tracking: touches (season totals)
    touches          REAL,
    front_ct_touches REAL,
    time_of_poss     REAL,
    avg_sec_per_touch  REAL,
    avg_drib_per_touch REAL,
    elbow_touches    REAL,
    post_touches     REAL,
    paint_touches    REAL,

    -- Tracking: pull-up (season totals)
    pull_up_fga      REAL,
    pull_up_fgm      REAL,
    pull_up_fg_pct   REAL,
    pull_up_fg3a     REAL,
    pull_up_fg3_pct  REAL,
    pull_up_efg_pct  REAL,

    -- Tracking: catch & shoot (season totals)
    cs_fga           REAL,
    cs_fgm           REAL,
    cs_fg_pct        REAL,
    cs_fg3a          REAL,
    cs_fg3_pct       REAL,
    cs_efg_pct       REAL,

    -- Tracking: post-up (season totals)
    post_touch_fga   REAL,
    post_touch_fg_pct REAL,
    post_touch_pts   REAL,
    post_touch_ast   REAL,
    post_touch_tov   REAL,

    -- Tracking: speed/distance (season totals)
    dist_miles       REAL,
    dist_miles_off   REAL,
    dist_miles_def   REAL,
    avg_speed        REAL,
    avg_speed_off    REAL,
    avg_speed_def    REAL,

    -- Tracking: defense (season totals)
    def_rim_fga      REAL,
    def_rim_fgm      REAL,
    def_rim_fg_pct   REAL,

    -- Defender shooting (season)
    d_fga_overall    REAL,
    d_fg_pct_overall REAL,
    normal_fg_pct    REAL,
    d_fga_2pt        REAL,
    d_fg_pct_2pt     REAL,
    ns_fg2_pct       REAL,
    d_fga_3pt        REAL,
    d_fg_pct_3pt     REAL,
    ns_fg3_pct       REAL,

    -- Hustle (season totals)
    contested_shots  REAL,
    contested_2pt    REAL,
    contested_3pt    REAL,
    deflections      REAL,
    charges_drawn    REAL,
    screen_assists   REAL,
    screen_ast_pts   REAL,
    loose_balls      REAL,
    box_outs         REAL,
    off_box_outs     REAL,
    def_box_outs     REAL,

    -- Synergy offensive (season)
    iso_ppp          REAL,
    iso_fga          REAL,
    iso_efg_pct      REAL,
    iso_tov_pct      REAL,
    pnr_bh_ppp       REAL,
    pnr_bh_fga       REAL,
    pnr_roll_ppp     REAL,
    post_ppp         REAL,
    spotup_ppp       REAL,
    spotup_efg_pct   REAL,
    transition_ppp   REAL,
    transition_fga   REAL,

    -- Synergy defensive (season)
    def_iso_ppp      REAL,
    def_pnr_bh_ppp   REAL,

    -- Clutch (season)
    clutch_net_rating  REAL,
    clutch_ts_pct      REAL,
    clutch_usg_pct     REAL,
    clutch_min         REAL,

    -- On/Off (season, team-scoped)
    on_net_rating    REAL,
    off_net_rating   REAL,
    on_off_diff      REAL,

    -- External metrics (nullable)
    bpm              REAL,
    raptor_total     REAL,
    raptor_offense   REAL,
    raptor_defense   REAL,
    lebron           REAL,
    epm              REAL,
    darko            REAL,

    -- Experimental: NBA Inside the Game (2025-26+)
    itg_gravity_overall        REAL,
    itg_gravity_perim_onball   REAL,
    itg_gravity_perim_offball  REAL,
    itg_gravity_int_onball     REAL,
    itg_gravity_int_offball    REAL,
    itg_leverage_total         REAL,
    itg_leverage_offense       REAL,
    itg_leverage_defense       REAL,
    itg_shot_difficulty        REAL,

    updated_at       TIMESTAMP DEFAULT NOW(),

    UNIQUE(player_id, season, season_type, league)
);

-- ── Derived metrics & composites ──────────────────────────────
-- Computed from player_seasons — never fetched directly
-- Recomputed whenever player_seasons is updated
CREATE TABLE IF NOT EXISTS player_metrics (
    id               SERIAL PRIMARY KEY,
    player_id        INTEGER REFERENCES players(player_id) ON DELETE CASCADE,
    season           TEXT NOT NULL,
    season_type      TEXT NOT NULL,
    league           TEXT DEFAULT 'NBA',

    -- Shooting
    ts_pct_computed    REAL,
    ft_rate            REAL,
    shot_quality_delta REAL,
    creation_premium   REAL,
    paint_scoring_rate REAL,

    -- Playmaking
    potential_ast_per75   REAL,
    ast_conversion_rate   REAL,
    playmaking_gravity    REAL,
    secondary_ast_per75   REAL,
    pass_to_score_pct     REAL,
    ball_handler_load     REAL,
    drive_and_dish_rate   REAL,

    -- Defense
    def_delta_overall     REAL,
    def_delta_2pt         REAL,
    def_delta_3pt         REAL,
    rim_protection_score  REAL,
    def_disruption_rate   REAL,
    box_out_rate          REAL,

    -- Hustle
    screen_assist_rate    REAL,
    loose_ball_rate       REAL,
    hustle_composite      REAL,
    motor_score           REAL,

    -- Role
    creation_load         REAL,
    dribble_pressure_idx  REAL,
    cs_fga_rate           REAL,

    -- BPM computed
    bpm_computed          REAL,

    -- Composite scores (0–100, position-normalized z-score)
    playmaker_score       REAL,
    creator_score         REAL,
    defender_score        REAL,
    three_and_d_score     REAL,
    hustle_score          REAL,

    -- Percentile ranks within position group (0–100)
    ts_pct_pctile         REAL,
    usg_pct_pctile        REAL,
    ast_pct_pctile        REAL,
    net_rating_pctile     REAL,
    def_delta_pctile      REAL,
    rim_prot_pctile       REAL,
    playmaker_pctile      REAL,
    creator_pctile        REAL,
    defender_pctile       REAL,
    three_and_d_pctile    REAL,
    hustle_pctile         REAL,

    updated_at  TIMESTAMP DEFAULT NOW(),

    UNIQUE(player_id, season, season_type, league)
);

-- ── Shot zones ────────────────────────────────────────────────
-- One row per player per zone per season
CREATE TABLE IF NOT EXISTS player_shot_zones (
    id              SERIAL PRIMARY KEY,
    player_id       INTEGER REFERENCES players(player_id) ON DELETE CASCADE,
    season          TEXT NOT NULL,
    zone            TEXT NOT NULL,
    fga             INTEGER,
    fgm             INTEGER,
    fg_pct          REAL,
    league_fg_pct   REAL,
    updated_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(player_id, season, zone)
);

-- ── Indexes ───────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_player_seasons_player_season
    ON player_seasons(player_id, season, season_type);

CREATE INDEX IF NOT EXISTS idx_player_seasons_season
    ON player_seasons(season, season_type);

CREATE INDEX IF NOT EXISTS idx_player_metrics_player_season
    ON player_metrics(player_id, season, season_type);

CREATE INDEX IF NOT EXISTS idx_player_metrics_season
    ON player_metrics(season, season_type);

CREATE INDEX IF NOT EXISTS idx_player_metrics_composites
    ON player_metrics(season, season_type, playmaker_score, creator_score,
                      defender_score, three_and_d_score, hustle_score);

CREATE INDEX IF NOT EXISTS idx_players_position_group
    ON players(position_group);

CREATE INDEX IF NOT EXISTS idx_shot_zones_player_season
    ON player_shot_zones(player_id, season);
"""


def create_schema():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        print("Creating tables...")
        cur.execute(SCHEMA)

        # Verify tables were created
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = [row[0] for row in cur.fetchall()]

        print(f"\n✅ Schema created successfully. Tables:")
        for t in tables:
            print(f"   {t}")

        # Show column counts
        print(f"\nColumn counts:")
        for t in tables:
            cur.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = '{t}';
            """)
            col_count = cur.fetchone()[0]
            print(f"   {t:<25} {col_count} columns")

        cur.close()
        conn.close()
        print(f"\n✅ Done.")

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    create_schema()