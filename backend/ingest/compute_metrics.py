"""
The Impact Board — Metrics Computation v2
==========================================
python backend/ingest/compute_metrics.py
"""

import os
import sys
import math
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import numpy as np

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
SEASON       = os.getenv('NBA_SEASON',      '2024-25')
SEASON_TYPE  = os.getenv('NBA_SEASON_TYPE', 'Regular Season')

if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)

MIN_MINUTES_TOTAL = 1000
MIN_FGA_PER_GAME  = 2.0
MIN_PULL_UP_FGA   = 50
MIN_CS_FGA        = 50
MIN_DEF_FGA       = 150
MIN_RIM_FGA       = 20

LEAGUE_AVG_FT_PCT  = 0.778
RIM_XFG_BASELINE   = 0.650
POSSESSION_VALUE   = 1.15


def div(a, b, default=None):
    if a is None or b is None: return default
    if b == 0: return default
    return a / b

def safe(val, default=None):
    if val is None: return default
    try:
        if isinstance(val, float) and math.isnan(val): return default
        return float(val)
    except: return default

def s(val, default=0.0):
    return safe(val, default)


def load_seasons(conn, season, season_type):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT ps.*, p.position_group, p.position
        FROM player_seasons ps
        JOIN players p ON ps.player_id = p.player_id
        WHERE ps.season = %s AND ps.season_type = %s AND ps.league = 'NBA'
    """, (season, season_type))
    rows = cur.fetchall()
    cur.close()
    print(f"  Loaded {len(rows)} player season rows")
    return [dict(r) for r in rows]


def compute_player_metrics(p):
    pid       = p['player_id']
    min_total = s(p.get('min'))
    min_pg    = s(p.get('min_per_game'))
    gp        = s(p.get('gp'), 1)
    poss      = s(p.get('poss'))
    pos_group = p.get('position_group', 'F')

    poss_pg = poss if poss and poss > 0 else 75.0
    per75   = 75.0 / poss_pg if poss_pg > 0 else 1.0

    pts  = s(p.get('pts'))
    fga  = s(p.get('fga'))
    fta  = s(p.get('fta'))
    fgm  = s(p.get('fgm'))
    ftm  = s(p.get('ftm'))
    fg3m = s(p.get('fg3m'))
    fg3a = s(p.get('fg3a'))

    # ── Shooting ──────────────────────────────────────────────
    ts_denom        = 2 * (fga + 0.44 * fta)
    ts_pct_computed = div(pts, ts_denom)
    ft_rate         = div(fta, fga)

    efg_pct = s(p.get('efg_pct'))
    LEAGUE_AVG_EFG  = 0.535
    shot_quality_delta = (efg_pct - LEAGUE_AVG_EFG) if efg_pct > 0 else None

    pu_efg     = s(p.get('pull_up_efg_pct'))
    cs_efg     = s(p.get('cs_efg_pct'))
    pu_fga     = s(p.get('pull_up_fga'))
    cs_fga_tot = s(p.get('cs_fga'))

    creation_premium = None
    if pu_fga >= MIN_PULL_UP_FGA and cs_fga_tot >= MIN_CS_FGA:
        creation_premium = pu_efg - cs_efg

    pts_paint    = s(p.get('pts_paint'))
    paint_touches = s(p.get('paint_touches'))
    paint_scoring_rate = div(pts_paint, paint_touches)

    # ── Playmaking ────────────────────────────────────────────
    ast_pts_created = s(p.get('ast_pts_created'))
    potential_ast   = s(p.get('potential_ast'))
    secondary_ast   = s(p.get('secondary_ast'))
    passes_made     = s(p.get('passes_made'))
    ast_pg          = s(p.get('ast'))
    time_of_poss    = s(p.get('time_of_poss'))
    ft_ast          = s(p.get('ft_ast'))
    touches         = s(p.get('touches'))
    tov_pg          = s(p.get('tov'))
    drive_passes    = s(p.get('drive_passes'))
    drive_fga       = s(p.get('drive_fga'))
    drive_tov       = s(p.get('drive_tov'))

    # Normalize tracking totals to per-game first, then per-75
    potential_ast_pg    = potential_ast / gp if gp > 0 else 0
    secondary_ast_pg    = secondary_ast / gp if gp > 0 else 0
    passes_made_pg      = passes_made / gp if gp > 0 else 0
    ast_pts_created_pg  = ast_pts_created / gp if gp > 0 else 0
    ft_ast_pg           = ft_ast / gp if gp > 0 else 0
    drive_passes_pg     = (drive_passes / gp) if gp > 0 else drive_passes

    potential_ast_per75 = potential_ast_pg * per75 if potential_ast_pg > 0 else None
    ast_conversion_rate = div(ast_pg, potential_ast_pg)

    poss_used_passing  = div(time_of_poss, min_pg) if min_pg > 0 else None
    playmaking_gravity = div(ast_pts_created_pg, poss_used_passing) if poss_used_passing else None

    secondary_ast_per75 = secondary_ast_pg * per75 if secondary_ast_pg > 0 else None
    pass_to_score_pct   = div(ast_pg, passes_made_pg) if passes_made_pg > 0 else None
    ball_handler_load   = div(time_of_poss, min_pg)

    drive_total         = drive_fga + drive_tov + drive_passes_pg
    drive_and_dish_rate = div(drive_passes_pg, drive_total)

    # Potential AST / TOV ratio — both per game for consistent units
    # potential_ast is season total; divide by GP to get per-game
    potential_ast_pg = potential_ast / gp if gp > 0 else None
    pot_ast_per_tov  = div(potential_ast_pg, tov_pg) if tov_pg and tov_pg > 0 else None

    # Pass quality index: ast_pts_created per game / passes_made per game
    ast_pts_created_pg = ast_pts_created / gp if gp > 0 else None
    passes_made_pg     = passes_made / gp if gp > 0 else None
    pass_quality_index = div(ast_pts_created_pg, passes_made_pg)

    # ── Defense ───────────────────────────────────────────────
    d_fg_pct_overall = s(p.get('d_fg_pct_overall'))
    normal_fg_pct    = s(p.get('normal_fg_pct'))
    d_fga_overall    = s(p.get('d_fga_overall'))
    d_fg_pct_2pt     = s(p.get('d_fg_pct_2pt'))
    ns_fg2_pct       = s(p.get('ns_fg2_pct'))
    d_fga_2pt        = s(p.get('d_fga_2pt'))
    d_fg_pct_3pt     = s(p.get('d_fg_pct_3pt'))
    ns_fg3_pct       = s(p.get('ns_fg3_pct'))
    d_fga_3pt        = s(p.get('d_fga_3pt'))

    def_delta_overall = None
    def_delta_2pt     = None
    def_delta_3pt     = None

    if d_fga_overall >= MIN_DEF_FGA and normal_fg_pct > 0:
        def_delta_overall = normal_fg_pct - d_fg_pct_overall
    if d_fga_2pt >= MIN_DEF_FGA * 0.6 and ns_fg2_pct > 0:
        def_delta_2pt = ns_fg2_pct - d_fg_pct_2pt
    if d_fga_3pt >= MIN_DEF_FGA * 0.4 and ns_fg3_pct > 0:
        def_delta_3pt = ns_fg3_pct - d_fg_pct_3pt

    def_rim_fga    = s(p.get('def_rim_fga'))
    def_rim_fg_pct = s(p.get('def_rim_fg_pct'))
    rim_protection_score = None
    if def_rim_fga >= MIN_RIM_FGA:
        rim_protection_score = (RIM_XFG_BASELINE - def_rim_fg_pct) * def_rim_fga * 2

    stl         = s(p.get('stl'))   # already per-game from game log avg
    deflections = s(p.get('deflections')) / gp if gp > 0 else 0
    charges     = s(p.get('charges_drawn')) / gp if gp > 0 else 0
    def_disruption_rate = None
    if (stl + deflections + charges) > 0:
        def_disruption_rate = (stl + deflections * 0.35 + charges * 0.80) * per75

    box_outs = s(p.get('box_outs')) / gp if gp > 0 else 0
    box_out_rate = box_outs * per75 if box_outs > 0 else None

    # ── Hustle ────────────────────────────────────────────────
    screen_ast_pts  = s(p.get('screen_ast_pts')) / gp if gp > 0 else 0
    loose_balls     = s(p.get('loose_balls'))     / gp if gp > 0 else 0
    dist_miles_off  = s(p.get('dist_miles_off'))
    dist_miles_def  = s(p.get('dist_miles_def'))
    deflections_pg  = s(p.get('deflections'))     / gp if gp > 0 else 0
    charges_pg      = s(p.get('charges_drawn'))   / gp if gp > 0 else 0
    box_outs_pg     = s(p.get('box_outs'))        / gp if gp > 0 else 0

    screen_assist_rate = screen_ast_pts * per75 if screen_ast_pts > 0 else None
    loose_ball_rate    = loose_balls    * per75 if loose_balls    > 0 else None
    box_out_rate       = box_outs_pg    * per75 if box_outs_pg    > 0 else None

    hustle_raw = (
        deflections_pg * 0.35 + charges_pg * 0.80 +
        loose_balls    * 0.65 + screen_ast_pts * 0.10
    )
    hustle_composite = hustle_raw * per75 if hustle_raw > 0 else None

    motor_score = None
    if min_total >= MIN_MINUTES_TOTAL and (dist_miles_off + dist_miles_def) > 0:
        dist_per_game = dist_miles_off + dist_miles_def
        motor_score = (dist_per_game / min_pg) * 36 if min_pg > 0 else None

    # ── Role ──────────────────────────────────────────────────
    drives        = s(p.get('drives'))
    post_touches  = s(p.get('post_touches'))
    elbow_touches = s(p.get('elbow_touches'))

    creation_load       = div(drives + post_touches + elbow_touches, touches)
    avg_drib_per_touch  = s(p.get('avg_drib_per_touch'))
    dribble_pressure_idx = avg_drib_per_touch if avg_drib_per_touch > 0 else None
    cs_fga_rate         = div(cs_fga_tot, fga) if fga >= MIN_FGA_PER_GAME * gp else None

    # ── Scoring by zone (from shot locations stored in player_seasons) ─
    # We store these in player_shot_zones table, not player_seasons
    # These are exposed via the /api/players/:id endpoint directly
    # Here we compute a simple mid-range vs rim split from available data
    pct_pts_paint = s(p.get('pct_pts_paint'))
    pct_pts_3pt   = s(p.get('pct_pts_3pt'))
    pct_pts_mid2  = s(p.get('pct_pts_mid2'))
    pct_pts_ft    = s(p.get('pct_pts_ft'))

    # ── BPM ───────────────────────────────────────────────────
    ast_pct  = s(p.get('ast_pct'))
    reb_pct  = s(p.get('reb_pct'))
    stl_pct  = stl / poss_pg if poss_pg > 0 else 0
    blk      = s(p.get('blk'))
    blk_pct  = blk / poss_pg if poss_pg > 0 else 0
    usg_pct  = s(p.get('usg_pct'))
    ts_pct   = s(p.get('ts_pct'))

    pos_adj = {'G': 0.0, 'GF': -0.5, 'F': -1.0, 'FC': -1.5, 'C': -2.0}.get(pos_group, -1.0)

    bpm_computed = None
    if min_total >= MIN_MINUTES_TOTAL and usg_pct > 0 and ts_pct > 0:
        bpm_computed = (
            -2.611
            + 0.318 * ast_pct * 100
            + 0.449 * reb_pct * 100 * 0.3
            + 1.064 * (ts_pct - 0.500) * usg_pct * 100 / 25
            + 0.295 * stl_pct * 100
            + 0.396 * blk_pct * 100
            - 0.257 * (tov_pg / (poss_pg + 0.01)) * 100
            + pos_adj
        )

    def r(v, decimals=4):
        return round(v, decimals) if v is not None else None

    return {
        'player_id':   pid,
        'season':      p['season'],
        'season_type': p['season_type'],
        'league':      p.get('league', 'NBA'),

        'ts_pct_computed':    r(ts_pct_computed),
        'ft_rate':            r(ft_rate),
        'shot_quality_delta': r(shot_quality_delta),
        'creation_premium':   r(creation_premium),
        'paint_scoring_rate': r(paint_scoring_rate),

        'potential_ast_per75':  r(potential_ast_per75, 3),
        'ast_conversion_rate':  r(ast_conversion_rate),
        'playmaking_gravity':   r(playmaking_gravity),
        'secondary_ast_per75':  r(secondary_ast_per75, 3),
        'pass_to_score_pct':    r(pass_to_score_pct),
        'ball_handler_load':    r(ball_handler_load),
        'drive_and_dish_rate':  r(drive_and_dish_rate),
        'pot_ast_per_tov':      r(pot_ast_per_tov, 3),
        'pass_quality_index':   r(pass_quality_index, 4),

        'def_delta_overall':    r(def_delta_overall),
        'def_delta_2pt':        r(def_delta_2pt),
        'def_delta_3pt':        r(def_delta_3pt),
        'rim_protection_score': r(rim_protection_score, 3),
        'def_disruption_rate':  r(def_disruption_rate, 3),
        'box_out_rate':         r(box_out_rate, 3),

        'screen_assist_rate':   r(screen_assist_rate, 3),
        'loose_ball_rate':      r(loose_ball_rate, 3),
        'hustle_composite':     r(hustle_composite, 3),
        'motor_score':          r(motor_score, 3),

        'creation_load':         r(creation_load),
        'dribble_pressure_idx':  r(dribble_pressure_idx, 3),
        'cs_fga_rate':           r(cs_fga_rate),
        'bpm_computed':          r(bpm_computed, 2),
    }


def compute_composites(metrics_list, seasons_map):
    pos_groups = {}
    for m in metrics_list:
        pid    = m['player_id']
        ps     = seasons_map.get(pid, {})
        pos_g  = ps.get('position_group', 'F')
        min_t  = s(ps.get('min'), 0)
        if min_t < MIN_MINUTES_TOTAL:
            continue
        pos_groups.setdefault(pos_g, []).append(pid)

    metrics_by_pid = {m['player_id']: m for m in metrics_list}

    def get_vals(pids, col, source='metrics'):
        vals = []
        for pid in pids:
            if source == 'metrics':
                v = safe(metrics_by_pid.get(pid, {}).get(col))
            else:
                v = safe(seasons_map.get(pid, {}).get(col))
            if v is not None:
                vals.append((pid, v))
        return vals

    def z_scores(pid_vals):
        if not pid_vals: return {}
        values = [v for _, v in pid_vals]
        mean   = np.mean(values)
        std    = np.std(values)
        if std == 0: return {pid: 0.0 for pid, _ in pid_vals}
        return {pid: (v - mean) / std for pid, v in pid_vals}

    def to_0_100(z):
        return max(0.0, min(100.0, 50.0 + z * 10.0))

    columns_needed = [
        ('ast_pct',             'seasons'),
        ('playmaking_gravity',  'metrics'),
        ('potential_ast_per75', 'metrics'),
        ('ast_conversion_rate', 'metrics'),
        ('pass_to_score_pct',   'metrics'),
        ('pct_uast_fgm',        'seasons'),
        ('pull_up_efg_pct',     'seasons'),
        ('creation_load',       'metrics'),
        ('iso_ppp',             'seasons'),
        ('drive_fg_pct',        'seasons'),
        ('def_delta_overall',   'metrics'),
        ('def_disruption_rate', 'metrics'),
        ('rim_protection_score','metrics'),
        ('dreb_pct',            'seasons'),
        ('box_out_rate',        'metrics'),
        ('def_delta_3pt',       'metrics'),
        ('cs_efg_pct',          'seasons'),
        ('cs_fga_rate',         'metrics'),
        ('contested_shots',     'seasons'),
        ('motor_score',         'metrics'),
        ('hustle_composite',    'metrics'),
        ('loose_ball_rate',     'metrics'),
        ('screen_assist_rate',  'metrics'),
    ]

    all_z = {}
    for col, src in columns_needed:
        all_z[col] = {}
        for pos_g, pids in pos_groups.items():
            pid_vals = get_vals(pids, col, source=src)
            z_map    = z_scores(pid_vals)
            all_z[col].update(z_map)

    composites = {
        'playmaker_score': (
            [('ast_pct','s'), ('playmaking_gravity','m'),
             ('potential_ast_per75','m'), ('ast_conversion_rate','m'),
             ('pass_to_score_pct','m')],
            [0.25, 0.25, 0.20, 0.15, 0.15]
        ),
        'creator_score': (
            [('pct_uast_fgm','s'), ('pull_up_efg_pct','s'),
             ('creation_load','m'), ('iso_ppp','s'), ('drive_fg_pct','s')],
            [0.25, 0.25, 0.20, 0.15, 0.15]
        ),
        'defender_score': (
            [('def_delta_overall','m'), ('def_disruption_rate','m'),
             ('rim_protection_score','m'), ('dreb_pct','s'), ('box_out_rate','m')],
            [0.30, 0.25, 0.20, 0.15, 0.10]
        ),
        'three_and_d_score': (
            [('def_delta_3pt','m'), ('cs_efg_pct','s'),
             ('cs_fga_rate','m'), ('def_disruption_rate','m'), ('contested_shots','s')],
            [0.30, 0.30, 0.15, 0.15, 0.10]
        ),
        'hustle_score': (
            [('motor_score','m'), ('hustle_composite','m'),
             ('loose_ball_rate','m'), ('box_out_rate','m'), ('screen_assist_rate','m')],
            [0.30, 0.25, 0.20, 0.15, 0.10]
        ),
    }

    for m in metrics_list:
        pid    = m['player_id']
        ps     = seasons_map.get(pid, {})
        min_t  = s(ps.get('min'), 0)

        if min_t < MIN_MINUTES_TOTAL:
            for comp in composites:
                m[comp] = None
            continue

        for comp_name, (components, weights) in composites.items():
            z_total = 0.0
            w_total = 0.0
            for (col, src), w in zip(components, weights):
                z = all_z.get(col, {}).get(pid)
                if z is not None:
                    z_total += z * w
                    w_total += w
            m[comp_name] = round(to_0_100(z_total / w_total), 1) if w_total > 0 else None

    # Percentiles (league-wide)
    pctile_cols = [
        ('ts_pct',      'ts_pct',              'seasons'),
        ('usg_pct',     'usg_pct',             'seasons'),
        ('ast_pct',     'ast_pct',             'seasons'),
        ('net_rating',  'net_rating',          'seasons'),
        ('def_delta',   'def_delta_overall',   'metrics'),
        ('rim_prot',    'rim_protection_score','metrics'),
        ('playmaker',   'playmaker_score',     'metrics'),
        ('creator',     'creator_score',       'metrics'),
        ('defender',    'defender_score',      'metrics'),
        ('three_and_d', 'three_and_d_score',   'metrics'),
        ('hustle',      'hustle_score',        'metrics'),
    ]

    for pctile_name, col, src in pctile_cols:
        all_vals = []
        for m in metrics_list:
            pid   = m['player_id']
            ps    = seasons_map.get(pid, {})
            min_t = s(ps.get('min'), 0)
            if min_t < MIN_MINUTES_TOTAL: continue
            v = safe(m.get(col)) if src == 'metrics' else safe(ps.get(col))
            if v is not None:
                all_vals.append((pid, v))
        if not all_vals: continue
        sorted_vals = sorted(all_vals, key=lambda x: x[1])
        n        = len(sorted_vals)
        rank_map = {pid: i for i, (pid, _) in enumerate(sorted_vals)}
        for m in metrics_list:
            pid = m['player_id']
            if pid in rank_map:
                m[f'{pctile_name}_pctile'] = round((rank_map[pid] / n) * 100, 1)
            else:
                m[f'{pctile_name}_pctile'] = None

    return metrics_list


def upsert_metrics(conn, rows):
    if not rows: return
    cols = [k for k in rows[0].keys()]
    cur  = conn.cursor()

    col_str     = ', '.join(cols)
    placeholder = ', '.join(['%s'] * len(cols))
    update_str  = ', '.join([f"{c} = EXCLUDED.{c}" for c in cols
                              if c not in ('player_id', 'season', 'season_type', 'league')])
    sql = f"""
        INSERT INTO player_metrics ({col_str}, updated_at)
        VALUES ({placeholder}, NOW())
        ON CONFLICT (player_id, season, season_type, league) DO UPDATE SET
            {update_str}, updated_at = NOW()
    """

    def clean(val):
        if val is None: return None
        if isinstance(val, (np.floating, np.integer)): return val.item()
        if isinstance(val, float) and math.isnan(val): return None
        return val

    values = [tuple(clean(r.get(c)) for c in cols) for r in rows]
    cur.executemany(sql, values)
    conn.commit()
    cur.close()
    print(f"  ✅ Upserted {len(rows)} metric rows")


def spot_check(conn, season, season_type):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print(f"\n{'='*90}")
    print(f"Top 15 Playmakers — {season}")
    print(f"{'='*90}")
    cur.execute("""
        SELECT p.player_name, p.position_group, ps.min, ps.pts, ps.ast,
               ps.net_rating, pm.bpm_computed, pm.playmaker_score,
               pm.creator_score, pm.defender_score, pm.hustle_score,
               pm.pot_ast_per_tov, pm.pass_quality_index
        FROM player_metrics pm
        JOIN player_seasons ps ON pm.player_id = ps.player_id
            AND pm.season = ps.season AND pm.season_type = ps.season_type
        JOIN players p ON pm.player_id = p.player_id
        WHERE pm.season = %s AND pm.season_type = %s AND ps.min >= %s
        ORDER BY pm.playmaker_score DESC NULLS LAST LIMIT 15
    """, (season, season_type, MIN_MINUTES_TOTAL))

    print(f"{'Player':<22} {'Pos':<4} {'MIN':>5} {'PTS':>5} {'AST':>5} "
          f"{'NET':>6} {'BPM':>5} {'PLYMK':>6} {'CREAT':>6} "
          f"{'POT/TOV':>8} {'PASS_Q':>7}")
    print("─" * 90)
    for r in cur.fetchall():
        print(f"{r['player_name']:<22} {r['position_group'] or '':<4} "
              f"{int(r['min'] or 0):>5} {r['pts'] or 0:>5.1f} {r['ast'] or 0:>5.1f} "
              f"{r['net_rating'] or 0:>6.1f} {r['bpm_computed'] or 0:>5.1f} "
              f"{r['playmaker_score'] or 0:>6.1f} {r['creator_score'] or 0:>6.1f} "
              f"{r['pot_ast_per_tov'] or 0:>8.2f} {r['pass_quality_index'] or 0:>7.3f}")

    print(f"\nTop 10 Defenders:")
    print("─" * 70)
    cur.execute("""
        SELECT p.player_name, p.position_group, ps.stl, ps.blk,
               pm.defender_score, pm.def_delta_overall,
               pm.def_delta_3pt, pm.rim_protection_score, pm.def_disruption_rate
        FROM player_metrics pm
        JOIN player_seasons ps ON pm.player_id = ps.player_id
            AND pm.season = ps.season AND pm.season_type = ps.season_type
        JOIN players p ON pm.player_id = p.player_id
        WHERE pm.season = %s AND pm.season_type = %s AND ps.min >= %s
        ORDER BY pm.defender_score DESC NULLS LAST LIMIT 10
    """, (season, season_type, MIN_MINUTES_TOTAL))
    for r in cur.fetchall():
        print(f"  {r['player_name']:<22} {r['position_group'] or '':<4} "
              f"STL={r['stl'] or 0:.1f}  BLK={r['blk'] or 0:.1f}  "
              f"DEF={r['defender_score'] or 0:.1f}  "
              f"Δ={r['def_delta_overall'] or 0:+.3f}  "
              f"RIM={r['rim_protection_score'] or 0:.1f}")

    cur.close()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--season',      default=SEASON)
    parser.add_argument('--season-type', default=SEASON_TYPE)
    args = parser.parse_args()

    season      = args.season
    season_type = args.season_type

    print(f"\nThe Impact Board — Metrics Computation")
    print(f"Season: {season} | Type: {season_type}")
    print(f"Min minutes for composites: {MIN_MINUTES_TOTAL}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    conn = psycopg2.connect(DATABASE_URL)
    print("Loading season data...")
    seasons     = load_seasons(conn, season, season_type)
    seasons_map = {s['player_id']: s for s in seasons}

    print("Computing derived metrics...")
    metrics_list = [compute_player_metrics(ps) for ps in seasons]
    print(f"  {len(metrics_list)} players")

    print("Computing composites and percentiles...")
    metrics_list = compute_composites(metrics_list, seasons_map)
    qualifying   = sum(1 for m in metrics_list if m.get('playmaker_score') is not None)
    print(f"  {qualifying} players qualify (>={MIN_MINUTES_TOTAL} min)")

    print("\nWriting to database...")
    upsert_metrics(conn, metrics_list)
    spot_check(conn, season, season_type)
    conn.close()

    print(f"\n{'='*60}")
    print(f"✅ Done — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()