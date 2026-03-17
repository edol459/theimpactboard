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

ZONE_MIN_FGA_PG = {
    'paint':        2.0,
    'midrange':     1.0,
    'corner3':      0.5,
    'above_break3': 1.5,
}


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
    fg3m = s(p.get('fg3m'))
    fg3a = s(p.get('fg3a'))

    # ── Shooting ──────────────────────────────────────────────
    ts_denom           = 2 * (fga + 0.44 * fta)
    ts_pct_computed    = div(pts, ts_denom)
    ft_rate            = div(fta, fga)

    efg_pct            = s(p.get('efg_pct'))
    shot_quality_delta = (efg_pct - 0.535) if efg_pct > 0 else None

    pu_efg     = s(p.get('pull_up_efg_pct'))
    cs_efg     = s(p.get('cs_efg_pct'))
    pu_fga     = s(p.get('pull_up_fga'))
    cs_fga_tot = s(p.get('cs_fga'))

    creation_premium = None
    if pu_fga >= MIN_PULL_UP_FGA and cs_fga_tot >= MIN_CS_FGA:
        creation_premium = pu_efg - cs_efg

    pts_paint          = s(p.get('pts_paint'))
    paint_touches      = s(p.get('paint_touches'))
    paint_scoring_rate = div(pts_paint, paint_touches)

    # ── Playmaking ────────────────────────────────────────────
    ast_pts_created = s(p.get('ast_pts_created'))
    potential_ast   = s(p.get('potential_ast'))
    secondary_ast   = s(p.get('secondary_ast'))
    passes_made     = s(p.get('passes_made'))
    ast_pg          = s(p.get('ast'))
    time_of_poss    = s(p.get('time_of_poss'))
    touches         = s(p.get('touches'))
    tov_pg          = s(p.get('tov'))
    drive_passes    = s(p.get('drive_passes'))
    drive_fga       = s(p.get('drive_fga'))
    drive_tov       = s(p.get('drive_tov'))

    potential_ast_pg   = potential_ast   / gp if gp > 0 else 0
    secondary_ast_pg   = secondary_ast   / gp if gp > 0 else 0
    passes_made_pg     = passes_made     / gp if gp > 0 else 0
    ast_pts_created_pg = ast_pts_created / gp if gp > 0 else 0
    drive_passes_pg    = drive_passes    / gp if gp > 0 else drive_passes

    potential_ast_per75 = potential_ast_pg * per75 if potential_ast_pg > 0 else None
    ast_conversion_rate = div(ast_pg, potential_ast_pg)
    poss_used_passing   = div(time_of_poss, min_pg) if min_pg > 0 else None
    playmaking_gravity  = div(ast_pts_created_pg, poss_used_passing) if poss_used_passing else None
    secondary_ast_per75 = secondary_ast_pg * per75 if secondary_ast_pg > 0 else None
    pass_to_score_pct   = div(ast_pg, passes_made_pg) if passes_made_pg > 0 else None
    ball_handler_load   = div(time_of_poss, min_pg)
    drive_total         = drive_fga + drive_tov + drive_passes_pg
    drive_and_dish_rate = div(drive_passes_pg, drive_total)
    pass_quality_index  = div(ast_pts_created_pg, passes_made_pg)

    # Potential AST / Bad Pass TOV (falls back to total TOV)
    bad_pass_total = safe(p.get('bad_pass_tov'))
    if bad_pass_total is not None and bad_pass_total > 0 and gp > 0:
        pot_ast_per_tov = div(potential_ast_pg, bad_pass_total / gp)
    else:
        pot_ast_per_tov = div(potential_ast_pg, tov_pg) if tov_pg and tov_pg > 0 else None

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

    def_delta_overall = (normal_fg_pct - d_fg_pct_overall) if d_fga_overall >= MIN_DEF_FGA and normal_fg_pct > 0 else None
    def_delta_2pt     = (ns_fg2_pct - d_fg_pct_2pt)       if d_fga_2pt >= MIN_DEF_FGA * 0.6 and ns_fg2_pct > 0 else None
    def_delta_3pt     = (ns_fg3_pct - d_fg_pct_3pt)       if d_fga_3pt >= MIN_DEF_FGA * 0.4 and ns_fg3_pct > 0 else None

    def_rim_fga    = s(p.get('def_rim_fga'))
    def_rim_fg_pct = s(p.get('def_rim_fg_pct'))
    rim_protection_score = (RIM_XFG_BASELINE - def_rim_fg_pct) * def_rim_fga * 2 if def_rim_fga >= MIN_RIM_FGA else None

    stl         = s(p.get('stl'))
    deflections = s(p.get('deflections')) / gp if gp > 0 else 0
    charges     = s(p.get('charges_drawn')) / gp if gp > 0 else 0
    def_disruption_rate = (stl + deflections * 0.35 + charges * 0.80) * per75 if (stl + deflections + charges) > 0 else None

    box_outs_pg  = s(p.get('box_outs')) / gp if gp > 0 else 0
    box_out_rate = box_outs_pg * per75 if box_outs_pg > 0 else None

    # ── Hustle ────────────────────────────────────────────────
    screen_ast_pts = s(p.get('screen_ast_pts')) / gp if gp > 0 else 0
    loose_balls    = s(p.get('loose_balls'))    / gp if gp > 0 else 0
    dist_miles_off = s(p.get('dist_miles_off'))
    dist_miles_def = s(p.get('dist_miles_def'))
    deflections_pg = s(p.get('deflections'))    / gp if gp > 0 else 0
    charges_pg     = s(p.get('charges_drawn'))  / gp if gp > 0 else 0

    screen_assist_rate = screen_ast_pts * per75 if screen_ast_pts > 0 else None
    loose_ball_rate    = loose_balls    * per75 if loose_balls    > 0 else None
    box_out_rate       = box_outs_pg    * per75 if box_outs_pg    > 0 else None

    hustle_raw       = deflections_pg * 0.35 + charges_pg * 0.80 + loose_balls * 0.65 + screen_ast_pts * 0.10
    hustle_composite = hustle_raw * per75 if hustle_raw > 0 else None

    motor_score = None
    if min_total >= MIN_MINUTES_TOTAL and (dist_miles_off + dist_miles_def) > 0 and min_pg > 0:
        motor_score = ((dist_miles_off + dist_miles_def) / min_pg) * 36

    # ── Role ──────────────────────────────────────────────────
    drives        = s(p.get('drives'))
    post_touches  = s(p.get('post_touches'))
    elbow_touches = s(p.get('elbow_touches'))

    creation_load        = div(drives + post_touches + elbow_touches, touches)
    avg_drib_per_touch   = s(p.get('avg_drib_per_touch'))
    dribble_pressure_idx = avg_drib_per_touch if avg_drib_per_touch > 0 else None
    cs_fga_rate          = div(cs_fga_tot, fga) if fga >= MIN_FGA_PER_GAME * gp else None

    # ── BPM ───────────────────────────────────────────────────
    ast_pct  = s(p.get('ast_pct'))
    reb_pct  = s(p.get('reb_pct'))
    stl_pct  = stl / poss_pg if poss_pg > 0 else 0
    blk      = s(p.get('blk'))
    blk_pct  = blk / poss_pg if poss_pg > 0 else 0
    usg_pct  = s(p.get('usg_pct'))
    ts_pct   = s(p.get('ts_pct'))
    pos_adj  = {'G': 0.0, 'GF': -0.5, 'F': -1.0, 'FC': -1.5, 'C': -2.0}.get(pos_group, -1.0)

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

    def r(v, d=4):
        return round(v, d) if v is not None else None

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
        'creation_load':        r(creation_load),
        'dribble_pressure_idx': r(dribble_pressure_idx, 3),
        'cs_fga_rate':          r(cs_fga_rate),
        'bpm_computed':         r(bpm_computed, 2),
    }


def compute_composites(metrics_list, seasons_map):
    pos_groups = {}
    for m in metrics_list:
        pid   = m['player_id']
        ps    = seasons_map.get(pid, {})
        pos_g = ps.get('position_group', 'F')
        min_t = s(ps.get('min'), 0)
        if min_t < MIN_MINUTES_TOTAL: continue
        pos_groups.setdefault(pos_g, []).append(pid)

    # All qualifying player IDs (league-wide, for non-position-normalized composites)
    all_qualifying = [pid for pids in pos_groups.values() for pid in pids]

    metrics_by_pid = {m['player_id']: m for m in metrics_list}

    def get_vals(pids, col, source='metrics'):
        vals = []
        for pid in pids:
            v = safe(metrics_by_pid.get(pid, {}).get(col)) if source == 'metrics' else safe(seasons_map.get(pid, {}).get(col))
            if v is not None: vals.append((pid, v))
        return vals

    def z_scores(pid_vals):
        if not pid_vals: return {}
        values = [v for _, v in pid_vals]
        mean, std = np.mean(values), np.std(values)
        if std == 0: return {pid: 0.0 for pid, _ in pid_vals}
        return {pid: (v - mean) / std for pid, v in pid_vals}

    def to_0_100(z):
        return max(0.0, min(100.0, 50.0 + z * 10.0))

    # ── All columns needed ────────────────────────────────────
    columns_needed = [
        # Main composites
        ('ast_pct','seasons'), ('playmaking_gravity','metrics'),
        ('potential_ast_per75','metrics'), ('ast_conversion_rate','metrics'),
        ('pass_to_score_pct','metrics'), ('pct_uast_fgm','seasons'),
        ('pull_up_efg_pct','seasons'), ('creation_load','metrics'),
        ('iso_ppp','seasons'), ('drive_fg_pct','seasons'),
        ('def_delta_overall','metrics'), ('def_disruption_rate','metrics'),
        ('rim_protection_score','metrics'), ('dreb_pct','seasons'),
        ('box_out_rate','metrics'), ('def_delta_3pt','metrics'),
        ('cs_efg_pct','seasons'), ('cs_fga_rate','metrics'),
        ('contested_shots','seasons'), ('motor_score','metrics'),
        ('hustle_composite','metrics'), ('loose_ball_rate','metrics'),
        ('screen_assist_rate','metrics'),
        # Sub-composite additional columns
        ('ts_pct','seasons'), ('shot_quality_delta','metrics'),
        ('paint_scoring_rate','metrics'), ('ft_rate','metrics'),
        ('paint_efg_vw','metrics'), ('creation_premium','metrics'),
        ('pass_quality_index','metrics'), ('secondary_ast_per75','metrics'),
        ('drive_and_dish_rate','metrics'), ('pnr_bh_ppp','seasons'),
        ('transition_ppp','seasons'), ('pot_ast_per_tov','metrics'),
        ('def_delta_2pt','metrics'), ('blk','seasons'), ('stl','seasons'),
        ('def_iso_ppp','seasons'), ('def_pnr_bh_ppp','seasons'),
        ('all3_efg_vw','metrics'),
    ]

    # Position-normalized z-scores (for defense/hustle)
    all_z_pos = {}
    for col, src in columns_needed:
        all_z_pos[col] = {}
        for pos_g, pids in pos_groups.items():
            all_z_pos[col].update(z_scores(get_vals(pids, col, source=src)))

    # League-wide z-scores (for scoring/playmaking sub-composites)
    all_z_lg = {}
    for col, src in columns_needed:
        all_z_lg[col] = z_scores(get_vals(all_qualifying, col, source=src))

    # ── Main composites (position-normalized) ─────────────────
    main_composites = {
        'playmaker_score': (
            [('ast_pct','s'),('playmaking_gravity','m'),('potential_ast_per75','m'),('ast_conversion_rate','m'),('pass_to_score_pct','m')],
            [0.25,0.25,0.20,0.15,0.15], 'pos'
        ),
        'creator_score': (
            [('pct_uast_fgm','s'),('pull_up_efg_pct','s'),('creation_load','m'),('iso_ppp','s'),('drive_fg_pct','s')],
            [0.25,0.25,0.20,0.15,0.15], 'pos'
        ),
        'defender_score': (
            [('def_delta_overall','m'),('def_disruption_rate','m'),('rim_protection_score','m'),('dreb_pct','s'),('box_out_rate','m')],
            [0.30,0.25,0.20,0.15,0.10], 'pos'
        ),
        'three_and_d_score': (
            [('def_delta_3pt','m'),('cs_efg_pct','s'),('cs_fga_rate','m'),('def_disruption_rate','m'),('contested_shots','s')],
            [0.30,0.30,0.15,0.15,0.10], 'pos'
        ),
        'hustle_score': (
            [('motor_score','m'),('hustle_composite','m'),('loose_ball_rate','m'),('box_out_rate','m'),('screen_assist_rate','m')],
            [0.30,0.25,0.20,0.15,0.10], 'pos'
        ),
    }

    # ── Sub-category composites (with volume gates) ───────────────
    # Default thresholds — players who don't meet these get NULL
    # These same thresholds are applied dynamically in the API
    # when users adjust them via the Filters drawer.
    SUB_GATES = {
        'finishing':     {'paint_fga_pg': 2.0},
        'shooting':      {},   # gated via TS% min attempts (already in box score)
        'creation':      {'drives_pg': 2.0},
        'passing':       {'ast_pg': 2.0},
        'ballhandling':  {'touches_pg': 40.0, 'pos_groups': {'G','GF'}},
        'perimeter_def': {},
        'interior_def':  {'def_rim_fga': 50},
        'activity':      {},
        'rebounding':    {},
    }

    sub_composites = {
        'finishing_score': (
            [('paint_efg_vw','m'),('paint_scoring_rate','m'),('ft_rate','m')],
            [0.50, 0.30, 0.20], 'lg', 'finishing'
        ),
        'shooting_score': (
            [('ts_pct','s'),('shot_quality_delta','m'),
             ('pull_up_efg_pct','s'),('cs_efg_pct','s'),('all3_efg_vw','m')],
            [0.25, 0.20, 0.20, 0.15, 0.20], 'lg', 'shooting'
        ),
        'creation_score': (
            [('pct_uast_fgm','s'),('creation_load','m'),('iso_ppp','s'),('creation_premium','m')],
            [0.30, 0.25, 0.25, 0.20], 'lg', 'creation'
        ),
        'passing_score': (
            [('pot_ast_per_tov','m'),('pass_quality_index','m'),
             ('ast_conversion_rate','m'),('pass_to_score_pct','m')],
            [0.40, 0.25, 0.20, 0.15], 'lg', 'passing'
        ),
        'ballhandling_score': (
            [('playmaking_gravity','m'),('drive_and_dish_rate','m'),
             ('pnr_bh_ppp','s'),('transition_ppp','s')],
            [0.30, 0.25, 0.25, 0.20], 'lg', 'ballhandling'
        ),
        'perimeter_def_score': (
            [('def_delta_3pt','m'),('def_disruption_rate','m'),
             ('stl','s'),('def_delta_overall','m'),('def_iso_ppp','s')],
            [0.30, 0.25, 0.20, 0.15, 0.10], 'pos', 'perimeter_def'
        ),
        'interior_def_score': (
            [('rim_protection_score','m'),('def_delta_2pt','m'),
             ('dreb_pct','s'),('blk','s'),('box_out_rate','m')],
            [0.40, 0.25, 0.20, 0.10, 0.05], 'pos', 'interior_def'
        ),
        'activity_score': (
            [('motor_score','m'),('hustle_composite','m'),('def_disruption_rate','m')],
            [0.40, 0.35, 0.25], 'pos', 'activity'
        ),
        'rebounding_score': (
            [('dreb_pct','s'),('box_out_rate','m'),('loose_ball_rate','m')],
            [0.50, 0.30, 0.20], 'pos', 'rebounding'
        ),
    }

    # Add sub-composite columns needed for z-score computation
    sub_cols_needed = [
        ('pot_ast_per_tov','metrics'), ('pass_quality_index','metrics'),
        ('ast_conversion_rate','metrics'), ('pass_to_score_pct','metrics'),
        ('playmaking_gravity','metrics'), ('drive_and_dish_rate','metrics'),
        ('pnr_bh_ppp','seasons'), ('transition_ppp','seasons'),
        ('ts_pct','seasons'), ('shot_quality_delta','metrics'),
        ('pull_up_efg_pct','seasons'), ('cs_efg_pct','seasons'),
        ('all3_efg_vw','metrics'), ('paint_efg_vw','metrics'),
        ('paint_scoring_rate','metrics'), ('ft_rate','metrics'),
        ('pct_uast_fgm','seasons'), ('creation_load','metrics'),
        ('iso_ppp','seasons'), ('creation_premium','metrics'),
        ('def_delta_3pt','metrics'), ('def_disruption_rate','metrics'),
        ('stl','seasons'), ('def_delta_overall','metrics'),
        ('def_iso_ppp','seasons'), ('rim_protection_score','metrics'),
        ('def_delta_2pt','metrics'), ('dreb_pct','seasons'),
        ('blk','seasons'), ('box_out_rate','metrics'),
        ('motor_score','metrics'), ('hustle_composite','metrics'),
        ('loose_ball_rate','metrics'),
    ]
    for col, src in sub_cols_needed:
        if col not in all_z_pos:
            all_z_pos[col] = {}
            for pos_g, pids in pos_groups.items():
                all_z_pos[col].update(z_scores(get_vals(pids, col, source=src)))
        if col not in all_z_lg:
            all_z_lg[col] = z_scores(get_vals(all_qualifying, col, source=src))

    all_composites = {**main_composites}

    for m in metrics_list:
        pid     = m['player_id']
        ps      = seasons_map.get(pid, {})
        min_t   = s(ps.get('min'), 0)
        pos_g   = ps.get('position_group', 'F')

        if min_t < MIN_MINUTES_TOTAL:
            for comp in {**main_composites, **sub_composites}: m[comp] = None
            continue

        # Main composites
        for comp_name, (components, weights, norm) in main_composites.items():
            all_z = all_z_pos if norm == 'pos' else all_z_lg
            z_total = w_total = 0.0
            for (col, src), w in zip(components, weights):
                z = all_z.get(col, {}).get(pid)
                if z is not None:
                    z_total += z * w; w_total += w
            m[comp_name] = round(to_0_100(z_total / w_total), 1) if w_total > 0 else None

        # Sub-composites with volume gates
        for comp_name, (components, weights, norm, gate_key) in sub_composites.items():
            gates = SUB_GATES.get(gate_key, {})

            # Check position gate
            pos_allowed = gates.get('pos_groups')
            if pos_allowed and pos_g not in pos_allowed:
                m[comp_name] = None
                continue

            # Check volume gates against season stats
            gate_fail = False
            if 'ast_pg' in gates and s(ps.get('ast'), 0) < gates['ast_pg']:
                gate_fail = True
            if 'touches_pg' in gates and s(ps.get('touches'), 0) / max(s(ps.get('gp'), 1), 1) < gates['touches_pg']:
                gate_fail = True
            if 'drives_pg' in gates and s(ps.get('drives'), 0) / max(s(ps.get('gp'), 1), 1) < gates['drives_pg']:
                gate_fail = True
            if 'paint_fga_pg' in gates:
                pfga = safe(metrics_by_pid.get(pid, {}).get('paint_fga_pg'))
                if pfga is None or pfga < gates['paint_fga_pg']:
                    gate_fail = True
            if 'def_rim_fga' in gates and s(ps.get('def_rim_fga'), 0) < gates['def_rim_fga']:
                gate_fail = True

            if gate_fail:
                m[comp_name] = None
                continue

            all_z = all_z_pos if norm == 'pos' else all_z_lg
            z_total = w_total = 0.0
            for (col, src), w in zip(components, weights):
                z = all_z.get(col, {}).get(pid)
                if z is not None:
                    z_total += z * w; w_total += w
            m[comp_name] = round(to_0_100(z_total / w_total), 1) if w_total > 0 else None

    # ── Percentiles ───────────────────────────────────────────
    pctile_cols = [
        ('ts_pct','ts_pct','seasons'), ('usg_pct','usg_pct','seasons'),
        ('ast_pct','ast_pct','seasons'), ('net_rating','net_rating','seasons'),
        ('def_delta','def_delta_overall','metrics'), ('rim_prot','rim_protection_score','metrics'),
        ('playmaker','playmaker_score','metrics'), ('creator','creator_score','metrics'),
        ('defender','defender_score','metrics'), ('three_and_d','three_and_d_score','metrics'),
        ('hustle','hustle_score','metrics'),
    ]
    for pctile_name, col, src in pctile_cols:
        all_vals = []
        for m in metrics_list:
            pid   = m['player_id']
            min_t = s(seasons_map.get(pid, {}).get('min'), 0)
            if min_t < MIN_MINUTES_TOTAL: continue
            v = safe(m.get(col)) if src == 'metrics' else safe(seasons_map.get(pid, {}).get(col))
            if v is not None: all_vals.append((pid, v))
        if not all_vals: continue
        sorted_vals = sorted(all_vals, key=lambda x: x[1])
        rank_map    = {pid: i for i, (pid, _) in enumerate(sorted_vals)}
        n           = len(sorted_vals)
        for m in metrics_list:
            pid = m['player_id']
            m[f'{pctile_name}_pctile'] = round((rank_map[pid] / n) * 100, 1) if pid in rank_map else None

    return metrics_list


def upsert_metrics(conn, rows):
    if not rows: return
    cols        = list(rows[0].keys())
    col_str     = ', '.join(cols)
    placeholder = ', '.join(['%s'] * len(cols))
    update_str  = ', '.join([f"{c} = EXCLUDED.{c}" for c in cols if c not in ('player_id','season','season_type','league')])
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
    cur = conn.cursor()
    cur.executemany(sql, [tuple(clean(r.get(c)) for c in cols) for r in rows])
    conn.commit()
    cur.close()
    print(f"  ✅ Upserted {len(rows)} metric rows")


def compute_zone_metrics(conn, season, season_type):
    from collections import defaultdict
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT sz.player_id, sz.zone, sz.fga, sz.fgm, sz.fg_pct, sz.league_fg_pct, ps.gp
        FROM player_shot_zones sz
        JOIN player_seasons ps ON sz.player_id = ps.player_id
            AND ps.season = %s AND ps.season_type = %s
        WHERE sz.season = %s
    """, (season, season_type, season))
    rows = cur.fetchall()
    cur.close()

    if not rows:
        print(f"  ⚠️  No shot zone data found")
        return

    player_zones = defaultdict(dict)
    league       = {}
    for r in rows:
        player_zones[r['player_id']][r['zone']] = dict(r)
        z = r['zone']
        if z not in league: league[z] = {'fgm': 0, 'fga': 0}
        league[z]['fgm'] += r['fgm'] or 0
        league[z]['fga'] += r['fga'] or 0

    league_efg = {}
    for z, t in league.items():
        if t['fga'] > 0:
            is3 = z in ('Corner 3','Left Corner 3','Right Corner 3','Above the Break 3')
            league_efg[z] = (t['fgm'] * 1.5 / t['fga']) if is3 else (t['fgm'] / t['fga'])

    print(f"  League EFG% by zone:")
    for z, efg in sorted(league_efg.items()):
        print(f"    {z:<25} {efg:.3f}")

    updates = []
    for pid, zones in player_zones.items():
        gp = next((z['gp'] for z in zones.values() if z.get('gp')), 1) or 1

        def ze(zone_name, is3=False):
            z   = zones.get(zone_name, {})
            fga = int(z.get('fga') or 0)
            fgm = int(z.get('fgm') or 0)
            if fga == 0: return None, None, 0
            efg   = (fgm * 1.5 / fga) if is3 else (fgm / fga)
            delta = efg - league_efg.get(zone_name, 0)
            return round(efg, 4), round(delta, 4), fga

        def cze(zone_names, is3=False):
            tfga = sum(int(zones.get(z, {}).get('fga') or 0) for z in zone_names)
            tfgm = sum(int(zones.get(z, {}).get('fgm') or 0) for z in zone_names)
            if tfga == 0: return None, None, 0
            efg    = (tfgm * 1.5 / tfga) if is3 else (tfgm / tfga)
            lg_fga = sum(league.get(z, {}).get('fga', 0) for z in zone_names)
            lg_fgm = sum(league.get(z, {}).get('fgm', 0) for z in zone_names)
            lg_efg = (lg_fgm * 1.5 / lg_fga) if (is3 and lg_fga > 0) else (lg_fgm / lg_fga if lg_fga > 0 else 0)
            return round(efg, 4), round(efg - lg_efg, 4), tfga

        pe, pd_, pf = cze(['Restricted Area', 'In The Paint (Non-RA)'])
        me, md, mf  = ze('Mid-Range')
        ce, cd, cf  = ze('Corner 3', is3=True)
        ae, ad, af  = ze('Above the Break 3', is3=True)

        pf_pg = round(pf / gp, 2)
        mf_pg = round(mf / gp, 2)
        cf_pg = round(cf / gp, 2)
        af_pg = round(af / gp, 2)

        # Apply minimum FGA/game gates
        if pf_pg < ZONE_MIN_FGA_PG['paint']:        pe = pd_ = None
        if mf_pg < ZONE_MIN_FGA_PG['midrange']:     me = md  = None
        if cf_pg < ZONE_MIN_FGA_PG['corner3']:      ce = cd  = None
        if af_pg < ZONE_MIN_FGA_PG['above_break3']: ae = ad  = None

        # Volume-weighted delta = delta * fga_per_game
        paint_vw = round(pd_ * pf_pg, 4) if pd_ is not None else None
        mid_vw   = round(md  * mf_pg, 4) if md  is not None else None
        c3_vw    = round(cd  * cf_pg, 4) if cd  is not None else None
        ab3_vw   = round(ad  * af_pg, 4) if ad  is not None else None

        # Combined all 3PT = Corner 3 + Above the Break 3
        all3_efg, all3_delta, all3_fga = cze(['Corner 3', 'Above the Break 3'], is3=True)
        all3_fga_pg = round(all3_fga / gp, 2)
        # No minimum gate on combined — if either zone has attempts it counts
        all3_vw = round(all3_delta * all3_fga_pg, 4) if all3_delta is not None else None

        updates.append((
            pe, pd_, pf_pg, paint_vw,
            me, md, mf_pg, mid_vw,
            ce, cd, cf_pg, c3_vw,
            ae, ad, af_pg, ab3_vw,
            all3_efg, all3_delta, all3_fga_pg, all3_vw,
            pid, season, season_type
        ))

    upd = conn.cursor()
    upd.executemany("""
        UPDATE player_metrics SET
            paint_efg=%s,         paint_efg_delta=%s,        paint_fga_pg=%s,        paint_efg_vw=%s,
            midrange_efg=%s,      midrange_efg_delta=%s,     midrange_fga_pg=%s,     midrange_efg_vw=%s,
            corner3_efg=%s,       corner3_efg_delta=%s,      corner3_fga_pg=%s,      corner3_efg_vw=%s,
            above_break3_efg=%s,  above_break3_efg_delta=%s, above_break3_fga_pg=%s, above_break3_efg_vw=%s,
            all3_efg=%s,          all3_efg_delta=%s,         all3_fga_pg=%s,         all3_efg_vw=%s,
            updated_at=NOW()
        WHERE player_id=%s AND season=%s AND season_type=%s
    """, updates)
    conn.commit()
    upd.close()
    print(f"  ✅ Zone metrics updated for {len(updates)} players")

    # Spot check
    chk = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    for title, col_efg, col_delta, col_fga in [
        ('Top 10 Paint Finishers',      'paint_efg',   'paint_efg_delta',   'paint_fga_pg'),
        ('Top 10 Mid-Range Shooters',   'midrange_efg','midrange_efg_delta','midrange_fga_pg'),
        ('Top 10 All 3PT Shooters (VW)','all3_efg',    'all3_efg_vw',       'all3_fga_pg'),
    ]:
        chk.execute(f"""
            SELECT p.player_name, p.position_group, pm.{col_efg}, pm.{col_delta}, pm.{col_fga}
            FROM player_metrics pm
            JOIN players p ON pm.player_id = p.player_id
            JOIN player_seasons ps ON pm.player_id = ps.player_id
                AND pm.season = ps.season AND pm.season_type = ps.season_type
            WHERE pm.season = %s AND pm.season_type = %s
              AND ps.min >= 1000 AND pm.{col_efg} IS NOT NULL
            ORDER BY pm.{col_delta} DESC NULLS LAST LIMIT 10
        """, (season, season_type))
        print(f"\n  {title}:")
        print(f"  {'Player':<22} {'POS':<4} {'EFG/VW':>8} {'Delta/VW':>9} {'FGA/G':>7}")
        print(f"  {'─'*54}")
        for r in chk.fetchall():
            print(f"  {r['player_name']:<22} {r['position_group'] or '':<4} "
                  f"{r[col_efg] or 0:>8.3f} {r[col_delta] or 0:>+9.3f} {r[col_fga] or 0:>7.1f}")
    chk.close()


def spot_check(conn, season, season_type):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    print(f"\n{'='*90}")
    print(f"Top 15 Playmakers — {season}")
    print(f"{'='*90}")
    cur.execute("""
        SELECT p.player_name, p.position_group, ps.min, ps.pts, ps.ast,
               ps.net_rating, pm.bpm_computed, pm.playmaker_score,
               pm.creator_score, pm.pot_ast_per_tov, pm.pass_quality_index
        FROM player_metrics pm
        JOIN player_seasons ps ON pm.player_id = ps.player_id AND pm.season = ps.season AND pm.season_type = ps.season_type
        JOIN players p ON pm.player_id = p.player_id
        WHERE pm.season = %s AND pm.season_type = %s AND ps.min >= %s
        ORDER BY pm.playmaker_score DESC NULLS LAST LIMIT 15
    """, (season, season_type, MIN_MINUTES_TOTAL))
    print(f"{'Player':<22} {'Pos':<4} {'MIN':>5} {'PTS':>5} {'AST':>5} {'NET':>6} {'BPM':>5} {'PLYMK':>6} {'CREAT':>6} {'POT/TOV':>8} {'PASS_Q':>7}")
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
               pm.defender_score, pm.def_delta_overall, pm.rim_protection_score
        FROM player_metrics pm
        JOIN player_seasons ps ON pm.player_id = ps.player_id AND pm.season = ps.season AND pm.season_type = ps.season_type
        JOIN players p ON pm.player_id = p.player_id
        WHERE pm.season = %s AND pm.season_type = %s AND ps.min >= %s
        ORDER BY pm.defender_score DESC NULLS LAST LIMIT 10
    """, (season, season_type, MIN_MINUTES_TOTAL))
    for r in cur.fetchall():
        print(f"  {r['player_name']:<22} {r['position_group'] or '':<4} "
              f"STL={r['stl'] or 0:.1f}  BLK={r['blk'] or 0:.1f}  "
              f"DEF={r['defender_score'] or 0:.1f}  Δ={r['def_delta_overall'] or 0:+.3f}  "
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

    # Load data then close — don't hold connection during long computation
    conn = psycopg2.connect(DATABASE_URL)
    print("Loading season data...")
    seasons = load_seasons(conn, season, season_type)
    conn.close()

    seasons_map = {s['player_id']: s for s in seasons}

    print("Computing derived metrics...")
    metrics_list = [compute_player_metrics(ps) for ps in seasons]
    print(f"  {len(metrics_list)} players")

    print("Computing composites and percentiles...")
    metrics_list = compute_composites(metrics_list, seasons_map)
    qualifying   = sum(1 for m in metrics_list if m.get('playmaker_score') is not None)
    print(f"  {qualifying} players qualify (>={MIN_MINUTES_TOTAL} min)")

    # Fresh connection for all writes
    print("\nWriting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    upsert_metrics(conn, metrics_list)

    print("\nComputing zone efficiency metrics...")
    compute_zone_metrics(conn, season, season_type)

    spot_check(conn, season, season_type)
    conn.close()

    print(f"\n{'='*60}")
    print(f"✅ Done — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()