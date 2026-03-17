"""
Fix shot zones ingestion — handles MultiIndex columns
python backend/ingest/fix_shot_zones.py

Run once to populate player_shot_zones table.
After this, fetch_season.py will be updated to handle it correctly too.
"""
import os
import time
import math
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')
SEASON       = os.getenv('NBA_SEASON', '2024-25')

def try_import(name):
    try:
        import importlib
        mod = importlib.import_module("nba_api.stats.endpoints")
        return getattr(mod, name)
    except (ImportError, AttributeError):
        return None

LeagueDashPlayerShotLocations = try_import("LeagueDashPlayerShotLocations")

print(f"Fetching shot locations for {SEASON}...")
time.sleep(1.5)
ep  = LeagueDashPlayerShotLocations(
    season=SEASON,
    season_type_all_star='Regular Season',
    distance_range='By Zone',
)
df = ep.get_data_frames()[0]
print(f"  {len(df)} players")

# ── MultiIndex column access ──────────────
# Columns are tuples: ('Restricted Area', 'FGM') etc.
# Helper to get a zone's stats safely
def get_zone(df, row, zone, stat):
    try:
        return row[(zone, stat)]
    except KeyError:
        return 0

# Define the zones we care about
ZONES = [
    'Restricted Area',
    'In The Paint (Non-RA)',
    'Mid-Range',
    'Corner 3',          # combined L+R corner (pre-computed by NBA)
    'Above the Break 3',
    'Left Corner 3',
    'Right Corner 3',
]

# ── Compute league averages per zone ──────
league_avg = {}
for zone in ZONES:
    try:
        total_fga = df[(zone, 'FGA')].sum()
        total_fgm = df[(zone, 'FGM')].sum()
        league_avg[zone] = float(total_fgm / total_fga) if total_fga > 0 else 0.0
    except KeyError:
        league_avg[zone] = 0.0

print(f"\nLeague averages by zone:")
for zone, avg in league_avg.items():
    print(f"  {zone:<25} {avg:.3f}")

# ── Build rows ────────────────────────────
def safe_float(val):
    try:
        v = float(val)
        return None if math.isnan(v) else v
    except:
        return None

rows = []
for _, row in df.iterrows():
    pid = None
    try:
        pid = int(float(row[('', 'PLAYER_ID')]))
    except:
        try:
            pid = int(float(row.iloc[0]))
        except:
            continue
    if not pid:
        continue

    for zone in ZONES:
        try:
            fga = safe_float(row[(zone, 'FGA')])
            fgm = safe_float(row[(zone, 'FGM')])
            pct = safe_float(row[(zone, 'FG_PCT')])
        except KeyError:
            continue

        if fga is None:
            fga = 0
        if fgm is None:
            fgm = 0

        rows.append((
            pid,
            SEASON,
            zone,
            int(fga),
            int(fgm),
            pct,
            league_avg.get(zone),
            datetime.now(),
        ))

print(f"\nBuilt {len(rows)} zone rows for {len(df)} players")

# ── Write to DB ───────────────────────────
conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

sql = """
    INSERT INTO player_shot_zones
        (player_id, season, zone, fga, fgm, fg_pct, league_fg_pct, updated_at)
    VALUES %s
    ON CONFLICT (player_id, season, zone) DO UPDATE SET
        fga           = EXCLUDED.fga,
        fgm           = EXCLUDED.fgm,
        fg_pct        = EXCLUDED.fg_pct,
        league_fg_pct = EXCLUDED.league_fg_pct,
        updated_at    = NOW()
"""
execute_values(cur, sql, rows)
conn.commit()

# Verify
cur.execute("""
    SELECT zone, COUNT(*) players, AVG(fg_pct) avg_fg_pct
    FROM player_shot_zones
    WHERE season = %s
    GROUP BY zone ORDER BY zone
""", (SEASON,))
print(f"\nVerification — zones in DB:")
print(f"{'Zone':<25} {'Players':>8} {'Avg FG%':>8}")
print("─" * 45)
for r in cur.fetchall():
    print(f"  {r[0]:<23} {r[1]:>8} {float(r[2] or 0):>8.3f}")

cur.close()
conn.close()
print(f"\n✅ Shot zones loaded. Run compute_metrics.py next.")