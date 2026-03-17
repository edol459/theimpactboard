"""
Fix player positions using CommonPlayerInfo
python backend/ingest/fix_positions.py [--season 2025-26]

Takes ~15 min for full run. Safe to interrupt and re-run.
"""
import os, sys, time, argparse
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

parser = argparse.ArgumentParser()
parser.add_argument('--season', default=os.getenv('NBA_SEASON', '2024-25'))
args   = parser.parse_args()
SEASON = args.season

def try_import(name):
    try:
        import importlib
        mod = importlib.import_module("nba_api.stats.endpoints")
        return getattr(mod, name)
    except (ImportError, AttributeError):
        return None

CommonPlayerInfo = try_import("CommonPlayerInfo")
PlayerIndex      = try_import("PlayerIndex")
DELAY = 1.5

def normalize_position(pos):
    mapping = {
        # Abbreviations (from PlayerIndex)
        'PG': 'G', 'SG': 'G', 'G': 'G',
        'G-F': 'GF', 'F-G': 'GF',
        'SF': 'F', 'PF': 'F', 'F': 'F',
        'F-C': 'FC', 'C-F': 'FC',
        'C': 'C',
        # Full words (from CommonPlayerInfo)
        'GUARD': 'G',
        'GUARD-FORWARD': 'GF', 'FORWARD-GUARD': 'GF',
        'FORWARD': 'F',
        'FORWARD-CENTER': 'FC', 'CENTER-FORWARD': 'FC',
        'CENTER': 'C',
    }
    return mapping.get(str(pos).strip().upper(), None)

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# ── Step 0: Re-normalize positions already stored in DB ───────
print("Step 0: Re-normalizing existing position strings...")
cur.execute("SELECT player_id, position FROM players WHERE position IS NOT NULL AND position != ''")
all_players = cur.fetchall()
fixed = 0
for row in all_players:
    pid = row['player_id']
    pos = str(row['position']).strip()
    pos_group = normalize_position(pos) or 'F'
    upd = conn.cursor()
    upd.execute("""
        UPDATE players SET position_group=%s, updated_at=NOW()
        WHERE player_id=%s AND position_group != %s
    """, (pos_group, pid, pos_group))
    if upd.rowcount > 0:
        fixed += 1
    upd.close()
conn.commit()
print(f"  ✅ Re-normalized {fixed} position groups")

# ── Step 1: PlayerIndex for current roster players ────────────
print(f"Step 1: PlayerIndex for current players ({SEASON})...")
try:
    time.sleep(DELAY)
    df = PlayerIndex(league_id="00", season=SEASON).get_data_frames()[0]
    updated = 0
    for _, row in df.iterrows():
        pid = int(row['PERSON_ID'])
        pos = str(row.get('POSITION', '')).strip()
        if not pos or pos in ('', 'nan'):
            continue
        pos_group = normalize_position(pos) or 'F'
        upd = conn.cursor()
        upd.execute("""
            UPDATE players SET position=%s, position_group=%s, updated_at=NOW()
            WHERE player_id=%s
        """, (pos, pos_group, pid))
        if upd.rowcount > 0:
            updated += 1
        upd.close()
    conn.commit()
    print(f"  ✅ {updated} players updated from PlayerIndex")
except Exception as e:
    print(f"  ⚠️  PlayerIndex: {e}")

# ── Step 2: CommonPlayerInfo for remaining ────────────────────
cur.execute("""
    SELECT player_id, player_name FROM players
    WHERE position IS NULL OR position = ''
    ORDER BY player_name
""")
missing = cur.fetchall()
print(f"\nStep 2: {len(missing)} players need CommonPlayerInfo")
print(f"Estimated time: {len(missing) * DELAY / 60:.1f} min\n")

updated = failed = 0
for i, row in enumerate(missing):
    pid  = row['player_id']
    name = row['player_name']
    try:
        time.sleep(DELAY)
        dfs  = CommonPlayerInfo(player_id=pid).get_data_frames()
        pos  = ''
        if dfs and len(dfs[0]) > 0:
            p = dfs[0].iloc[0]
            pos = str(p.get('POSITION', '')).strip()
            if pos in ('', 'nan', 'None'):
                pos = ''
        pos_group = normalize_position(pos) or 'F'
        upd = conn.cursor()
        upd.execute("""
            UPDATE players SET position=%s, position_group=%s, updated_at=NOW()
            WHERE player_id=%s
        """, (pos or None, pos_group, pid))
        conn.commit()
        upd.close()
        updated += 1
        if i % 25 == 0:
            print(f"  [{i+1}/{len(missing)}] {name:<25} pos={pos or '?'} group={pos_group}")
    except Exception as e:
        failed += 1
        if i % 25 == 0:
            print(f"  [{i+1}/{len(missing)}] {name:<25} ❌ {e}")

print(f"\nResults: {updated} updated, {failed} errors")

# ── Step 3: Verify ────────────────────────────────────────────
cur.execute("""
    SELECT position_group, COUNT(*) cnt
    FROM players GROUP BY position_group ORDER BY cnt DESC
""")
print(f"\nPosition group distribution:")
for r in cur.fetchall():
    print(f"  {str(r['position_group']):<8} {r['cnt']} players")

cur.execute("""
    SELECT player_name, position, position_group FROM players
    WHERE player_name IN (
        'Nikola Jokić','Stephen Curry','LeBron James',
        'Giannis Antetokounmpo','Rudy Gobert','Shai Gilgeous-Alexander',
        'Tyrese Haliburton','Anthony Davis','OG Anunoby'
    )
""")
print(f"\nSpot check:")
for r in cur.fetchall():
    print(f"  {r['player_name']:<25} pos={str(r['position']):<8} group={r['position_group']}")

cur.close()
conn.close()
print(f"\n✅ Done. Run compute_metrics.py next.")