"""
The Impact Board — Import DARKO DPM
=====================================
python backend/ingest/import_darko.py path/to/darko-dpm-leaderboard.csv

Matches players by name, updates player_seasons with:
  darko_dpm, darko_odpm, darko_ddpm, darko_box
"""

import os
import sys
import csv
import re
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')
SEASON       = os.getenv('NBA_SEASON', '2024-25')
SEASON_TYPE  = os.getenv('NBA_SEASON_TYPE', 'Regular Season')

if not DATABASE_URL:
    print("❌ DATABASE_URL not set"); sys.exit(1)

csv_path = sys.argv[1] if len(sys.argv) > 1 else 'darko-dpm-leaderboard.csv'
if not os.path.exists(csv_path):
    print(f"❌ File not found: {csv_path}"); sys.exit(1)


def parse_num(val):
    """Parse '+7', '-2', '7' etc to float."""
    if not val or val.strip() in ('', 'N/A', '-'):
        return None
    try:
        return float(val.strip().replace('+', ''))
    except:
        return None


def normalize_name(name):
    """Lowercase, strip accents roughly, remove suffixes for matching."""
    name = name.lower().strip()
    for suffix in [' jr.', ' sr.', ' iii', ' ii', ' iv']:
        name = name.replace(suffix, '')
    replacements = {
        'č':'c','ć':'c','š':'s','ž':'z','đ':'d',
        'á':'a','é':'e','í':'i','ó':'o','ú':'u',
        'ā':'a','ē':'e','ī':'i','ō':'o','ū':'u',
        'ą':'a','ę':'e','ń':'n','ź':'z','ż':'z',
        'ő':'o','ű':'u','ö':'o','ü':'u','ä':'a',
        'ñ':'n','ç':'c','ğ':'g','ı':'i',
        "'":"'","'":"'",  # curly apostrophes → straight
    }
    for src, dst in replacements.items():
        name = name.replace(src, dst)
    return name.strip()


# Manual overrides: DARKO name → DB name
NAME_OVERRIDES = {
    'kristaps porzingis':      'Kristaps Porziņģis',
    'kon knueppel':            'Kon Knüppel',
    "nah'shon hyland":         'Nah\'Shon Hyland',
    'a.j. green':              'AJ Green',
    'daron holmes ii':         'DaRon Holmes II',
    'yanic konan niederhauser':'Yanic Konan Niederhäuser',
    'hugo gonzalez':           'Hugo González',
    'moussa cisse':            'Moussa Sissoko',  # if different
}


# ── Load CSV ──────────────────────────────
rows = []
with open(csv_path, encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        name = row['Player'].strip()
        if not name:
            continue
        rows.append({
            'name':      name,
            'name_norm': normalize_name(name),
            'dpm':       parse_num(row.get('DPM')),
            'odpm':      parse_num(row.get('ODPM')),
            'ddpm':      parse_num(row.get('DDPM')),
            'box':       parse_num(row.get('Box')),
        })

print(f"Loaded {len(rows)} players from CSV")

# ── Load DB players ───────────────────────
conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Add columns if needed
for col, typ in [
    ('darko_dpm',  'REAL'),
    ('darko_odpm', 'REAL'),
    ('darko_ddpm', 'REAL'),
    ('darko_box',  'REAL'),
]:
    cur.execute(f"ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS {col} {typ}")
conn.commit()

cur.execute("""
    SELECT p.player_id, p.player_name
    FROM players p
    JOIN player_seasons ps ON p.player_id = ps.player_id
    WHERE ps.season = %s AND ps.season_type = %s
""", (SEASON, SEASON_TYPE))
db_players = cur.fetchall()
db_by_norm = {normalize_name(r['player_name']): r['player_id'] for r in db_players}
db_by_name = {r['player_name'].lower(): r['player_id'] for r in db_players}

print(f"Found {len(db_players)} players in DB for {SEASON}")

# ── Match and update ──────────────────────
matched   = []
unmatched = []

for row in rows:
    pid = None
    norm = row['name_norm']

    # Check manual override first
    override = NAME_OVERRIDES.get(norm)
    if override:
        pid = db_by_name.get(override.lower()) or db_by_norm.get(normalize_name(override))

    # Exact normalized match
    if not pid:
        pid = db_by_norm.get(norm)

    # Original lowercase
    if not pid:
        pid = db_by_name.get(row['name'].lower())

    # Last-name first-name swap
    if not pid:
        parts = norm.split()
        if len(parts) == 2:
            pid = db_by_norm.get(f"{parts[1]} {parts[0]}")

    if pid:
        matched.append((row['dpm'], row['odpm'], row['ddpm'], row['box'], pid))
    else:
        unmatched.append(row['name'])

# Batch update
update_cur = conn.cursor()
updated = 0
for dpm, odpm, ddpm, box, pid in matched:
    update_cur.execute("""
        UPDATE player_seasons SET
            darko_dpm  = %s,
            darko_odpm = %s,
            darko_ddpm = %s,
            darko_box  = %s
        WHERE player_id = %s AND season = %s AND season_type = %s
    """, (dpm, odpm, ddpm, box, pid, SEASON, SEASON_TYPE))
    if update_cur.rowcount > 0:
        updated += 1

conn.commit()
update_cur.close()

print(f"\n✅ Matched and updated: {updated} players")
print(f"⚠️  Unmatched: {len(unmatched)} players")
if unmatched:
    print("   First 20 unmatched:")
    for n in unmatched[:20]:
        print(f"     - {n}")

# ── Spot check ────────────────────────────
cur.execute("""
    SELECT p.player_name, ps.darko_dpm, ps.darko_odpm, ps.darko_ddpm, ps.darko_box
    FROM player_seasons ps
    JOIN players p ON ps.player_id = p.player_id
    WHERE ps.season = %s AND ps.season_type = %s
      AND ps.darko_dpm IS NOT NULL
    ORDER BY ps.darko_dpm DESC NULLS LAST
    LIMIT 15
""", (SEASON, SEASON_TYPE))

print(f"\n{'='*65}")
print(f"Top 15 by DARKO DPM")
print(f"{'='*65}")
print(f"{'Player':<25} {'DPM':>5} {'ODPM':>6} {'DDPM':>6} {'BOX':>6}")
print("─" * 55)
for r in cur.fetchall():
    print(f"{r['player_name']:<25} {r['darko_dpm'] or 0:>+5.0f} "
          f"{r['darko_odpm'] or 0:>+6.0f} {r['darko_ddpm'] or 0:>+6.0f} "
          f"{r['darko_box'] or 0:>+6.0f}")

cur.close()
conn.close()
print(f"\nNext step: python backend/ingest/compute_metrics.py")