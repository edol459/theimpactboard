"""
Quick check of bio stats columns and position data
python backend/ingest/check_positions.py
"""
import os, time
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

# Check what's actually stored in the players table
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

cur.execute("""
    SELECT player_name, position, position_group
    FROM players
    ORDER BY player_name
    LIMIT 30
""")
rows = cur.fetchall()
print("Current position data in players table:")
print(f"{'Player':<25} {'position':<15} {'position_group'}")
print("─" * 55)
for r in rows:
    print(f"{r['player_name']:<25} {str(r['position']):<15} {str(r['position_group'])}")

# Check a few specific players
print("\nSpot check key players:")
cur.execute("""
    SELECT player_name, position, position_group
    FROM players
    WHERE player_name IN (
        'Nikola Jokić', 'Stephen Curry', 'LeBron James',
        'Tyrese Haliburton', 'Giannis Antetokounmpo',
        'Rudy Gobert', 'OG Anunoby', 'Shai Gilgeous-Alexander'
    )
""")
for r in cur.fetchall():
    print(f"  {r['player_name']:<25} pos={r['position']:<15} group={r['position_group']}")

cur.close()
conn.close()

# Also check what the bio stats API actually returns
print("\n\nChecking bio stats API column names...")
try:
    from nba_api.stats.endpoints import LeagueDashPlayerBioStats
    time.sleep(1)
    bio = LeagueDashPlayerBioStats(
        season='2024-25',
        season_type_all_star='Regular Season',
        per_mode_simple='PerGame'
    ).get_data_frames()[0]
    print(f"Bio stats columns: {list(bio.columns)}")
    print(f"\nSample rows (position-related columns):")
    pos_cols = [c for c in bio.columns if 'POS' in c.upper() or 'HEIGHT' in c.upper()
                or 'WEIGHT' in c.upper()]
    print(f"Position columns found: {pos_cols}")
    sample = bio[['PLAYER_NAME'] + pos_cols].head(10)
    print(sample.to_string(index=False))
except Exception as e:
    print(f"API error: {e}")