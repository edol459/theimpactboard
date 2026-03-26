import psycopg2, os
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

cur.execute("""
    SELECT p.player_name, p.position_group,
           ps.gravity_score,
           ps.gravity_onball_perimeter,
           ps.gravity_offball_perimeter,
           ps.gravity_onball_interior,
           ps.gravity_offball_interior
    FROM player_seasons ps
    JOIN players p ON ps.player_id = p.player_id
    WHERE ps.season = '2025-26' AND ps.season_type = 'Regular Season'
      AND ps.gravity_score IS NOT NULL
      AND ps.min >= 1000
    ORDER BY ps.gravity_score DESC
    LIMIT 15
""")
rows = cur.fetchall()
print(f"Players with gravity data: checking...")
print(f"{'Player':<25} {'POS':<5} {'Overall':>8} {'OnBall-P':>9} {'OffBall-P':>10} {'OnBall-I':>9} {'OffBall-I':>10}")
for r in rows:
    print(f"{r[0]:<25} {r[1]:<5} {str(r[2]):>8} {str(r[3]):>9} {str(r[4]):>10} {str(r[5]):>9} {str(r[6]):>10}")

# Check how many have data
cur.execute("""
    SELECT 
        COUNT(*) FILTER (WHERE gravity_score IS NOT NULL) as has_overall,
        COUNT(*) FILTER (WHERE gravity_onball_perimeter IS NOT NULL) as has_onball_p,
        COUNT(*) FILTER (WHERE gravity_offball_perimeter IS NOT NULL) as has_offball_p,
        COUNT(*) FILTER (WHERE gravity_onball_interior IS NOT NULL) as has_onball_i,
        COUNT(*) FILTER (WHERE gravity_offball_interior IS NOT NULL) as has_offball_i
    FROM player_seasons
    WHERE season = '2025-26' AND season_type = 'Regular Season'
""")
r = cur.fetchone()
print(f"\nCoverage: overall={r[0]}, onball_p={r[1]}, offball_p={r[2]}, onball_i={r[3]}, offball_i={r[4]}")