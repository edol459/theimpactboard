"""
Migration — restore sub-composite columns
python backend/migrate.py
"""
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur  = conn.cursor()

cols = [
    "ALTER TABLE player_metrics ADD COLUMN IF NOT EXISTS finishing_score    REAL",
    "ALTER TABLE player_metrics ADD COLUMN IF NOT EXISTS shooting_score     REAL",
    "ALTER TABLE player_metrics ADD COLUMN IF NOT EXISTS creation_score     REAL",
    "ALTER TABLE player_metrics ADD COLUMN IF NOT EXISTS passing_score      REAL",
    "ALTER TABLE player_metrics ADD COLUMN IF NOT EXISTS ballhandling_score REAL",
    "ALTER TABLE player_metrics ADD COLUMN IF NOT EXISTS perimeter_def_score REAL",
    "ALTER TABLE player_metrics ADD COLUMN IF NOT EXISTS interior_def_score  REAL",
    "ALTER TABLE player_metrics ADD COLUMN IF NOT EXISTS activity_score     REAL",
    "ALTER TABLE player_metrics ADD COLUMN IF NOT EXISTS rebounding_score   REAL",
]

for sql in cols:
    cur.execute(sql)
    col = sql.split('ADD COLUMN IF NOT EXISTS ')[1].split(' ')[0]
    print(f"✅ {col}")

conn.commit()
cur.close()
conn.close()
print("\nDone.")