"""
Migration — add new metric columns
python backend/migrate.py
"""
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur  = conn.cursor()

migrations = [
    "ALTER TABLE player_metrics ADD COLUMN IF NOT EXISTS pot_ast_per_tov REAL",
    "ALTER TABLE player_metrics ADD COLUMN IF NOT EXISTS pass_quality_index REAL",
]

for sql in migrations:
    cur.execute(sql)
    print(f"✅ {sql}")

conn.commit()
cur.close()
conn.close()
print("Done.")