"""KFIT DB 스키마/연결성 점검 스크립트

사용법:
  python db_health_check.py

- DB 파일 위치: ~/KFIT_Data/kfit_local.db (database.py의 DB_PATH 기준)
- tables / columns / row count를 출력합니다.
"""

import sqlite3
import database


def main():
    database.init_db()
    conn = database.get_connection()
    cur = conn.cursor()

    print(f"DB_PATH: {database.DB_PATH}\n")

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    print("[Tables]")
    for t in tables:
        print(f"- {t}")
    print()

    for t in tables:
        cur.execute(f"PRAGMA table_info({t})")
        cols = [r[1] for r in cur.fetchall()]
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        cnt = cur.fetchone()[0]
        print(f"[{t}] rows={cnt}")
        print("  columns:", ", ".join(cols))
        print()

    conn.close()


if __name__ == "__main__":
    main()
