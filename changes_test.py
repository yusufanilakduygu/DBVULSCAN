# -*- coding: utf-8 -*-
"""
changes_test.py
DBVulScan - Changes (start_date vs end_date) test runner.

Kullanım:
  python changes_test.py --start 2026-02-01 --end 2026-02-28 --domain 3
Opsiyonel DB parametreleri:
  --host 127.0.0.1 --port 3306 --db repo --user root --password root
"""

import argparse
import sys

try:
    import mysql.connector
except ImportError:
    print("ERROR: mysql-connector-python not installed. Run: pip install mysql-connector-python")
    sys.exit(1)


QUERY = """
SELECT
  a.name AS assessment_name,
  u.checkpoint_name,
  u.checkpoint_severity,
  u.checkpoint_category,

  CASE
    WHEN u.start_result IS NULL AND u.end_result IS NOT NULL
      THEN CONCAT('new->', u.end_result)
    WHEN u.start_result IS NOT NULL AND u.end_result IS NULL
      THEN CONCAT(u.start_result, '->deleted')
    WHEN u.start_result <> u.end_result
      THEN CONCAT(u.start_result, '->', u.end_result)
    ELSE NULL
  END AS result_change,

  u.end_result AS test_result

FROM assessments a

JOIN
(
  /* start -> end */
  SELECT
    s.assessment_id,
    s.checkpoint_id,
    s.checkpoint_name,
    s.checkpoint_severity,
    s.checkpoint_category,
    s.start_result,
    e.end_result
  FROM
  (
    SELECT
      ar.assessment_id,
      arc.checkpoint_id,
      arc.checkpoint_name,
      arc.checkpoint_severity,
      arc.checkpoint_category,
      arc.test_result AS start_result
    FROM assessment_runs ar
    JOIN assessment_run_checkpoints arc
      ON arc.run_id = ar.run_id
    WHERE ar.executed_at >= CONCAT(%s, ' 00:00:00')
      AND ar.executed_at <  CONCAT(DATE_ADD(%s, INTERVAL 1 DAY), ' 00:00:00')
  ) s
  LEFT JOIN
  (
    SELECT
      ar.assessment_id,
      arc.checkpoint_id,
      arc.test_result AS end_result
    FROM assessment_runs ar
    JOIN assessment_run_checkpoints arc
      ON arc.run_id = ar.run_id
    WHERE ar.executed_at >= CONCAT(%s, ' 00:00:00')
      AND ar.executed_at <  CONCAT(DATE_ADD(%s, INTERVAL 1 DAY), ' 00:00:00')
  ) e
    ON e.assessment_id = s.assessment_id
   AND e.checkpoint_id  = s.checkpoint_id

  UNION ALL

  /* end -> start (start’ta olmayan “new” kayıtlar) */
  SELECT
    e.assessment_id,
    e.checkpoint_id,
    e.checkpoint_name,
    e.checkpoint_severity,
    e.checkpoint_category,
    s.start_result,
    e.end_result
  FROM
  (
    SELECT
      ar.assessment_id,
      arc.checkpoint_id,
      arc.checkpoint_name,
      arc.checkpoint_severity,
      arc.checkpoint_category,
      arc.test_result AS end_result
    FROM assessment_runs ar
    JOIN assessment_run_checkpoints arc
      ON arc.run_id = ar.run_id
    WHERE ar.executed_at >= CONCAT(%s, ' 00:00:00')
      AND ar.executed_at <  CONCAT(DATE_ADD(%s, INTERVAL 1 DAY), ' 00:00:00')
  ) e
  LEFT JOIN
  (
    SELECT
      ar.assessment_id,
      arc.checkpoint_id,
      arc.test_result AS start_result
    FROM assessment_runs ar
    JOIN assessment_run_checkpoints arc
      ON arc.run_id = ar.run_id
    WHERE ar.executed_at >= CONCAT(%s, ' 00:00:00')
      AND ar.executed_at <  CONCAT(DATE_ADD(%s, INTERVAL 1 DAY), ' 00:00:00')
  ) s
    ON s.assessment_id = e.assessment_id
   AND s.checkpoint_id  = e.checkpoint_id
  WHERE s.checkpoint_id IS NULL
) u
  ON u.assessment_id = a.assessment_id

WHERE
  a.domain_id = %s
  AND (
    (u.start_result IS NULL AND u.end_result IS NOT NULL)
    OR (u.start_result IS NOT NULL AND u.end_result IS NULL)
    OR (u.start_result <> u.end_result)
  )

ORDER BY
  a.name, u.checkpoint_category, u.checkpoint_severity, u.checkpoint_name;
"""


def print_table(headers, rows, max_width=60):
    """Terminalde basit ama okunur tablo çıktısı."""
    if not rows:
        print("(no rows)")
        return

    def fmt(v):
        if v is None:
            return ""
        s = str(v)
        s = s.replace("\r", " ").replace("\n", " ")
        if len(s) > max_width:
            s = s[: max_width - 3] + "..."
        return s

    data = [[fmt(v) for v in r] for r in rows]
    cols = list(zip(*([headers] + data)))
    widths = [max(len(x) for x in col) for col in cols]

    sep = "+".join("-" * (w + 2) for w in widths)
    sep = f"+{sep}+"

    def line(items):
        return "|" + "|".join(f" {items[i]:<{widths[i]}} " for i in range(len(items))) + "|"

    print(sep)
    print(line(headers))
    print(sep)
    for r in data:
        print(line(r))
    print(sep)
    print(f"Rows: {len(rows)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (start_date)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (end_date)")
    ap.add_argument("--domain", required=True, type=int, help="domain_id")

    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", default=3306, type=int)
    ap.add_argument("--db", default="repo")
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="root")
    ap.add_argument("--limit", default=200, type=int, help="ekranda ilk N satırı göster")

    args = ap.parse_args()

    params = [
        args.start, args.start,   # start window
        args.end, args.end,       # end window (left join)
        args.end, args.end,       # end window (union part)
        args.start, args.start,   # start window (union part)
        args.domain              # domain_id
    ]

    con = mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
    )

    try:
        cur = con.cursor()
        cur.execute(QUERY, params)
        rows = cur.fetchall()
        headers = [d[0] for d in cur.description]

        # Limit uygula (sadece görüntü için)
        if args.limit and len(rows) > args.limit:
            rows = rows[: args.limit]

        print_table(headers, rows)

    finally:
        try:
            cur.close()
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()