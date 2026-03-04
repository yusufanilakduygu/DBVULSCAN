# -*- coding: utf-8 -*-
from math import ceil
from flask import render_template, request, session
from db import get_db
from . import analysis_reports_bp

PAGE_SIZE = 17

# changes_test.py içindeki sorgu baz alınarak test_result kolonu çıkarıldı
BASE_QUERY = """
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
  END AS result_change

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
"""

SEVERITY_OPTIONS = ["caution", "minor", "major", "critical"]
CATEGORY_OPTIONS = ["AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"]

# UI kolaylığı için birkaç hazır seçenek (serbest arama da var)
RESULT_CHANGE_OPTIONS = [
    "pass->fail",
    "fail->pass",
    "new->pass",
    "new->fail",
    "pass->deleted",
    "fail->deleted",
    "error->pass",
    "error->fail",
    "new->error",
    "error->deleted",
]


def _get_domains():
    con = get_db()
    try:
        with con.cursor() as cur:
            cur.execute(
                "SELECT domain_id, name FROM domains WHERE is_active=1 ORDER BY name"
            )
            return cur.fetchall() or []
    finally:
        try:
            con.close()
        except Exception:
            pass


def _apply_persistent_params(args_dict):
    """
    Kullanıcı tercihi: değerler session'da kalıcı olsun.
    Query string ile gelen varsa session'a yaz, yoksa session'dan oku.
    """
    keys = [
        "start_date",
        "end_date",
        "domain_id",
        "severity",
        "category",
        "result_change",
        "page",
    ]

    out = {}
    for k in keys:
        if k in args_dict:
            v = args_dict.get(k, "").strip()
            session[f"changes_{k}"] = v
            out[k] = v
        else:
            out[k] = (session.get(f"changes_{k}") or "").strip()

    # page default
    try:
        out["page"] = int(out["page"]) if out["page"] else 1
    except Exception:
        out["page"] = 1

    if out["page"] < 1:
        out["page"] = 1

    return out


def _build_filtered_sql():
    """
    BASE_QUERY'yi derived table yapıp filtreleri dış WHERE ile uyguluyoruz.
    Böylece result_change alias'ı üzerinden filtre yapabiliyoruz.
    """
    sql = f"SELECT * FROM ( {BASE_QUERY} ) t WHERE 1=1"
    return sql


@analysis_reports_bp.route("/changes", methods=["GET"])
def changes():
    params_in = dict(request.args)
    p = _apply_persistent_params(params_in)

    domains = _get_domains()

    # Form zorunluları yoksa sadece ekranı aç
    if not p["start_date"] or not p["end_date"] or not p["domain_id"]:
        return render_template(
            "analysis_reports/changes.html",
            domains=domains,
            form=p,
            rows=[],
            total=0,
            page=1,
            pages=0,
            page_size=PAGE_SIZE,
            severity_options=SEVERITY_OPTIONS,
            category_options=CATEGORY_OPTIONS,
            result_change_options=RESULT_CHANGE_OPTIONS,
        )

    # domain_id int
    try:
        domain_id_int = int(p["domain_id"])
    except Exception:
        domain_id_int = 0

    # SQL + bind params
    bind = [
        p["start_date"], p["start_date"],
        p["end_date"], p["end_date"],
        p["end_date"], p["end_date"],
        p["start_date"], p["start_date"],
        domain_id_int,
    ]

    filtered_sql = _build_filtered_sql()
    filtered_bind = list(bind)

    # Filters
    if p["severity"]:
        filtered_sql += " AND t.checkpoint_severity = %s"
        filtered_bind.append(p["severity"])

    if p["category"]:
        filtered_sql += " AND t.checkpoint_category = %s"
        filtered_bind.append(p["category"])

    if p["result_change"]:
        # contains search
        filtered_sql += " AND t.result_change LIKE %s"
        filtered_bind.append(f"%{p['result_change']}%")

    # Count
    count_sql = f"SELECT COUNT(1) AS cnt FROM ( {filtered_sql} ) x"
    con = get_db()
    try:
        with con.cursor() as cur:
            cur.execute(count_sql, tuple(filtered_bind))
            row = cur.fetchone()
            total = int(row["cnt"]) if row and "cnt" in row else 0

            pages = ceil(total / PAGE_SIZE) if total > 0 else 0
            page = p["page"]
            if pages and page > pages:
                page = pages
            if page < 1:
                page = 1

            offset = (page - 1) * PAGE_SIZE

            data_sql = (
                filtered_sql
                + " ORDER BY t.assessment_name, t.checkpoint_category, t.checkpoint_severity, t.checkpoint_name"
                + " LIMIT %s OFFSET %s"
            )
            data_bind = list(filtered_bind) + [PAGE_SIZE, offset]

            cur.execute(data_sql, tuple(data_bind))
            rows = cur.fetchall() or []

    finally:
        try:
            con.close()
        except Exception:
            pass

    # form.page UI'da current sayfa olarak güncellensin
    p["page"] = page

    return render_template(
        "analysis_reports/changes.html",
        domains=domains,
        form=p,
        rows=rows,
        total=total,
        page=page,
        pages=pages,
        page_size=PAGE_SIZE,
        severity_options=SEVERITY_OPTIONS,
        category_options=CATEGORY_OPTIONS,
        result_change_options=RESULT_CHANGE_OPTIONS,
    )