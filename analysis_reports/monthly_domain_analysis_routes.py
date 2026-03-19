# -*- coding: utf-8 -*-
from flask import render_template, request, send_file
from db import get_db
from . import analysis_reports_bp
from .monthly_domain_analysis_report import generate_monthly_domain_analysis_pdf


def _fetch_monthly_domain_stats(*, domain_id: str, end_date: str) -> dict:
    """
    Assessment Statistics icin 4 metrik (SADECE end_date gunu icin).

    MySQL esdegeri:
      DATE(r.executed_at) = %s
    """
    con = get_db()
    stats = {
        "tests_with_errors": 0,
        "assessments_with_errors": 0,
        "total_tests": 0,
        "total_assessments": 0,
    }

    with con.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM assessment_runs r
            JOIN assessments a ON a.assessment_id = r.assessment_id
            WHERE a.domain_id = %s
              AND DATE(r.executed_at) = %s
            """,
            (domain_id, end_date),
        )
        row = cur.fetchone()
        stats["total_assessments"] = int((row or {}).get("cnt", 0) or 0)

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM assessment_runs r
            JOIN assessments a ON a.assessment_id = r.assessment_id
            WHERE a.domain_id = %s
              AND DATE(r.executed_at) = %s
              AND (
                    r.status = 'error'
                    OR EXISTS (
                        SELECT 1
                        FROM assessment_run_checkpoints c
                        WHERE c.run_id = r.run_id
                          AND c.run_month = r.run_month
                          AND c.test_result = 'error'
                    )
                  )
            """,
            (domain_id, end_date),
        )
        row = cur.fetchone()
        stats["assessments_with_errors"] = int((row or {}).get("cnt", 0) or 0)

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM assessment_run_checkpoints c
            JOIN assessment_runs r
              ON r.run_id = c.run_id
             AND r.run_month = c.run_month
            JOIN assessments a ON a.assessment_id = r.assessment_id
            WHERE a.domain_id = %s
              AND DATE(r.executed_at) = %s
            """,
            (domain_id, end_date),
        )
        row = cur.fetchone()
        stats["total_tests"] = int((row or {}).get("cnt", 0) or 0)

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM assessment_run_checkpoints c
            JOIN assessment_runs r
              ON r.run_id = c.run_id
             AND r.run_month = c.run_month
            JOIN assessments a ON a.assessment_id = r.assessment_id
            WHERE a.domain_id = %s
              AND DATE(r.executed_at) = %s
              AND c.test_result = 'error'
            """,
            (domain_id, end_date),
        )
        row = cur.fetchone()
        stats["tests_with_errors"] = int((row or {}).get("cnt", 0) or 0)

    return stats


def _fetch_failed_tests_severity_distribution(*, domain_id: str, report_date: str) -> dict:
    con = get_db()
    dist = {"critical": 0, "major": 0, "minor": 0}

    with con.cursor() as cur:
        cur.execute(
            """
            SELECT c.checkpoint_severity AS severity, COUNT(*) AS cnt
            FROM assessment_run_checkpoints c
            JOIN assessment_runs r
              ON r.run_id = c.run_id
             AND r.run_month = c.run_month
            JOIN assessments a
              ON a.assessment_id = r.assessment_id
            WHERE a.domain_id = %s
              AND DATE(r.executed_at) = %s
              AND c.test_result = 'fail'
              AND c.checkpoint_severity IN ('minor','major','critical')
            GROUP BY c.checkpoint_severity
            """,
            (domain_id, report_date),
        )
        rows = cur.fetchall() or []
        for r in rows:
            sev = (r.get("severity") or "").lower()
            if sev in dist:
                dist[sev] = int(r.get("cnt") or 0)

    return dist


def _fetch_assessment_success_pct_rows(*, domain_id: str, start_date: str, end_date: str) -> list[dict]:
    con = get_db()

    def _fetch_for_date(report_date: str) -> dict:
        if not report_date:
            return {}

        rows_by_assessment = {}
        with con.cursor() as cur:
            cur.execute(
                """
                SELECT
                    r.assessment_id,
                    r.assessment_name,
                    r.success_pct
                FROM assessment_runs r
                JOIN assessments a ON a.assessment_id = r.assessment_id
                WHERE a.domain_id = %s
                  AND DATE(r.executed_at) = %s
                  AND r.run_id = (
                      SELECT r2.run_id
                      FROM assessment_runs r2
                      WHERE r2.assessment_id = r.assessment_id
                        AND DATE(r2.executed_at) = %s
                      ORDER BY r2.executed_at DESC, r2.run_id DESC
                      LIMIT 1
                  )
                ORDER BY r.assessment_name
                """,
                (domain_id, report_date, report_date),
            )
            for row in cur.fetchall() or []:
                assessment_id = str(row.get("assessment_id"))
                rows_by_assessment[assessment_id] = {
                    "assessment_id": assessment_id,
                    "assessment_name": row.get("assessment_name") or f"Assessment {assessment_id}",
                    "success_pct": row.get("success_pct"),
                }

        return rows_by_assessment

    start_rows = _fetch_for_date(start_date)
    end_rows = _fetch_for_date(end_date)

    merged_rows = []
    for assessment_id in sorted(
        set(start_rows.keys()) | set(end_rows.keys()),
        key=lambda item: (
            (start_rows.get(item) or end_rows.get(item) or {}).get("assessment_name", "").lower(),
            int(item) if str(item).isdigit() else item,
        ),
    ):
        start_row = start_rows.get(assessment_id, {})
        end_row = end_rows.get(assessment_id, {})
        merged_rows.append(
            {
                "assessment_id": assessment_id,
                "assessment_name": start_row.get("assessment_name")
                or end_row.get("assessment_name")
                or f"Assessment {assessment_id}",
                "start_success_pct": start_row.get("success_pct"),
                "end_success_pct": end_row.get("success_pct"),
            }
        )

    return merged_rows


def _fetch_failed_critical_checkpoint_rows(*, domain_id: str, start_date: str, end_date: str) -> list[dict]:
    con = get_db()
    rows_out = []

    with con.cursor() as cur:
        cur.execute(
            """
            SELECT
                c.checkpoint_name,
                SUM(CASE WHEN DATE(r.executed_at) = %s THEN 1 ELSE 0 END) AS start_date_fail_count,
                SUM(CASE WHEN DATE(r.executed_at) = %s THEN 1 ELSE 0 END) AS end_date_fail_count
            FROM assessment_run_checkpoints c
            JOIN assessment_runs r
              ON r.run_id = c.run_id
             AND r.run_month = c.run_month
            JOIN assessments a
              ON a.assessment_id = r.assessment_id
            WHERE a.domain_id = %s
              AND DATE(r.executed_at) IN (%s, %s)
              AND c.checkpoint_severity = 'critical'
              AND c.test_result = 'fail'
            GROUP BY c.checkpoint_name
            ORDER BY c.checkpoint_name
            """,
            (start_date, end_date, domain_id, start_date, end_date),
        )

        for row in cur.fetchall() or []:
            rows_out.append(
                {
                    "checkpoint_name": row.get("checkpoint_name") or "-",
                    "start_fail_count": int(row.get("start_date_fail_count") or 0),
                    "end_fail_count": int(row.get("end_date_fail_count") or 0),
                }
            )

    return rows_out


def _fetch_failed_critical_tests_by_assessment_rows(*, domain_id: str, start_date: str, end_date: str) -> list[dict]:
    con = get_db()
    rows_out = []

    with con.cursor() as cur:
        cur.execute(
            """
            SELECT
                r.assessment_name,
                SUM(CASE WHEN DATE(r.executed_at) = %s THEN 1 ELSE 0 END) AS start_date_critical_failed_test_count,
                SUM(CASE WHEN DATE(r.executed_at) = %s THEN 1 ELSE 0 END) AS end_date_critical_failed_test_count
            FROM assessment_run_checkpoints c
            JOIN assessment_runs r
              ON r.run_id = c.run_id
             AND r.run_month = c.run_month
            JOIN assessments a
              ON a.assessment_id = r.assessment_id
            WHERE a.domain_id = %s
              AND DATE(r.executed_at) IN (%s, %s)
              AND c.checkpoint_severity = 'critical'
              AND c.test_result = 'fail'
            GROUP BY r.assessment_name
            ORDER BY r.assessment_name
            """,
            (start_date, end_date, domain_id, start_date, end_date),
        )

        for row in cur.fetchall() or []:
            rows_out.append(
                {
                    "assessment_name": row.get("assessment_name") or "-",
                    "start_fail_count": int(row.get("start_date_critical_failed_test_count") or 0),
                    "end_fail_count": int(row.get("end_date_critical_failed_test_count") or 0),
                }
            )

    return rows_out


def _fetch_vulnerability_assessment_changes(*, domain_id: str, start_date: str, end_date: str) -> list[dict]:
    con = get_db()
    rows_out = []

    base_query = """
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
         AND arc.run_month = ar.run_month
        WHERE ar.executed_at >= CONCAT(%s, ' 00:00:00')
          AND ar.executed_at < CONCAT(DATE_ADD(%s, INTERVAL 1 DAY), ' 00:00:00')
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
         AND arc.run_month = ar.run_month
        WHERE ar.executed_at >= CONCAT(%s, ' 00:00:00')
          AND ar.executed_at < CONCAT(DATE_ADD(%s, INTERVAL 1 DAY), ' 00:00:00')
      ) e
        ON e.assessment_id = s.assessment_id
       AND e.checkpoint_id = s.checkpoint_id

      UNION ALL

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
         AND arc.run_month = ar.run_month
        WHERE ar.executed_at >= CONCAT(%s, ' 00:00:00')
          AND ar.executed_at < CONCAT(DATE_ADD(%s, INTERVAL 1 DAY), ' 00:00:00')
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
         AND arc.run_month = ar.run_month
        WHERE ar.executed_at >= CONCAT(%s, ' 00:00:00')
          AND ar.executed_at < CONCAT(DATE_ADD(%s, INTERVAL 1 DAY), ' 00:00:00')
      ) s
        ON s.assessment_id = e.assessment_id
       AND s.checkpoint_id = e.checkpoint_id
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
    ORDER BY a.name, u.checkpoint_category, u.checkpoint_severity, u.checkpoint_name
    """

    bind = (
        start_date, start_date,
        end_date, end_date,
        end_date, end_date,
        start_date, start_date,
        domain_id,
    )

    with con.cursor() as cur:
        cur.execute(base_query, bind)
        for row in cur.fetchall() or []:
            rows_out.append(
                {
                    "assessment_name": row.get("assessment_name") or "-",
                    "checkpoint_name": row.get("checkpoint_name") or "-",
                    "checkpoint_severity": row.get("checkpoint_severity") or "-",
                    "checkpoint_category": row.get("checkpoint_category") or "-",
                    "result_change": row.get("result_change") or "-",
                }
            )

    return rows_out


@analysis_reports_bp.route("/monthly-domain-analysis", methods=["GET"])
def monthly_domain_analysis():
    form = {
        "start_date": request.args.get("start_date", ""),
        "end_date": request.args.get("end_date", ""),
        "domain_id": request.args.get("domain_id", ""),
    }

    con = get_db()
    with con.cursor() as cur:
        cur.execute(
            """
            SELECT domain_id, name
            FROM domains
            WHERE is_active = 1
            ORDER BY name
            """
        )
        rows = cur.fetchall()

    domains = [{"domain_id": r["domain_id"], "name": r["name"]} for r in rows]

    if form["domain_id"] and form["end_date"] and request.args.get("run") == "1":
        domain_name = ""
        for d in domains:
            if str(d["domain_id"]) == str(form["domain_id"]):
                domain_name = d["name"]
                break
        if not domain_name:
            domain_name = f"Domain {form['domain_id']}"

        stats = _fetch_monthly_domain_stats(
            domain_id=str(form["domain_id"]),
            end_date=form["end_date"],
        )

        severity_dist_start = {}
        if form["start_date"]:
            severity_dist_start = _fetch_failed_tests_severity_distribution(
                domain_id=str(form["domain_id"]),
                report_date=form["start_date"],
            )

        severity_dist_end = _fetch_failed_tests_severity_distribution(
            domain_id=str(form["domain_id"]),
            report_date=form["end_date"],
        )

        assessment_score_rows = _fetch_assessment_success_pct_rows(
            domain_id=str(form["domain_id"]),
            start_date=form["start_date"],
            end_date=form["end_date"],
        )

        failed_critical_checkpoint_rows = _fetch_failed_critical_checkpoint_rows(
            domain_id=str(form["domain_id"]),
            start_date=form["start_date"],
            end_date=form["end_date"],
        )

        failed_critical_tests_by_assessment_rows = _fetch_failed_critical_tests_by_assessment_rows(
            domain_id=str(form["domain_id"]),
            start_date=form["start_date"],
            end_date=form["end_date"],
        )

        vulnerability_assessment_change_rows = _fetch_vulnerability_assessment_changes(
            domain_id=str(form["domain_id"]),
            start_date=form["start_date"],
            end_date=form["end_date"],
        )

        result = generate_monthly_domain_analysis_pdf(
            domain_id=str(form["domain_id"]),
            domain_name=domain_name,
            start_date=form["start_date"],
            end_date=form["end_date"],
            stats=stats,
            severity_dist_start=severity_dist_start,
            severity_dist_end=severity_dist_end,
            assessment_score_rows=assessment_score_rows,
            failed_critical_checkpoint_rows=failed_critical_checkpoint_rows,
            failed_critical_tests_by_assessment_rows=failed_critical_tests_by_assessment_rows,
            vulnerability_assessment_change_rows=vulnerability_assessment_change_rows,
        )

        return send_file(result.abs_path, mimetype="application/pdf", as_attachment=False)

    return render_template(
        "analysis_reports/monthly_domain_analysis.html",
        form=form,
        domains=domains,
    )
