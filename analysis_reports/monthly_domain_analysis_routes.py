# -*- coding: utf-8 -*-
from flask import render_template, request, send_file
from db import get_db
from . import analysis_reports_bp
from .monthly_domain_analysis_report import generate_monthly_domain_analysis_pdf


def _fetch_monthly_domain_stats(*, domain_id: str, end_date: str) -> dict:
    """
    Assessment Statistics için 4 metrik (SADECE end_date günü için).

    MySQL eşdeğeri:
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
        # 1) The number of Security Assessments  (runs)
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

        # 2) The Number of Security Assessments with Errors
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

        # 3) The number of Security Assessment Tests
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

        # 4) The Number of Security Assessment Tests with Errors
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


# --- UPDATED: Fail testlerin severity dağılımı (minor/major/critical) - parametreli tarih ---
def _fetch_failed_tests_severity_distribution(*, domain_id: str, report_date: str) -> dict:
    """
    Fail testlerin severity dağılımı:
    - Domain: assessments.domain_id
    - Tarih: DATE(assessment_runs.executed_at) = report_date
    - Fail: assessment_run_checkpoints.test_result = 'fail'
    - Severity: checkpoint_severity IN ('minor','major','critical')
    """
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


@analysis_reports_bp.route("/monthly-domain-analysis", methods=["GET"])
def monthly_domain_analysis():
    form = {
        "start_date": request.args.get("start_date", ""),
        "end_date": request.args.get("end_date", ""),
        "domain_id": request.args.get("domain_id", ""),
    }

    # Domains LOV (sadece aktifler)
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

    # RUN
    if form["domain_id"] and form["end_date"] and request.args.get("run") == "1":
        # domain_name bul
        domain_name = ""
        for d in domains:
            if str(d["domain_id"]) == str(form["domain_id"]):
                domain_name = d["name"]
                break
        if not domain_name:
            domain_name = f"Domain {form['domain_id']}"

        # Assessment Statistics: SADECE end_date
        stats = _fetch_monthly_domain_stats(
            domain_id=str(form["domain_id"]),
            end_date=form["end_date"],
        )

        # --- UPDATED: chart data artık 2 tarih için (start + end) ---
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

        result = generate_monthly_domain_analysis_pdf(
            domain_id=str(form["domain_id"]),
            domain_name=domain_name,
            start_date=form["start_date"],
            end_date=form["end_date"],
            stats=stats,
            severity_dist_start=severity_dist_start,
            severity_dist_end=severity_dist_end,
        )

        return send_file(result.abs_path, mimetype="application/pdf", as_attachment=False)

    return render_template(
        "analysis_reports/monthly_domain_analysis.html",
        form=form,
        domains=domains,
    )