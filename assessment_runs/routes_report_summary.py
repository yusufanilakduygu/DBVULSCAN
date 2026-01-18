# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

from flask import abort, redirect, render_template, url_for, make_response

from db import get_db
from . import assessment_runs_bp


# -----------------------------
# REPORT: Assessment Run Summary (PDF only)
# -----------------------------
@assessment_runs_bp.route("/<int:run_id>/report", methods=["GET"])
def report_assessment_run_summary(run_id: int):
    """
    PDF-only endpoint (Oracle tool style).
    No HTML preview.
    """
    from weasyprint import HTML, CSS

    db = get_db()
    try:
        cur = db.cursor()

        # Summary + Metrics (all from assessment_runs)
        cur.execute(
            """
            SELECT
              run_id,
              assessment_id,
              executed_at,
              status,
              assessment_name,
              db_type,
              datasource_name,
              benchmark_name,

              total_count,
              success_count,
              fail_count,
              error_count,
              success_pct,
              risk,
              asset_adjusted_risk,

              risk_level,
              asset_adjusted_risk_level
            FROM assessment_runs
            WHERE run_id=%s
            LIMIT 1
            """,
            (run_id,),
        )
        r = cur.fetchone()
        if not r:
            abort(404)

        # Asset impact (authoritative source: assessments)
        # NOTE: assessment_runs already has asset_impact snapshot, but user requested to pull from assessments.
        cur.execute(
            """
            SELECT asset_impact
            FROM assessments
            WHERE assessment_id=%s
            LIMIT 1
            """,
            (r["assessment_id"],),
        )
        arow = cur.fetchone()
        if arow and arow.get("asset_impact"):
            r["asset_impact"] = arow["asset_impact"]

        # Risk level descriptions
        cur.execute("SELECT description FROM risk_levels WHERE risk_level=%s LIMIT 1", (r["risk_level"],))
        row1 = cur.fetchone()
        r["risk_level_desc"] = (
            (row1["description"] if row1 and row1.get("description") else "") if isinstance(r, dict) else ""
        )

        cur.execute(
            "SELECT description FROM risk_levels WHERE risk_level=%s LIMIT 1",
            (r["asset_adjusted_risk_level"],),
        )
        row2 = cur.fetchone()
        r["asset_risk_level_desc"] = (
            (row2["description"] if row2 and row2.get("description") else "") if isinstance(r, dict) else ""
        )

        # Category Risk Map (dimension_type='category' in assessment_run_metrics)
        cur.execute(
            """
            SELECT
              dimension_value,
              risk,
              risk_level,
              asset_adjusted_risk,
              asset_adjusted_risk_level
            FROM assessment_run_metrics
            WHERE run_id=%s AND dimension_type='category'
            ORDER BY dimension_value ASC
            """,
            (run_id,),
        )
        cat_metric_rows = cur.fetchall() or []
        cat_metrics: Dict[str, Dict[str, Any]] = {}
        for cm in cat_metric_rows:
            dv = cm.get("dimension_value")
            if dv:
                cat_metrics[str(dv)] = cm

        # Checkpoints (FAIL -> ERROR -> PASS) (grouping in Python)
        cur.execute(
            """
            SELECT
              run_checkpoint_id,
              checkpoint_name,
              checkpoint_severity,
              checkpoint_category,
              test_result
            FROM assessment_run_checkpoints
            WHERE run_id=%s
            """,
            (run_id,),
        )
        all_cp_rows = cur.fetchall() or []

        # Group by category
        cp_by_category: Dict[str, List[Dict[str, Any]]] = {}
        for row in all_cp_rows:
            cat = (row.get("checkpoint_category") or "").strip()
            if not cat:
                continue
            cp_by_category.setdefault(cat, []).append(row)

        # Sort inside each category: FAIL first (then ERROR, then PASS), then Severity, then id
        res_pri = {"fail": 1, "error": 2, "pass": 3}
        sev_pri = {"critical": 1, "major": 2, "minor": 3, "caution": 4}

        def _key(x: Dict[str, Any]):
            tr = (x.get("test_result") or "").lower()
            sv = (x.get("checkpoint_severity") or "").lower()
            return (
                res_pri.get(tr, 9),
                sev_pri.get(sv, 9),
                int(x.get("run_checkpoint_id") or 0),
            )

        for cat, items in cp_by_category.items():
            items.sort(key=_key)

        categories_order = ["AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"]
        report_categories = [c for c in categories_order if c in cp_by_category]

    finally:
        try:
            db.close()
        except Exception:
            pass

    # Render HTML (PDF-first template)
    html_str = render_template(
        "assessment_runs/report_assessment_run_summary.html",
        r=r,
        report_categories=report_categories,
        cp_by_category=cp_by_category,
        cat_metrics=cat_metrics,
        css_href="",  # PDF uses CSS file directly
    )

    css_file = "/home/anil/dbvulscan/static/reports/assessment_run_summary.css"

    pdf_bytes = HTML(string=html_str, base_url="file:///home/anil/dbvulscan/").write_pdf(
        stylesheets=[CSS(filename=css_file)]
    )

    resp = make_response(pdf_bytes)
    resp.headers["Content-Type"] = "application/pdf"
    resp.headers["Content-Disposition"] = f'inline; filename="assessment_run_summary_run_{run_id}.pdf"'
    return resp


# Optional: keep .pdf endpoint but make it point to /report
@assessment_runs_bp.route("/<int:run_id>/report.pdf", methods=["GET"])
def report_assessment_run_summary_pdf(run_id: int):
    return redirect(url_for("assessment_runs.report_assessment_run_summary", run_id=run_id))
