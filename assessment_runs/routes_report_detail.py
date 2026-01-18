# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List

from flask import abort, make_response, redirect, render_template, url_for

from db import get_db
from . import assessment_runs_bp


def _safe_filename_part(s: str) -> str:
    """Turn an arbitrary string into a filesystem-safe token."""
    s = (s or "").strip()
    if not s:
        return "unknown"
    # Replace whitespace with underscore
    s = re.sub(r"\s+", "_", s)
    # Keep only safe chars
    s = re.sub(r"[^A-Za-z0-9_.-]", "", s)
    # Avoid empty after cleanup
    return s or "unknown"


def _format_exec_date(dt_val: Any) -> str:
    """Format executed_at into a stable, filename-friendly date."""
    if isinstance(dt_val, datetime):
        return dt_val.strftime("%Y-%m-%d")
    if isinstance(dt_val, str):
        # Try common MySQL datetime string
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(dt_val, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
    return "unknown-date"


# -----------------------------
# REPORT: Assessment Run Detail (PDF only)
# -----------------------------
@assessment_runs_bp.route("/<int:run_id>/report_detail", methods=["GET"])
def report_assessment_run_detail(run_id: int):
    """PDF-only endpoint for Assessment Run Detail Report."""

    from weasyprint import CSS, HTML

    db = get_db()
    try:
        cur = db.cursor()

        # Run snapshot (+ asset impact via assessment_id)
        cur.execute(
            """
            SELECT
              r.run_id,
              r.assessment_id,
              r.executed_at,
              r.status,
              r.assessment_name,
              r.db_type,
              r.datasource_name,
              r.benchmark_name,

              a.asset_impact AS asset_impact,

              r.total_count,
              r.success_count,
              r.fail_count,
              r.error_count,
              r.success_pct,
              r.risk,
              r.asset_adjusted_risk,

              r.risk_level,
              r.asset_adjusted_risk_level
            FROM assessment_runs r
            LEFT JOIN assessments a ON a.assessment_id = r.assessment_id
            WHERE r.run_id=%s
            LIMIT 1
            """,
            (run_id,),
        )
        r = cur.fetchone()
        if not r:
            abort(404)

        # Fallbacks: if assessment_id is NULL or asset_impact is NULL, try alternate lookups
        if r.get("asset_impact") is None:
            try:
                aid = r.get("assessment_id")
                if aid is not None:
                    cur.execute(
                        "SELECT asset_impact FROM assessments WHERE id=%s LIMIT 1",
                        (aid,),
                    )
                    arow = cur.fetchone()
                    if arow and arow.get("asset_impact") is not None:
                        r["asset_impact"] = arow.get("asset_impact")
            except Exception:
                pass

        if r.get("asset_impact") is None:
            try:
                aname = (r.get("assessment_name") or "").strip()
                if aname:
                    cur.execute(
                        "SELECT asset_impact FROM assessments WHERE name=%s LIMIT 1",
                        (aname,),
                    )
                    arow = cur.fetchone()
                    if arow and arow.get("asset_impact") is not None:
                        r["asset_impact"] = arow.get("asset_impact")
            except Exception:
                pass


        # Risk level descriptions
        cur.execute("SELECT description FROM risk_levels WHERE risk_level=%s LIMIT 1", (r["risk_level"],))
        row1 = cur.fetchone()
        r["risk_level_desc"] = (row1.get("description") or "") if row1 else ""

        cur.execute(
            "SELECT description FROM risk_levels WHERE risk_level=%s LIMIT 1",
            (r["asset_adjusted_risk_level"],),
        )
        row2 = cur.fetchone()
        r["asset_risk_level_desc"] = (row2.get("description") or "") if row2 else ""

        # Category risk map (from assessment_run_metrics)
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
        cat_risk_map: Dict[str, Dict[str, Any]] = {}
        for cm in cat_metric_rows:
            dv = cm.get("dimension_value")
            if dv:
                cat_risk_map[str(dv)] = cm

        # Category x Severity matrix (assessment_run_category_metrics)
        cur.execute(
            """
            SELECT category, severity, total_count, pass_count, fail_count, error_count
            FROM assessment_run_category_metrics
            WHERE run_id=%s
            """,
            (run_id,),
        )
        cat_rows = cur.fetchall() or []

        cat_matrix: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for cr in cat_rows:
            c = cr.get("category")
            s = cr.get("severity")
            if not c or not s:
                continue
            cat_matrix.setdefault(str(c), {})[str(s)] = cr

        # Checkpoints (grouped by Category)
        cur.execute(
            """
            SELECT
              run_checkpoint_id,
              checkpoint_name,
              checkpoint_description,
              checkpoint_severity,
              checkpoint_category,
              test_result,
              checkpoint_text_pass,
              checkpoint_text_fail,
              evidence_text,
              error_text
            FROM assessment_run_checkpoints
            WHERE run_id=%s
            """,
            (run_id,),
        )
        all_cp_rows = cur.fetchall() or []

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
        report_categories = categories_order
        severities = ["critical", "major", "minor", "caution"]

    finally:
        try:
            db.close()
        except Exception:
            pass

    html_str = render_template(
        "assessment_runs/report_assessment_run_detail.html",
        r=r,
        report_categories=report_categories,
        cp_by_category=cp_by_category,
        cat_risk_map=cat_risk_map,
        cat_matrix=cat_matrix,
        severities=severities,
        css_href="",  # PDF uses CSS file directly
    )

    css_file = "/home/anil/dbvulscan/static/reports/assessment_run_detail.css"

    pdf_bytes = HTML(string=html_str, base_url="file:///home/anil/dbvulscan/").write_pdf(
        stylesheets=[CSS(filename=css_file)]
    )

    # Required filename format:
    # BenchmarkNAme_Vulnerability_execution_date_detail_Vulnerability_report.pdf
    bench = _safe_filename_part(str(r.get("benchmark_name") or ""))
    exec_date = _format_exec_date(r.get("executed_at"))
    filename = f"{bench}_Vulnerability_{exec_date}_detail_Vulnerability_report.pdf"

    resp = make_response(pdf_bytes)
    resp.headers["Content-Type"] = "application/pdf"
    resp.headers["Content-Disposition"] = f'inline; filename="{filename}"'
    return resp


@assessment_runs_bp.route("/<int:run_id>/report_detail.pdf", methods=["GET"])
def report_assessment_run_detail_pdf(run_id: int):
    return redirect(url_for("assessment_runs.report_assessment_run_detail", run_id=run_id))
