# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from math import ceil
from typing import Any, Dict, List

from flask import abort, redirect, render_template, request, session, url_for, make_response

from db import get_db
from . import assessment_runs_bp

PAGE_SIZE = 10


def _normalize_date(s: str) -> str:
    # Expect YYYY-MM-DD; return '' if invalid
    if not s:
        return ""
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except Exception:
        return ""


# -----------------------------
# RUNS: session filters
# -----------------------------
def _get_runs_filters() -> Dict[str, str]:
    keys = [
        "start_date",
        "end_date",
        "run_id",
        "db_type",
        "status",
        "risk_level",
        "asset_adjusted_risk_level",
    ]
    store_key = "assessment_runs_filters"
    saved = session.get(store_key, {}) if isinstance(session.get(store_key), dict) else {}
    out: Dict[str, str] = {k: (saved.get(k, "") or "") for k in keys}

    touched = False
    for k in keys:
        if k in request.args:
            val = (request.args.get(k, "") or "").strip()
            if k in ("start_date", "end_date"):
                val = _normalize_date(val)
            out[k] = val
            touched = True

    if touched:
        session[store_key] = out

    return out


@assessment_runs_bp.route("/", methods=["GET"])
def list_assessment_runs():
    # Reset filters
    if request.args.get("reset") == "1":
        session.pop("assessment_runs_filters", None)
        return redirect(url_for("assessment_runs.list_assessment_runs"))

    f = _get_runs_filters()

    # Pagination
    try:
        page = int(request.args.get("page", "1"))
    except Exception:
        page = 1
    if page < 1:
        page = 1

    where: List[str] = []
    params: List[Any] = []

    # executed_at range (date inputs)
    if f["start_date"]:
        where.append("executed_at >= %s")
        params.append(f["start_date"] + " 00:00:00")
    if f["end_date"]:
        where.append("executed_at <= %s")
        params.append(f["end_date"] + " 23:59:59")

    # run_id (exact)
    if f["run_id"]:
        where.append("run_id = %s")
        params.append(f["run_id"])

    # enums
    if f["db_type"]:
        where.append("db_type = %s")
        params.append(f["db_type"])
    if f["status"]:
        where.append("status = %s")
        params.append(f["status"])
    if f["risk_level"]:
        where.append("risk_level = %s")
        params.append(f["risk_level"])
    if f["asset_adjusted_risk_level"]:
        where.append("asset_adjusted_risk_level = %s")
        params.append(f["asset_adjusted_risk_level"])

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    db = get_db()
    try:
        cur = db.cursor()

        # Count
        cur.execute("SELECT COUNT(*) AS cnt FROM assessment_runs" + where_sql, params)
        total_records = int(cur.fetchone()["cnt"] or 0)

        total_pages = max(1, int(ceil(total_records / PAGE_SIZE))) if total_records else 1
        if page > total_pages:
            page = total_pages

        offset = (page - 1) * PAGE_SIZE

        # Data (error_count dahil)
        sql = (
            "SELECT run_id, assessment_name, db_type, status, total_count, success_count, fail_count, error_count, "
            "success_pct, risk, risk_level, asset_adjusted_risk, asset_adjusted_risk_level, executed_at "
            "FROM assessment_runs"
            + where_sql
            + " ORDER BY executed_at DESC "
            + "LIMIT %s OFFSET %s"
        )
        cur.execute(sql, params + [PAGE_SIZE, offset])
        rows = cur.fetchall()
    finally:
        try:
            db.close()
        except Exception:
            pass

    return render_template(
        "assessment_runs/list.html",
        rows=rows,
        filters=f,
        page=page,
        page_size=PAGE_SIZE,
        total_records=total_records,
        total_pages=total_pages,
    )


# -----------------------------
# RUN DETAIL (read-only form)
# -----------------------------
@assessment_runs_bp.route("/<int:run_id>", methods=["GET"])
def run_detail(run_id: int):
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("SELECT * FROM assessment_runs WHERE run_id=%s LIMIT 1", (run_id,))
        r = cur.fetchone()
    finally:
        try:
            db.close()
        except Exception:
            pass

    if not r:
        abort(404)

    return render_template("assessment_runs/run_detail.html", r=r)


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

        # Risk level descriptions
        cur.execute("SELECT description FROM risk_levels WHERE risk_level=%s LIMIT 1", (r["risk_level"],))
        row1 = cur.fetchone()
        r["risk_level_desc"] = (row1["description"] if row1 and row1.get("description") else "") if isinstance(r, dict) else ""

        cur.execute(
            "SELECT description FROM risk_levels WHERE risk_level=%s LIMIT 1",
            (r["asset_adjusted_risk_level"],),
        )
        row2 = cur.fetchone()
        r["asset_risk_level_desc"] = (row2["description"] if row2 and row2.get("description") else "") if isinstance(r, dict) else ""

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


# -----------------------------
# REPORT: Assessment Run Detail (PDF only)
# -----------------------------
@assessment_runs_bp.route("/<int:run_id>/report_detail", methods=["GET"])
def report_assessment_run_detail(run_id: int):
    """
    PDF-only endpoint for Assessment Run Detail Report.
    Keeps the Summary Report visual standard (same font/palette/badges).
    """
    from weasyprint import HTML, CSS

    db = get_db()
    try:
        cur = db.cursor()

        # Run snapshot (same fields as summary)
        cur.execute(
            """
            SELECT
              run_id,
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

        # Risk level descriptions
        cur.execute("SELECT description FROM risk_levels WHERE risk_level=%s LIMIT 1", (r["risk_level"],))
        row1 = cur.fetchone()
        r["risk_level_desc"] = (row1["description"] if row1 and row1.get("description") else "") if isinstance(r, dict) else ""

        cur.execute(
            "SELECT description FROM risk_levels WHERE risk_level=%s LIMIT 1",
            (r["asset_adjusted_risk_level"],),
        )
        row2 = cur.fetchone()
        r["asset_risk_level_desc"] = (row2["description"] if row2 and row2.get("description") else "") if isinstance(r, dict) else ""

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

        # Checkpoints with detailed texts (grouped by Category)
        cur.execute(
            """
            SELECT
              run_checkpoint_id,
              checkpoint_name,
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

        # Tüm category'ler metrics'te görünsün (0 olsa bile)
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

    resp = make_response(pdf_bytes)
    resp.headers["Content-Type"] = "application/pdf"
    resp.headers["Content-Disposition"] = f'inline; filename="assessment_run_detail_run_{run_id}.pdf"'
    return resp


@assessment_runs_bp.route("/<int:run_id>/report_detail.pdf", methods=["GET"])
def report_assessment_run_detail_pdf(run_id: int):
    return redirect(url_for("assessment_runs.report_assessment_run_detail", run_id=run_id))


