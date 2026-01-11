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

        # Checkpoints (error -> fail -> pass)
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
            ORDER BY
              CASE test_result
                WHEN 'error' THEN 1
                WHEN 'fail'  THEN 2
                WHEN 'pass'  THEN 3
                ELSE 9
              END,
              run_checkpoint_id ASC
            """,
            (run_id,),
        )
        cp_rows = cur.fetchall()

    finally:
        try:
            db.close()
        except Exception:
            pass

    # Render HTML (PDF-first template)
    html_str = render_template(
        "assessment_runs/report_assessment_run_summary.html",
        r=r,
        cp_rows=cp_rows,
        css_href="",  # PDF uses CSS file directly
    )

    css_file = "/home/anil/dbvulscan/static/reports/assessment_run_summary.css"

    pdf_bytes = HTML(string=html_str, base_url="file:///home/anil/dbvulscan/").write_pdf(
        stylesheets=[CSS(filename=css_file)]
    )

    resp = make_response(pdf_bytes)
    resp.headers["Content-Type"] = "application/pdf"
    # Oracle tool hissi: tarayıcıda aç (inline). İstersen attachment yaparsın.
    resp.headers["Content-Disposition"] = f'inline; filename="assessment_run_summary_run_{run_id}.pdf"'
    return resp


# Optional: keep .pdf endpoint but make it point to /report
@assessment_runs_bp.route("/<int:run_id>/report.pdf", methods=["GET"])
def report_assessment_run_summary_pdf(run_id: int):
    return redirect(url_for("assessment_runs.report_assessment_run_summary", run_id=run_id))


# -----------------------------
# CHECKPOINTS: session filters per run_id
# -----------------------------
def _get_cp_filters(run_id: int) -> Dict[str, str]:
    keys = ["severity", "category", "result"]
    store_key = f"assessment_run_checkpoints_filters:{run_id}"
    saved = session.get(store_key, {}) if isinstance(session.get(store_key), dict) else {}
    out: Dict[str, str] = {k: (saved.get(k, "") or "") for k in keys}

    touched = False
    for k in keys:
        if k in request.args:
            out[k] = (request.args.get(k, "") or "").strip()
            touched = True

    if touched:
        session[store_key] = out

    return out


@assessment_runs_bp.route("/<int:run_id>/checkpoints", methods=["GET"])
def checkpoints_list(run_id: int):
    # reset filters
    if request.args.get("reset") == "1":
        session.pop(f"assessment_run_checkpoints_filters:{run_id}", None)
        return redirect(url_for("assessment_runs.checkpoints_list", run_id=run_id))

    f = _get_cp_filters(run_id)

    try:
        page = int(request.args.get("page", "1"))
    except Exception:
        page = 1
    if page < 1:
        page = 1

    where: List[str] = ["run_id = %s"]
    params: List[Any] = [run_id]

    if f["severity"]:
        where.append("checkpoint_severity = %s")
        params.append(f["severity"])
    if f["category"]:
        where.append("checkpoint_category = %s")
        params.append(f["category"])
    if f["result"]:
        where.append("test_result = %s")
        params.append(f["result"])

    where_sql = " WHERE " + " AND ".join(where)

    db = get_db()
    try:
        cur = db.cursor()

        # run header info
        cur.execute("SELECT run_id, assessment_name, executed_at FROM assessment_runs WHERE run_id=%s LIMIT 1", (run_id,))
        run = cur.fetchone()
        if not run:
            abort(404)

        # count
        cur.execute("SELECT COUNT(*) AS cnt FROM assessment_run_checkpoints" + where_sql, params)
        total_records = int(cur.fetchone()["cnt"] or 0)
        total_pages = max(1, int(ceil(total_records / PAGE_SIZE))) if total_records else 1
        if page > total_pages:
            page = total_pages

        offset = (page - 1) * PAGE_SIZE

        # list
        cur.execute(
            "SELECT run_checkpoint_id, checkpoint_name, checkpoint_severity, checkpoint_category, test_result "
            "FROM assessment_run_checkpoints"
            + where_sql
            + " ORDER BY run_checkpoint_id ASC "
            "LIMIT %s OFFSET %s",
            params + [PAGE_SIZE, offset],
        )
        rows = cur.fetchall()
    finally:
        try:
            db.close()
        except Exception:
            pass

    return render_template(
        "assessment_runs/checkpoints_list.html",
        run=run,
        rows=rows,
        filters=f,
        page=page,
        total_pages=total_pages,
        total_records=total_records,
    )


@assessment_runs_bp.route("/<int:run_id>/checkpoints/<int:run_checkpoint_id>", methods=["GET"])
def checkpoint_detail(run_id: int, run_checkpoint_id: int):
    db = get_db()
    try:
        cur = db.cursor()

        cur.execute("SELECT run_id, assessment_name, executed_at FROM assessment_runs WHERE run_id=%s LIMIT 1", (run_id,))
        run = cur.fetchone()

        cur.execute(
            "SELECT * FROM assessment_run_checkpoints WHERE run_id=%s AND run_checkpoint_id=%s LIMIT 1",
            (run_id, run_checkpoint_id),
        )
        r = cur.fetchone()
    finally:
        try:
            db.close()
        except Exception:
            pass

    if not run or not r:
        abort(404)

    return render_template("assessment_runs/checkpoint_detail.html", run=run, r=r)


@assessment_runs_bp.route("/<int:run_id>/metrics", methods=["GET"])
def metrics_list(run_id: int):
    tab = (request.args.get("tab", "general") or "general").strip().lower()
    if tab not in ("general", "category"):
        tab = "general"

    db = get_db()
    try:
        cur = db.cursor()

        cur.execute("SELECT run_id, assessment_name, executed_at FROM assessment_runs WHERE run_id=%s LIMIT 1", (run_id,))
        run = cur.fetchone()
        if not run:
            abort(404)

        cur.execute(
            "SELECT run_metric_id, dimension_type, dimension_value, total_count, success_count, fail_count, error_count, "
            "success_pct, fail_pct, error_pct, risk, risk_level, asset_adjusted_risk, asset_adjusted_risk_level, "
            "severity_sum, failed_severity_sum, executed_at "
            "FROM assessment_run_metrics "
            "WHERE run_id=%s "
            "ORDER BY dimension_type ASC, dimension_value ASC",
            (run_id,),
        )
        rows = cur.fetchall()

        # Category x Severity execution metrics
        cur.execute(
            "SELECT category, severity, total_count, pass_count, fail_count, error_count "
            "FROM assessment_run_category_metrics "
            "WHERE run_id=%s",
            (run_id,),
        )
        cat_rows = cur.fetchall()
    finally:
        try:
            db.close()
        except Exception:
            pass

    # Build matrix: category -> severity -> counts
    cat_matrix: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for cr in cat_rows or []:
        c = cr.get("category")
        s = cr.get("severity")
        if not c or not s:
            continue
        cat_matrix.setdefault(str(c), {})[str(s)] = cr

    # Risk map for each category from assessment_run_metrics (dimension_type='category')
    cat_risk_map: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        if (r.get("dimension_type") or "") == "category":
            dv = r.get("dimension_value")
            if dv:
                cat_risk_map[str(dv)] = r

    categories = ["AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"]
    severities = ["critical", "major", "minor", "caution"]

    return render_template(
        "assessment_runs/metrics_list.html",
        run=run,
        rows=rows,
        tab=tab,
        categories=categories,
        severities=severities,
        cat_matrix=cat_matrix,
        cat_risk_map=cat_risk_map,
    )
