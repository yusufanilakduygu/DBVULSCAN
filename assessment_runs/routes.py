# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from math import ceil
from typing import Any, Dict, List

from flask import abort, redirect, render_template, request, session, url_for

from db import get_db
from . import assessment_runs_bp

PAGE_SIZE = 10


def _normalize_date(s: str) -> str:
    if not s:
        return ""
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except Exception:
        return ""


def _normalize_int(s: str) -> str:
    """Return string form of positive int, else ''."""
    if not s:
        return ""
    try:
        v = int(str(s).strip())
        return str(v) if v > 0 else ""
    except Exception:
        return ""


# -----------------------------
# RUNS: session filters
# -----------------------------
def _get_runs_filters() -> Dict[str, str]:
    keys = [
        "domain_run_id",
        "start_date",
        "end_date",
        "assessment_id",
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

            if k in ("assessment_id", "domain_run_id"):
                val = _normalize_int(val)

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

    # domain_run_id (exact)
    if f["domain_run_id"]:
        where.append("domain_run_id = %s")
        params.append(int(f["domain_run_id"]))

    # executed_at range (date inputs)
    if f["start_date"]:
        where.append("executed_at >= %s")
        params.append(f["start_date"] + " 00:00:00")
    if f["end_date"]:
        where.append("executed_at <= %s")
        params.append(f["end_date"] + " 23:59:59")

    # assessment_id (exact from dropdown)
    if f["assessment_id"]:
        where.append("assessment_id = %s")
        params.append(int(f["assessment_id"]))

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

        # LOV for assessments (id + name)
        cur.execute("SELECT assessment_id, name FROM assessments ORDER BY name")
        assessments_lov = cur.fetchall()

        # Count
        cur.execute("SELECT COUNT(*) AS cnt FROM assessment_runs" + where_sql, params)
        total_records = int(cur.fetchone()["cnt"] or 0)

        total_pages = max(1, int(ceil(total_records / PAGE_SIZE))) if total_records else 1
        if page > total_pages:
            page = total_pages

        offset = (page - 1) * PAGE_SIZE

        # NOTE: total/success/fail/error kolonları çıkarıldı.
        sql = (
            "SELECT domain_run_id, run_id, assessment_id, assessment_name, db_type, status, "
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
        assessments_lov=assessments_lov,
        filters=f,
        page=page,
        page_size=PAGE_SIZE,
        total_records=total_records,
        total_pages=total_pages,
    )


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
