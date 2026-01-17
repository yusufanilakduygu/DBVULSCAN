# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

from flask import abort, render_template, request

from db import get_db
from . import assessment_runs_bp


@assessment_runs_bp.route("/<int:run_id>/metrics", methods=["GET"])
def metrics_list(run_id: int):
    tab = (request.args.get("tab", "general") or "general").strip().lower()
    if tab not in ("general", "category"):
        tab = "general"

    db = get_db()
    try:
        cur = db.cursor()

        # Run header
        cur.execute(
            "SELECT run_id, assessment_name, executed_at "
            "FROM assessment_runs WHERE run_id=%s LIMIT 1",
            (run_id,),
        )
        run = cur.fetchone()
        if not run:
            abort(404)

        # Main metrics
        cur.execute(
            "SELECT run_metric_id, dimension_type, dimension_value, "
            "total_count, success_count, fail_count, error_count, "
            "success_pct, fail_pct, error_pct, "
            "risk, risk_level, asset_adjusted_risk, asset_adjusted_risk_level, "
            "severity_sum, failed_severity_sum, executed_at "
            "FROM assessment_run_metrics "
            "WHERE run_id=%s "
            "ORDER BY dimension_type ASC, dimension_value ASC",
            (run_id,),
        )
        rows = cur.fetchall()

        # Category x Severity matrix
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

    # Build category x severity matrix
    cat_matrix: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for cr in cat_rows or []:
        c = cr.get("category")
        s = cr.get("severity")
        if not c or not s:
            continue
        cat_matrix.setdefault(str(c), {})[str(s)] = cr

    # Risk map (category dimension)
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
