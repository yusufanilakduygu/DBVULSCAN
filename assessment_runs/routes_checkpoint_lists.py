# -*- coding: utf-8 -*-
from __future__ import annotations

from math import ceil
from typing import Any, Dict, List

from flask import abort, redirect, render_template, request, session, url_for

from db import get_db
from . import assessment_runs_bp

PAGE_SIZE = 10


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
        cur.execute(
            "SELECT run_id, assessment_name, executed_at "
            "FROM assessment_runs WHERE run_id=%s LIMIT 1",
            (run_id,),
        )
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

        # list (FAIL -> ERROR -> PASS)
        cur.execute(
            "SELECT run_checkpoint_id, checkpoint_name, checkpoint_severity, checkpoint_category, test_result "
            "FROM assessment_run_checkpoints"
            + where_sql
            + " ORDER BY "
              "CASE test_result "
                "WHEN 'fail' THEN 1 "
                "WHEN 'error' THEN 2 "
                "WHEN 'pass' THEN 3 "
                "ELSE 9 "
              "END ASC, "
              "run_checkpoint_id ASC "
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
