# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import abort, render_template

from db import get_db
from . import assessment_runs_bp


@assessment_runs_bp.route("/<int:run_id>/checkpoints/<int:run_checkpoint_id>", methods=["GET"])
def checkpoint_detail(run_id: int, run_checkpoint_id: int):
    db = get_db()
    try:
        cur = db.cursor()

        cur.execute(
            "SELECT run_id, assessment_name, executed_at "
            "FROM assessment_runs WHERE run_id=%s LIMIT 1",
            (run_id,),
        )
        run = cur.fetchone()

        cur.execute(
            "SELECT * FROM assessment_run_checkpoints "
            "WHERE run_id=%s AND run_checkpoint_id=%s LIMIT 1",
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
