# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import abort, send_file

from . import assessment_runs_bp
from report_service.report_assessment_detail import generate


@assessment_runs_bp.route("/<int:run_id>/report_detail", methods=["GET"])
def report_assessment_run_detail(run_id: int):
    """
    Detail PDF endpoint.
    """
    try:
        pdf_path = generate(run_id, force=False)
    except RuntimeError:
        abort(404)

    return send_file(
        pdf_path,
        mimetype="application/pdf",
        as_attachment=False,
        download_name=f"assessment_run_detail_run_{run_id}.pdf",
    )
