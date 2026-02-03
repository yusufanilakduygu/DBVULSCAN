# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import abort, redirect, url_for, send_file

from . import assessment_runs_bp
from report_service.report_assessment_summary import generate


# -----------------------------
# REPORT: Assessment Run Summary (PDF only)
# -----------------------------
@assessment_runs_bp.route("/<int:run_id>/report", methods=["GET"])
def report_assessment_run_summary(run_id: int):
    """
    PDF-only endpoint.
    - Cache: settings.report_dir/assessment_run_{run_id}_summary.pdf
    - Eğer dosya yoksa report_service.generate() üretir.
    """
    try:
        pdf_path = generate(run_id, force=False)
    except RuntimeError:
        abort(404)

    return send_file(
        pdf_path,
        mimetype="application/pdf",
        as_attachment=False,
        download_name=f"assessment_run_summary_run_{run_id}.pdf",
    )


# Optional: keep .pdf endpoint but make it point to /report
@assessment_runs_bp.route("/<int:run_id>/report.pdf", methods=["GET"])
def report_assessment_run_summary_pdf(run_id: int):
    return redirect(url_for("assessment_runs.report_assessment_run_summary", run_id=run_id))
