# -*- coding: utf-8 -*-
from flask import render_template
from . import analysis_reports_bp


@analysis_reports_bp.route("/", methods=["GET"])
def index():
    return render_template("analysis_reports/index.html")


# Şimdilik placeholder endpoint'ler (butonlar 404 olmasın)
@analysis_reports_bp.route("/fail-analysis", methods=["GET"])
def fail_analysis():
    return render_template("analysis_reports/placeholder.html", title="Fail Analysis")


@analysis_reports_bp.route("/monthly-reports", methods=["GET"])
def monthly_reports():
    return render_template("analysis_reports/placeholder.html", title="Monthly Reports")