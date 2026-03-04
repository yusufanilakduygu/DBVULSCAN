# -*- coding: utf-8 -*-
from flask import Blueprint

analysis_reports_bp = Blueprint(
    "analysis_reports",
    __name__,
    template_folder="../templates",
)

from . import routes  # noqa: E402,F401
from . import routes_change  # noqa: E402,F401