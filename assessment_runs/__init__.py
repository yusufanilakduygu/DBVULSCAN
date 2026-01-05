# -*- coding: utf-8 -*-
from flask import Blueprint

assessment_runs_bp = Blueprint("assessment_runs", __name__)

from . import routes  # noqa: E402,F401
