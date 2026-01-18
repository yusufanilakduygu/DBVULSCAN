# -*- coding: utf-8 -*-
from flask import Blueprint

assessment_runs_bp = Blueprint("assessment_runs", __name__)

from . import routes  # noqa: E402,F401
from . import routes_checkpoint_lists  
from . import routes_checkpoint_details 
from . import routes_metrics # noqa: E402,F401
from . import routes_report_detail  # noqa: E402,F401
from . import routes_report_summary