# -*- coding: utf-8 -*-
from flask import Blueprint

jobs_bp = Blueprint("jobs", __name__, template_folder="../templates/jobs")

from . import routes  # noqa: E402,F401
from . import routes_email  # noqa: E402,F401
