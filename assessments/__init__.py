# -*- coding: utf-8 -*-
from flask import Blueprint


assessments_bp = Blueprint("assessments", __name__)


from . import routes  # noqa: E402,F401
