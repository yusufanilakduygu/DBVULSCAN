# -*- coding: utf-8 -*-
from flask import Blueprint

# Domains blueprint
# URL prefix is registered in app.py as /domains

domains_bp = Blueprint("domains", __name__, template_folder="../templates")

from . import routes  # noqa: E402,F401