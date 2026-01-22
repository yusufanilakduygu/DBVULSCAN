# -*- coding: utf-8 -*-
from flask import Blueprint

# Domains blueprint
# URL prefix is registered in app.py as /domains

domains_bp = Blueprint("domains", __name__, template_folder="../templates")

from . import routes  # noqa: E402,F401
from . import routes_domain_run  # noqa: E402,F401
from . import routes_domain_edit  # noqa: E402,F401
