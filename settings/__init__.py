# -*- coding: utf-8 -*-
from flask import Blueprint

settings_bp = Blueprint(
    "settings",
    __name__,
    url_prefix="/settings",
)

from . import routes  # noqa: E402, F401
