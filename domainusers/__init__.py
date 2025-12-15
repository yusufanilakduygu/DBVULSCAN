# -*- coding: utf-8 -*-
from flask import Blueprint

domainusers_bp = Blueprint(
    "domainusers",
    __name__,
    url_prefix="/domain-users",
)

from . import routes  # noqa: E402, F401
