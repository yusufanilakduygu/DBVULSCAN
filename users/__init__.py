# -*- coding: utf-8 -*-
from flask import Blueprint

# Global blueprint
users_bp = Blueprint("users", __name__, url_prefix="/users")

# Route'ları kaydet
from . import routes  # noqa: E402, F401
