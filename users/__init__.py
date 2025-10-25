# -*- coding: utf-8 -*-
from flask import Blueprint

# Global templates klasörü (templates/users/) kullanılacak; o yüzden template_folder belirtmiyoruz.
users_bp = Blueprint(
    "users",
    __name__,
    url_prefix="/users",
)

# Route'ları kaydet
from . import routes  # noqa: E402, F401
