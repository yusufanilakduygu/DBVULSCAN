from flask import Blueprint

benchmarks_bp = Blueprint('benchmarks', __name__)

from . import routes
