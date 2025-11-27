from flask import Blueprint

# template_folder vermiyoruz; app zaten /templates'i biliyor
checkpoints_bp = Blueprint('checkpoints', __name__)

from . import routes