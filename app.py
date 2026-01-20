# -*- coding: utf-8 -*-
import os
from flask import Flask, render_template, redirect, url_for, session
from datetime import timedelta
from config import SECRET_KEY
from auth import auth_bp             # login/logout blueprint
from users import users_bp           # users CRUD blueprint
from db import get_db, get_version_line   # MySQL bağlantısı
from datasources import datasources_bp    # datasources blueprint
from checkpoints import checkpoints_bp    # checkpoints blueprint
from domainusers import domainusers_bp    # domain users blueprint
from settings import settings_bp     # settings CRUD blueprint
from benchmarks import benchmarks_bp
from assessments import assessments_bp
from assessment_runs import assessment_runs_bp
from domains import domains_bp


def get_setting_value(key):
    """
    Settings tablosundan verilen key için setting_value döndürür.
    Hata olursa veya kayıt yoksa None döner.
    """
    con = None
    try:
        con = get_db()
        with con.cursor() as cur:
            cur.execute(
                "SELECT setting_value FROM settings WHERE setting_key=%s LIMIT 1",
                (key,),
            )
            row = cur.fetchone()
            return row["setting_value"] if row else None
    except Exception:
        return None
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass


def create_app():
    app = Flask(__name__)
    app.secret_key = SECRET_KEY
    app.permanent_session_lifetime = timedelta(hours=8)
    app.config["TEMPLATES_AUTO_RELOAD"] = True

    # Kerberos config path'ini settings'ten oku ve ortama yaz
    krb_path = get_setting_value("krb5_config_path")
    if krb_path:
        os.environ["KRB5_CONFIG"] = krb_path
        print("[DBVULSCAN] KRB5_CONFIG env set to:", os.environ.get("KRB5_CONFIG"))
    else:
        print("[DBVULSCAN] WARNING: krb5_config_path not found in settings")

    # Blueprint kayıtları
    app.register_blueprint(auth_bp)
    app.register_blueprint(users_bp)          # url_prefix users/__init__.py içinde
    app.register_blueprint(datasources_bp)    # url_prefix datasources/__init__.py içinde
    app.register_blueprint(checkpoints_bp, url_prefix="/checkpoints")
    app.register_blueprint(domainusers_bp)    # url_prefix domainusers/__init__.py içinde
    app.register_blueprint(settings_bp)
    app.register_blueprint(benchmarks_bp, url_prefix="/benchmarks")
    app.register_blueprint(assessments_bp, url_prefix="/assessments")

    # Domains
    app.register_blueprint(domains_bp, url_prefix="/domains")

    # ✅ KRİTİK: Assessment Runs (Assessment Results) blueprint’i register et
    app.register_blueprint(assessment_runs_bp, url_prefix="/assessment-results")

    # Her şablonda current_user ve current_role otomatik görün (session tabanlı)
    @app.context_processor
    def inject_user():
        u = session.get("user") or {}
        return {
            "current_user": u.get("username", "Guest"),
            "current_role": u.get("role", "viewer"),
        }

    # Ana sayfa
    @app.route("/")
    def home():
        # Login yapılmamışsa login ekranına yönlendir
        if "user" not in session:
            return redirect(url_for("auth.login"))

        version_line = get_version_line()
        return render_template("index.html", version_line=version_line)

    return app


app = create_app()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
