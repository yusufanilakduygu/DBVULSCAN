# -*- coding: utf-8 -*-
from flask import Flask, render_template, redirect, url_for, session
from datetime import timedelta
from config import SECRET_KEY
from auth import auth_bp           # login/logout blueprint
from users import users_bp         # users CRUD blueprint
from db import get_db              # MySQL bağlantısı

def create_app():
    app = Flask(__name__)
    app.secret_key = SECRET_KEY
    app.permanent_session_lifetime = timedelta(hours=8)
    app.config["TEMPLATES_AUTO_RELOAD"] = True

    # Blueprint kayıtları
    app.register_blueprint(auth_bp)
    app.register_blueprint(users_bp)   # url_prefix users/__init__.py içinde zaten var

    # Her şablonda current_user ve current_role otomatik görünsün
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

        # Versions tablosundan verileri al
        try:
            with get_db().cursor() as cur:
                cur.execute("SELECT line FROM versions")
                versions = [r["line"] for r in cur.fetchall()]
        except Exception as e:
            versions = [f"Version info unavailable ({e})"]

        return render_template(
            "index.html",
            user=session["user"],
            versions=versions
        )

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, host="0.0.0.0", port=5000)
