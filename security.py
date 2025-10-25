# -*- coding: utf-8 -*-
from functools import wraps
from flask import session, redirect, url_for, flash

def login_required(f):
    @wraps(f)
    def w(*a, **kw):
        if "user" not in session:
            flash("Please sign in first.", "warning")
            return redirect(url_for("auth.login"))
        return f(*a, **kw)
    return w

def admin_required(f):
    @wraps(f)
    def w(*a, **kw):
        role = (session.get("user") or {}).get("role")
        if role != "admin":
            flash("Admins only.", "danger")
            return redirect(url_for("home"))  # app.py'deki @app.route("/") adı home
        return f(*a, **kw)
    return w
