# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from werkzeug.security import check_password_hash
from db import get_db, get_version_line

auth_bp = Blueprint("auth", __name__)

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    version_line = get_version_line()

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""

        if not username or not password:
            flash("Please enter a username and password.", "warning")
            return render_template("login.html", version_line=version_line)

        try:
            con = get_db()
            with con.cursor() as cur:
                # Sağlamlaştırma: trim + lower ile eşleşme istersen burayı değiştirebilirsin
                cur.execute("""
                    SELECT user_id, username, password_hash, role, status
                    FROM users
                    WHERE username=%s
                    LIMIT 1;
                """, (username,))
                user = cur.fetchone()

                if not user:
                    flash("User not found.", "danger")
                    return render_template("login.html", version_line=version_line)

                if user["status"] != "active":
                    if user["status"] == "inactive":
                        flash("Account is inactive. Please contact your administrator.", "danger")
                    elif user["status"] == "locked":
                        flash("Account is locked. Please contact your administrator.", "danger")
                    else:
                        flash("Invalid account status.", "danger")
                    return render_template("login.html", version_line=version_line)

                if not check_password_hash(user["password_hash"], password):
                    flash("Incorrect password.", "danger")
                    return render_template("login.html", version_line=version_line)

                # Success → session + last_login
                cur.execute("UPDATE users SET last_login=NOW() WHERE user_id=%s;", (user["user_id"],))
                session.permanent = True
                session["user"] = {
                    "user_id": user["user_id"],
                    "username": user["username"],
                    "role": user["role"]
                }
                return redirect(url_for("home"))

        except Exception as e:
            flash(f"Database error: {e}", "danger")
            return render_template("login.html", version_line=version_line)
        finally:
            try:
                con.close()
            except Exception:
                pass

    # GET: önceki oturumdan kalan flash mesajlarını temizle (ilk açılışta "User not found" vs. görünmesin)
    session.pop("_flashes", None)
    return render_template("login.html", version_line=version_line)

@auth_bp.route("/logout")
def logout():
    session.clear()
    flash("Signed out.", "info")
    return redirect(url_for("auth.login"))

from werkzeug.security import check_password_hash, generate_password_hash
# ... auth_bp, login, logout mevcut ...

@auth_bp.route("/change-password", methods=["GET", "POST"])
def change_password():
    # Versiyon satırını al (logo altında göstermek için)
    version_line = get_version_line()

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        current_pw = request.form.get("current_password") or ""
        new_pw = request.form.get("new_password") or ""
        confirm_pw = request.form.get("confirm_password") or ""

        # Basit kontroller
        if not username or not current_pw or not new_pw or not confirm_pw:
            flash("Please fill in all fields.", "warning")
            return render_template("change_password.html", version_line=version_line)

        if new_pw != confirm_pw:
            flash("New passwords do not match.", "danger")
            return render_template("change_password.html", version_line=version_line)

        # Basit parola politikası örneği (istersen genişletiriz)
        if len(new_pw) < 8:
            flash("New password must be at least 8 characters.", "warning")
            return render_template("change_password.html", version_line=version_line)

        try:
            con = get_db()
            with con.cursor() as cur:
                cur.execute("""
                    SELECT user_id, username, password_hash, status
                    FROM users
                    WHERE username=%s
                    LIMIT 1;
                """, (username,))
                user = cur.fetchone()

                if not user:
                    flash("User not found.", "danger")
                    return render_template("change_password.html", version_line=version_line)

                if user["status"] != "active":
                    flash("Account is not active.", "danger")
                    return render_template("change_password.html", version_line=version_line)

                if not check_password_hash(user["password_hash"], current_pw):
                    flash("Current password is incorrect.", "danger")
                    return render_template("change_password.html", version_line=version_line)

                # Hash'i PBKDF2-SHA256 ile üret
                new_hash = generate_password_hash(new_pw, method="pbkdf2:sha256", salt_length=16)

                cur.execute("""
                    UPDATE users
                    SET password_hash=%s, passwd_change_date=NOW()
                    WHERE user_id=%s
                """, (new_hash, user["user_id"]))

                flash("Password changed successfully. Please sign in with your new password.", "info")
                return redirect(url_for("auth.login"))

        except Exception as e:
            flash(f"Database error: {e}", "danger")
            return render_template("change_password.html", version_line=version_line)
        finally:
            try:
                con.close()
            except Exception:
                pass

    # GET: eski flash mesajı kalmasın
    session.pop("_flashes", None)
    return render_template("change_password.html", version_line=version_line)
