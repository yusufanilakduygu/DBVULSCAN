# -*- coding: utf-8 -*-
import os
import subprocess

from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from werkzeug.security import check_password_hash, generate_password_hash
from db import get_db, get_version_line

auth_bp = Blueprint("auth", __name__)


def kerberos_authenticate(principal: str, password: str) -> tuple[bool, str]:
    """
    kinit ile Kerberos doğrulaması yapar. Başarılıysa hemen kdestroy çağırır.
    Ticket/cache tutulmaz.
    """
    if not principal:
        return False, "Missing Kerberos principal."

    try:
        env = os.environ.copy()

        # kinit: password stdin ile verilir
        p = subprocess.run(
            ["kinit", principal],
            input=(password + "\n").encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            env=env,
        )

        if p.returncode != 0:
            err = p.stderr.decode("utf-8", errors="ignore").strip()
            return False, (err or "Kerberos authentication failed.")

        # başarılıysa hemen sil
        subprocess.run(
            ["kdestroy"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            env=env,
        )
        return True, ""

    except FileNotFoundError:
        return False, "kinit/kdestroy not found on server."
    except Exception as e:
        return False, f"Kerberos error: {e}"


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    version_line = get_version_line()

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""

        if not username or not password:
            flash("Please enter a username and password.", "warning")
            return render_template("login.html", version_line=version_line)

        con = None
        try:
            con = get_db()
            with con.cursor() as cur:
                cur.execute("""
                    SELECT user_id, username, password_hash, role, status, user_type, principal
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

                user_type = (user.get("user_type") or "local").strip()

                # --- AUTH ---
                if user_type == "AD":
                    principal = (user.get("principal") or "").strip()
                    if not principal:
                        flash("AD user is missing principal.", "danger")
                        return render_template("login.html", version_line=version_line)

                    ok, err = kerberos_authenticate(principal, password)
                    if not ok:
                        flash(f"Kerberos login failed: {err}", "danger")
                        return render_template("login.html", version_line=version_line)

                else:
                    # local
                    if not user.get("password_hash"):
                        flash("Local user is missing password hash.", "danger")
                        return render_template("login.html", version_line=version_line)

                    if not check_password_hash(user["password_hash"], password):
                        flash("Incorrect password.", "danger")
                        return render_template("login.html", version_line=version_line)

                # Success → session + last_login
                cur.execute("UPDATE users SET last_login=NOW() WHERE user_id=%s;", (user["user_id"],))

                session.permanent = True

                # Eski yapıyı koru
                session["user"] = {
                    "user_id": user["user_id"],
                    "username": user["username"],
                    "role": user["role"]
                }

                # Layout/menu için net alanlar
                session["user_id"] = user["user_id"]
                session["username"] = user["username"]
                session["role"] = user["role"]

                return redirect(url_for("home"))

        except Exception as e:
            flash(f"Database error: {e}", "danger")
            return render_template("login.html", version_line=version_line)

        finally:
            try:
                if con:
                    con.close()
            except Exception:
                pass

    # GET: önceki oturumdan kalan flash mesajlarını temizle
    session.pop("_flashes", None)
    return render_template("login.html", version_line=version_line)


@auth_bp.route("/logout")
def logout():
    session.clear()
    flash("Signed out.", "info")
    return redirect(url_for("auth.login"))


@auth_bp.route("/change-password", methods=["GET", "POST"])
def change_password():
    version_line = get_version_line()

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        current_pw = request.form.get("current_password") or ""
        new_pw = request.form.get("new_password") or ""
        confirm_pw = request.form.get("confirm_password") or ""

        if not username or not current_pw or not new_pw or not confirm_pw:
            flash("Please fill in all fields.", "warning")
            return render_template("change_password.html", version_line=version_line)

        if new_pw != confirm_pw:
            flash("New passwords do not match.", "danger")
            return render_template("change_password.html", version_line=version_line)

        if len(new_pw) < 8:
            flash("New password must be at least 8 characters.", "warning")
            return render_template("change_password.html", version_line=version_line)

        con = None
        try:
            con = get_db()
            with con.cursor() as cur:
                cur.execute("""
                    SELECT user_id, username, password_hash, status, user_type
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

                user_type = (user.get("user_type") or "local").strip()
                if user_type == "AD":
                    flash("Password change for AD users must be done in Active Directory.", "warning")
                    return render_template("change_password.html", version_line=version_line)

                if not user.get("password_hash"):
                    flash("Local user is missing password hash.", "danger")
                    return render_template("change_password.html", version_line=version_line)

                if not check_password_hash(user["password_hash"], current_pw):
                    flash("Current password is incorrect.", "danger")
                    return render_template("change_password.html", version_line=version_line)

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
                if con:
                    con.close()
            except Exception:
                pass

    session.pop("_flashes", None)
    return render_template("change_password.html", version_line=version_line)
