# -*- coding: utf-8 -*-
from flask import render_template, request, redirect, url_for, flash
from werkzeug.security import generate_password_hash
import pymysql
from db import get_db
from security import login_required, admin_required
from . import users_bp


# LIST
@users_bp.route("/", methods=["GET"])
@login_required
@admin_required
def list_users():
    with get_db().cursor() as cur:
        cur.execute(
            """
            SELECT user_id, username, full_name, email, role, status,
                   last_login, passwd_change_date
            FROM users
            ORDER BY user_id DESC
            """
        )
        rows = cur.fetchall()
    # Global templates: templates/users/list.html
    return render_template("users/list.html", rows=rows)


# CREATE
@users_bp.route("/create", methods=["GET", "POST"])
@login_required
@admin_required
def create_user():
    if request.method == "POST":
        f = request.form
        username = (f.get("username") or "").strip()
        password = (f.get("password") or "").strip()
        full_name = (f.get("full_name") or "").strip() or None
        email = (f.get("email") or "").strip() or None
        role = f.get("role", "viewer")
        status = f.get("status", "active")

        if not username or not password:
            flash("Username and Password are required.", "warning")
            return redirect(url_for("users.create_user"))

        pwd_hash = generate_password_hash(password)

        try:
            with get_db().cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users
                        (username, password_hash, full_name, email, role, status, passwd_change_date)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (username, pwd_hash, full_name, email, role, status),
                )
            flash("User created.", "success")
            return redirect(url_for("users.list_users"))
        except pymysql.err.IntegrityError as e:
            # UNIQUE(username) vb. hatalar
            flash(f"Cannot create user: {str(e)}", "danger")
            return redirect(url_for("users.create_user"))

    # Global templates: templates/users/form.html
    return render_template("users/form.html", mode="create", row=None)


# UPDATE
@users_bp.route("/<int:user_id>/edit", methods=["GET", "POST"])
@login_required
@admin_required
def edit_user(user_id: int):
    with get_db().cursor() as cur:
        cur.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))
        row = cur.fetchone()

    if not row:
        flash("User not found.", "warning")
        return redirect(url_for("users.list_users"))

    if request.method == "POST":
        f = request.form
        full_name = (f.get("full_name") or "").strip() or None
        email = (f.get("email") or "").strip() or None
        role = f.get("role", row["role"])
        status = f.get("status", row["status"])
        new_password = (f.get("password") or "").strip()

        try:
            with get_db().cursor() as cur:
                if new_password:
                    pwd_hash = generate_password_hash(new_password)
                    cur.execute(
                        """
                        UPDATE users
                           SET full_name=%s,
                               email=%s,
                               role=%s,
                               status=%s,
                               password_hash=%s,
                               passwd_change_date=NOW()
                         WHERE user_id=%s
                        """,
                        (full_name, email, role, status, pwd_hash, user_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE users
                           SET full_name=%s,
                               email=%s,
                               role=%s,
                               status=%s
                         WHERE user_id=%s
                        """,
                        (full_name, email, role, status, user_id),
                    )
            flash("User updated.", "success")
            return redirect(url_for("users.list_users"))
        except pymysql.MySQLError as e:
            flash(f"Update failed: {str(e)}", "danger")
            return redirect(url_for("users.edit_user", user_id=user_id))

    return render_template("users/form.html", mode="edit", row=row)


# DELETE (hard delete)
@users_bp.route("/<int:user_id>/delete", methods=["POST"])
@login_required
@admin_required
def delete_user(user_id: int):
    try:
        with get_db().cursor() as cur:
            cur.execute("DELETE FROM users WHERE user_id=%s", (user_id,))
        flash("User deleted.", "info")
    except pymysql.MySQLError as e:
        flash(f"Delete failed: {str(e)}", "danger")

    return redirect(url_for("users.list_users"))
