# -*- coding: utf-8 -*-
from flask import render_template, request, redirect, url_for, flash, session
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
    # ---- Search & pagination (session persistent) ----
    raw_search = request.args.get("search")
    raw_page = request.args.get("page", type=int)
    clear = request.args.get("clear")

    if clear:
        session.pop("users_search", None)
        session.pop("users_page", None)
        search = ""
        page = 1
    else:
        if raw_search is not None:
            search = (raw_search or "").strip()
            session["users_search"] = search
            page = 1
        else:
            search = session.get("users_search", "")

        if raw_page is not None:
            page = raw_page
        else:
            page = session.get("users_page", 1)

    if page is None or page < 1:
        page = 1

    per_page = 10

    base_sql = "FROM users WHERE 1=1"
    params = []

    if search:
        like = f"%{search}%"
        base_sql += """
          AND (
                username   LIKE %s
            OR  full_name LIKE %s
            OR  email     LIKE %s
            OR  role      LIKE %s
            OR  status    LIKE %s
          )
        """
        params.extend([like, like, like, like, like])

    count_sql = "SELECT COUNT(*) AS total " + base_sql

    with get_db().cursor() as cur:
        # toplam kayıt
        cur.execute(count_sql, params)
        row_cnt = cur.fetchone()
        total = row_cnt["total"] if row_cnt else 0

        pages = (total + per_page - 1) // per_page if total else 1
        if page > pages:
            page = pages

        session["users_page"] = page

        offset = (page - 1) * per_page

        data_sql = """
            SELECT
                user_id,
                username,
                full_name,
                email,
                role,
                status,
                last_login,
                passwd_change_date
        """ + base_sql + """
            ORDER BY user_id DESC
            LIMIT %s OFFSET %s
        """

        params_data = list(params)
        params_data.extend([per_page, offset])

        cur.execute(data_sql, params_data)
        rows = cur.fetchall()

    # Global templates: templates/users/list.html
    return render_template(
        "users/list.html",
        users=rows,
        page=page,
        pages=pages,
        per_page=per_page,
        total=total,
        search=search,
    )


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
