# -*- coding: utf-8 -*-
from flask import render_template, request, redirect, url_for, flash, session
from werkzeug.security import generate_password_hash
import pymysql
import datetime
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
            search = (session.get("users_search") or "").strip()

        if raw_page is not None:
            page = max(raw_page, 1)
            session["users_page"] = page
        else:
            page = max(int(session.get("users_page") or 1), 1)

    per_page = 10
    offset = (page - 1) * per_page

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
            OR  user_type  LIKE %s
            OR  principal  LIKE %s
          )
        """
        params.extend([like, like, like, like, like, like, like])

    count_sql = "SELECT COUNT(*) AS total " + base_sql

    data_sql = """
        SELECT
              user_id,
              username,
              user_type,
              principal,
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

    with get_db().cursor() as cur:
        cur.execute(count_sql, params)
        row_cnt = cur.fetchone()
        total = row_cnt["total"] if row_cnt else 0

        cur.execute(data_sql, params_data)
        users = cur.fetchall()

    total_pages = max((total + per_page - 1) // per_page, 1)

    return render_template(
        "users/list.html",
        users=users,
        search=search,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        current_role=session.get("role", "viewer"),
    )


# CREATE
@users_bp.route("/new", methods=["GET", "POST"])
@login_required
@admin_required
def create_user():
    if request.method == "POST":
        f = request.form
        username = (f.get("username") or "").strip()
        user_type = f.get("user_type", "local")
        principal = (f.get("principal") or "").strip() or None
        password = (f.get("password") or "").strip()
        full_name = (f.get("full_name") or "").strip() or None
        email = (f.get("email") or "").strip() or None
        role = f.get("role", "viewer")
        status = f.get("status", "active")

        if not username:
            flash("Username is required.", "warning")
            return redirect(url_for("users.create_user"))

        if user_type == "AD":
            if not principal:
                flash("Principal is required for AD users.", "warning")
                return redirect(url_for("users.create_user"))
            pwd_hash = None
        else:
            if not password:
                flash("Password is required for local users.", "warning")
                return redirect(url_for("users.create_user"))
            pwd_hash = generate_password_hash(password)
            principal = None

        try:
            with get_db().cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users
                        (username, user_type, principal, password_hash, full_name, email, role, status, passwd_change_date)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (username, user_type, principal, pwd_hash, full_name, email, role, status,
                     (datetime.datetime.now() if user_type != 'AD' else None)),
                )
            flash("User created.", "success")
            return redirect(url_for("users.list_users"))
        except pymysql.err.IntegrityError as e:
            flash(f"Cannot create user: {str(e)}", "danger")
            return redirect(url_for("users.create_user"))

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
        user_type = f.get("user_type", row.get("user_type") or "local")
        principal = (f.get("principal") or "").strip() or None
        full_name = (f.get("full_name") or "").strip() or None
        email = (f.get("email") or "").strip() or None
        role = f.get("role", row["role"])
        status = f.get("status", row["status"])
        new_password = (f.get("password") or "").strip()

        if user_type == "AD" and not principal:
            flash("Principal is required for AD users.", "warning")
            return redirect(url_for("users.edit_user", user_id=user_id))

        if user_type != "AD":
            principal = None

        if user_type != "AD" and (row.get("user_type") == "AD" or not row.get("password_hash")) and not new_password:
            flash("Password is required when switching to local user.", "warning")
            return redirect(url_for("users.edit_user", user_id=user_id))

        try:
            with get_db().cursor() as cur:
                if user_type == "AD":
                    cur.execute(
                        """
                        UPDATE users
                           SET user_type=%s,
                               principal=%s,
                               full_name=%s,
                               email=%s,
                               role=%s,
                               status=%s,
                               password_hash=NULL
                         WHERE user_id=%s
                        """,
                        (user_type, principal, full_name, email, role, status, user_id),
                    )
                else:
                    if new_password:
                        pwd_hash = generate_password_hash(new_password)
                        cur.execute(
                            """
                            UPDATE users
                               SET user_type=%s,
                                   principal=NULL,
                                   full_name=%s,
                                   email=%s,
                                   role=%s,
                                   status=%s,
                                   password_hash=%s,
                                   passwd_change_date=NOW()
                             WHERE user_id=%s
                            """,
                            (user_type, full_name, email, role, status, pwd_hash, user_id),
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE users
                               SET user_type=%s,
                                   principal=NULL,
                                   full_name=%s,
                                   email=%s,
                                   role=%s,
                                   status=%s
                             WHERE user_id=%s
                            """,
                            (user_type, full_name, email, role, status, user_id),
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
