# -*- coding: utf-8 -*-
from flask import render_template, request, redirect, url_for, flash, session
import pymysql
import subprocess
import os

from db import get_db
from security import login_required, admin_required
from . import domainusers_bp


# LIST
@domainusers_bp.route("/", methods=["GET"])
@login_required
@admin_required
def list_domain_users():
    # ---- Search & pagination (session persistent) ----
    raw_search = request.args.get("search")
    raw_page = request.args.get("page", type=int)
    clear = request.args.get("clear")

    if clear:
        session.pop("domainusers_search", None)
        session.pop("domainusers_page", None)
        search = ""
        page = 1
    else:
        if raw_search is not None:
            search = (raw_search or "").strip()
            session["domainusers_search"] = search
            page = 1
        else:
            search = session.get("domainusers_search", "")

        if raw_page is not None:
            page = raw_page
        else:
            page = session.get("domainusers_page", 1)

    if not page or page < 1:
        page = 1

    per_page = 10  # Sayfada max 10 kayıt

    base_sql = "FROM domain_users WHERE 1=1"
    params = []

    if search:
        like = f"%{search}%"
        base_sql += """
          AND (
                name            LIKE %s
            OR  domain_username LIKE %s
            OR  domain_fqdn     LIKE %s
            OR  IFNULL(netbios_name,'') LIKE %s
          )
        """
        params.extend([like, like, like, like])

    count_sql = "SELECT COUNT(*) AS total " + base_sql

    with get_db().cursor() as cur:
        cur.execute(count_sql, params)
        row_cnt = cur.fetchone()
        total = row_cnt["total"] if row_cnt else 0

        pages = (total + per_page - 1) // per_page if total else 1
        if page > pages and pages > 0:
            page = pages

        session["domainusers_page"] = page

        offset = (page - 1) * per_page

        data_sql = (
            "SELECT id, name, domain_username, domain_fqdn, netbios_name, "
            "is_active, created_at, updated_at "
            + base_sql +
            " ORDER BY name ASC "
            " LIMIT %s OFFSET %s"
        )
        params_with_paging = list(params) + [per_page, offset]
        cur.execute(data_sql, params_with_paging)
        rows = cur.fetchall() or []

    return render_template(
        "domainusers/list.html",
        items=rows,
        page=page,
        pages=pages if total else 1,
        per_page=per_page,
        total=total,
        search=search,
    )


# CREATE
@domainusers_bp.route("/create", methods=["GET", "POST"])
@login_required
@admin_required
def create_domain_user():
    if request.method == "POST":
        f = request.form
        name = (f.get("name") or "").strip()
        domain_username = (f.get("domain_username") or "").strip()
        domain_fqdn = (f.get("domain_fqdn") or "").strip()
        netbios_name = (f.get("netbios_name") or "").strip() or None
        password = (f.get("password") or "").strip()
        is_active_str = f.get("is_active", "1")

        if not name or not domain_username or not domain_fqdn:
            flash("Name, Domain Username and Domain FQDN are required.", "warning")
            return redirect(url_for("domainusers.create_domain_user"))

        if not password:
            flash("Password is required.", "warning")
            return redirect(url_for("domainusers.create_domain_user"))

        is_active = 1 if is_active_str == "1" else 0

        # Şimdilik şifreyi VARBINARY kolonda UTF-8 byte olarak tutuyoruz.
        password_enc = password.encode("utf-8")

        user = session.get("user") or {}
        user_id = user.get("user_id")

        try:
            with get_db().cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO domain_users
                        (name, domain_username, domain_fqdn, netbios_name,
                         password_enc, is_active, created_by, updated_by)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (name, domain_username, domain_fqdn, netbios_name,
                     password_enc, is_active, user_id, user_id),
                )
            flash("Domain user created.", "success")
            return redirect(url_for("domainusers.list_domain_users"))
        except pymysql.MySQLError as e:
            flash(f"Create failed: {str(e)}", "danger")
            return redirect(url_for("domainusers.create_domain_user"))

    # GET
    return render_template("domainusers/form.html", mode="create", row=None)


# UPDATE + KINIT TEST
@domainusers_bp.route("/<int:domain_user_id>/edit", methods=["GET", "POST"])
@login_required
@admin_required
def edit_domain_user(domain_user_id: int):
    with get_db().cursor() as cur:
        cur.execute("SELECT * FROM domain_users WHERE id=%s", (domain_user_id,))
        row = cur.fetchone()

    if not row:
        flash("Domain user not found.", "warning")
        return redirect(url_for("domainusers.list_domain_users"))

    if request.method == "POST":
        f = request.form
        action = f.get("action", "save")

        name = (f.get("name") or "").strip()
        domain_username = (f.get("domain_username") or "").strip()
        domain_fqdn = (f.get("domain_fqdn") or "").strip()
        netbios_name = (f.get("netbios_name") or "").strip() or None
        password = (f.get("password") or "").strip()
        is_active_str = f.get("is_active", str(row.get("is_active", 1)))
        is_active = 1 if is_active_str == "1" else 0

        if not name or not domain_username or not domain_fqdn:
            flash("Name, Domain Username and Domain FQDN are required.", "warning")
            # Formu güncel değerlerle tekrar göster
            form_row = dict(row)
            form_row.update({
                "name": name,
                "domain_username": domain_username,
                "domain_fqdn": domain_fqdn,
                "netbios_name": netbios_name,
                "is_active": is_active,
            })
            return render_template("domainusers/form.html", mode="edit", row=form_row)

        # ---- Sadece kinit testi istenmişse ----
        if action == "test":
            # Şifre: önce formdan, yoksa DB'den çözmeyi dener (şu an utf-8 plaintext bytes)
            if password:
                pwd_for_test = password
            else:
                enc = row.get("password_enc")
                pwd_for_test = None
                if isinstance(enc, (bytes, bytearray)):
                    try:
                        pwd_for_test = enc.decode("utf-8")
                    except Exception:
                        pwd_for_test = None

            if not pwd_for_test:
                flash("No password available for kinit test. Please enter password.", "warning")
            else:
                realm = (domain_fqdn or "").upper()
                principal = f"{domain_username}@{realm}" if realm else domain_username

                try:
                    env = os.environ.copy()
                    # İstersen burada KRB5CCNAME'i geçici memory/file cache'e alabilirsin.
                    proc = subprocess.run(
                        ["kinit", principal],
                        input=(pwd_for_test + "\n").encode("utf-8"),
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        timeout=10,
                        env=env,
                    )
                    if proc.returncode == 0:
                        flash(f"kinit successful for principal {principal}.", "success")
                    else:
                        err = (proc.stderr or b"").decode("utf-8", errors="ignore").strip()
                        if not err:
                            err = "Unknown error"
                        flash(f"kinit failed for {principal}: {err}", "danger")
                except Exception as e:
                    flash(f"kinit execution error: {e}", "danger")

            # Kayıt DB'de değiştirilmez, form sadece güncel değerlerle yeniden çizilir
            form_row = dict(row)
            form_row.update({
                "name": name,
                "domain_username": domain_username,
                "domain_fqdn": domain_fqdn,
                "netbios_name": netbios_name,
                "is_active": is_active,
            })
            return render_template("domainusers/form.html", mode="edit", row=form_row)

        # ---- Normal SAVE akışı ----
        user = session.get("user") or {}
        user_id = user.get("user_id")

        try:
            with get_db().cursor() as cur:
                if password:
                    password_enc = password.encode("utf-8")
                    cur.execute(
                        """
                        UPDATE domain_users
                           SET name=%s,
                               domain_username=%s,
                               domain_fqdn=%s,
                               netbios_name=%s,
                               password_enc=%s,
                               is_active=%s,
                               updated_by=%s
                         WHERE id=%s
                        """,
                        (
                            name,
                            domain_username,
                            domain_fqdn,
                            netbios_name,
                            password_enc,
                            is_active,
                            user_id,
                            domain_user_id,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE domain_users
                           SET name=%s,
                               domain_username=%s,
                               domain_fqdn=%s,
                               netbios_name=%s,
                               is_active=%s,
                               updated_by=%s
                         WHERE id=%s
                        """,
                        (
                            name,
                            domain_username,
                            domain_fqdn,
                            netbios_name,
                            is_active,
                            user_id,
                            domain_user_id,
                        ),
                    )
            flash("Domain user updated.", "success")
            return redirect(url_for("domainusers.list_domain_users"))
        except pymysql.MySQLError as e:
            flash(f"Update failed: {str(e)}", "danger")
            return redirect(url_for("domainusers.edit_domain_user", domain_user_id=domain_user_id))

    # GET
    return render_template("domainusers/form.html", mode="edit", row=row)
