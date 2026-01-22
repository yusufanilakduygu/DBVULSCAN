# -*- coding: utf-8 -*-

from flask import flash, redirect, render_template, request, session, url_for

from db import get_db

from . import domains_bp


def _get_persisted(key: str, default: str = "") -> str:
    """Get a query param and persist it in session."""
    if key in request.args:
        val = request.args.get(key, default) or ""
        session[f"domains_{key}"] = val
        return val
    return session.get(f"domains_{key}", default) or ""


@domains_bp.route("/")
def list_domains():
    if request.args.get("reset") == "1":
        session.pop("domains_q", None)
        session.pop("domains_active", None)
        return redirect(url_for("domains.list_domains"))

    search = _get_persisted("q", "")
    f_active = _get_persisted("active", "")

    conditions = []
    params = []
    if search:
        conditions.append("name LIKE %s")
        params.append(f"%{search}%")

    if f_active in ("1", "0"):
        conditions.append("is_active = %s")
        params.append(int(f_active))

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    db = get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT domain_id, name, description, is_active, created_at, updated_at
        FROM domains
        {where_clause}
        ORDER BY updated_at DESC, created_at DESC
        """,
        params,
    )
    domains = cur.fetchall()

    return render_template(
        "domains/list.html",
        domains=domains,
        search=search,
        f_active=f_active,
    )


@domains_bp.route("/new", methods=["GET", "POST"])
def new_domain():
    db = get_db()
    cur = db.cursor()

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        description = request.form.get("description")
        is_active = 1 if request.form.get("is_active") == "1" else 0

        if not name:
            flash("Name is required.", "error")
            return redirect(url_for("domains.new_domain"))

        try:
            cur.execute(
                """
                INSERT INTO domains (name, description, is_active)
                VALUES (%s, %s, %s)
                """,
                (name, description, is_active),
            )
            db.commit()
            flash("Domain created.", "success")
            return redirect(url_for("domains.list_domains"))
        except Exception as e:
            db.rollback()
            flash(f"Create failed: {e}", "error")

    return render_template("domains/form.html", domain=None)


@domains_bp.route("/edit/<int:domain_id>", methods=["GET", "POST"])
def edit_domain(domain_id: int):
    db = get_db()
    cur = db.cursor()

    cur.execute(
        "SELECT domain_id, name, description, is_active, created_at, updated_at FROM domains WHERE domain_id=%s",
        (domain_id,),
    )
    domain = cur.fetchone()
    if not domain:
        flash("Domain not found.", "error")
        return redirect(url_for("domains.list_domains"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        description = request.form.get("description")
        is_active = 1 if request.form.get("is_active") == "1" else 0

        if not name:
            flash("Name is required.", "error")
            return redirect(url_for("domains.edit_domain", domain_id=domain_id))

        try:
            cur.execute(
                """
                UPDATE domains
                SET name=%s,
                    description=%s,
                    is_active=%s,
                    updated_at=NOW()
                WHERE domain_id=%s
                """,
                (name, description, is_active, domain_id),
            )
            db.commit()
            flash("Domain updated.", "success")
            return redirect(url_for("domains.list_domains"))
        except Exception as e:
            db.rollback()
            flash(f"Update failed: {e}", "error")

    return render_template("domains/form.html", domain=domain)