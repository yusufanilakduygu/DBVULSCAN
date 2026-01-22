# -*- coding: utf-8 -*-
from flask import render_template, request, redirect, url_for, flash, session
import pymysql
from db import get_db
from security import login_required, admin_required
from . import settings_bp
import smtplib
from email.message import EmailMessage


# LIST
@settings_bp.route("/", methods=["GET"])
@login_required
@admin_required
def list_settings():
    # ---- Search & pagination (session persistent) ----
    raw_search = request.args.get("search")
    raw_page = request.args.get("page", type=int)
    clear = request.args.get("clear")

    if clear:
        session.pop("settings_search", None)
        session.pop("settings_page", None)
        search = ""
        page = 1
    else:
        if raw_search is not None:
            search = (raw_search or "").strip()
            session["settings_search"] = search
            page = 1
        else:
            search = session.get("settings_search", "")

        if raw_page is not None:
            page = raw_page
        else:
            page = session.get("settings_page", 1)

    if not page or page < 1:
        page = 1

    per_page = 10  # sayfada max 10 kayıt

    base_sql = "FROM settings WHERE 1=1"
    params = []

    if search:
        like = f"%{search}%"
        base_sql += """
          AND (
                setting_key   LIKE %s
            OR  setting_value LIKE %s
            OR  IFNULL(description,'') LIKE %s
          )
        """
        params.extend([like, like, like])

    count_sql = "SELECT COUNT(*) AS total " + base_sql

    with get_db().cursor() as cur:
        # toplam kayıt
        cur.execute(count_sql, params)
        row_cnt = cur.fetchone()
        total = row_cnt["total"] if row_cnt else 0

        pages = (total + per_page - 1) // per_page if total else 1
        if page > pages and pages > 0:
            page = pages

        session["settings_page"] = page

        offset = (page - 1) * per_page

        data_sql = (
            "SELECT setting_key, setting_value, value_type, description, "
            "created_at, updated_at "
            + base_sql +
            " ORDER BY setting_key ASC "
            " LIMIT %s OFFSET %s"
        )
        params_with_paging = list(params) + [per_page, offset]
        cur.execute(data_sql, params_with_paging)
        rows = cur.fetchall() or []

    return render_template(
        "settings/list.html",
        items=rows,
        page=page,
        pages=pages if total else 1,
        per_page=per_page,
        total=total,
        search=search,
    )


# CREATE
@settings_bp.route("/create", methods=["GET", "POST"])
@login_required
@admin_required
def create_setting():
    if request.method == "POST":
        f = request.form
        setting_key = (f.get("setting_key") or "").strip()
        # smtp_password is masked in UI; value comes from setting_value_password
        if setting_key == "smtp_password":
            setting_value = (f.get("setting_value_password") or "").strip()
        else:
            setting_value = (f.get("setting_value") or "").strip()
        value_type = (f.get("value_type") or "string").strip()
        description = (f.get("description") or "").strip() or None

        if not setting_key:
            flash("Setting key is required.", "warning")
            return redirect(url_for("settings.create_setting"))

        try:
            with get_db().cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO settings
                        (setting_key, setting_value, value_type, description)
                    VALUES
                        (%s, %s, %s, %s)
                    """,
                    (setting_key, setting_value, value_type, description),
                )
            flash("Setting created.", "success")
            return redirect(url_for("settings.list_settings"))
        except pymysql.err.IntegrityError as e:
            # UNIQUE(setting_key) vb. hatalar
            flash(f"Cannot create setting: {str(e)}", "danger")
            return redirect(url_for("settings.create_setting"))

    return render_template("settings/form.html", mode="create", row=None)


# UPDATE
@settings_bp.route("/<setting_key>/edit", methods=["GET", "POST"])
@login_required
@admin_required
def edit_setting(setting_key: str):
    with get_db().cursor() as cur:
        cur.execute(
            "SELECT setting_key, setting_value, value_type, description "
            "FROM settings WHERE setting_key=%s",
            (setting_key,),
        )
        row = cur.fetchone()

    if not row:
        flash("Setting not found.", "warning")
        return redirect(url_for("settings.list_settings"))

    if request.method == "POST":
        f = request.form
        # key'i burada değiştirtmiyoruz, sadece value/type/description
        if setting_key == "smtp_password":
            # masked: blank means keep existing
            new_pwd = (f.get("setting_value_password") or "").strip()
            setting_value = new_pwd if new_pwd else (row.get("setting_value") or "")
        else:
            setting_value = (f.get("setting_value") or "").strip()
        value_type = (f.get("value_type") or "string").strip()
        description = (f.get("description") or "").strip() or None

        try:
            with get_db().cursor() as cur:
                cur.execute(
                    """
                    UPDATE settings
                       SET setting_value = %s,
                           value_type    = %s,
                           description   = %s,
                           updated_at    = NOW()
                     WHERE setting_key   = %s
                    """,
                    (setting_value, value_type, description, setting_key),
                )
            flash("Setting updated.", "success")
        except pymysql.MySQLError as e:
            flash(f"Update failed: {str(e)}", "danger")

        return redirect(url_for("settings.list_settings"))

    return render_template("settings/form.html", mode="edit", row=row)


def _get_setting_value(key: str):
    with get_db().cursor() as cur:
        cur.execute("SELECT setting_value FROM settings WHERE setting_key=%s", (key,))
        r = cur.fetchone()
        return (r["setting_value"] if r else None)


def _truthy(val: str | None) -> bool:
    if val is None:
        return False
    v = str(val).strip().lower()
    return v in {"1", "true", "yes", "on", "enabled"}


@settings_bp.route("/mail-test", methods=["POST"])
@login_required
@admin_required
def mail_test():
    # Minimal keys (as discussed in the UI):
    # smtp_enabled, smtp_host, smtp_port, smtp_security, smtp_username, smtp_password,
    # smtp_from_address, smtp_from_name
    smtp_enabled = _get_setting_value("smtp_enabled")
    if smtp_enabled is not None and not _truthy(smtp_enabled):
        flash("SMTP is disabled (smtp_enabled=0).", "warning")
        return redirect(url_for("settings.list_settings"))

    host = (_get_setting_value("smtp_host") or "").strip()
    port_raw = (_get_setting_value("smtp_port") or "").strip()
    security = ((_get_setting_value("smtp_security") or "none").strip().lower())
    username = (_get_setting_value("smtp_username") or "").strip()
    password = (_get_setting_value("smtp_password") or "").strip()
    from_addr = (_get_setting_value("smtp_from_address") or username).strip()
    from_name = (_get_setting_value("smtp_from_name") or "OmniRiskDB").strip()

    if not host or not port_raw:
        flash("SMTP host/port is missing.", "danger")
        return redirect(url_for("settings.list_settings"))

    try:
        port = int(port_raw)
    except ValueError:
        flash("SMTP port must be numeric.", "danger")
        return redirect(url_for("settings.list_settings"))

    if not from_addr:
        flash("From address is missing (smtp_from_address).", "danger")
        return redirect(url_for("settings.list_settings"))

    # Send test mail to the same address (simple outbound verification)
    to_addr = from_addr

    msg = EmailMessage()
    msg["Subject"] = "OmniRiskDB SMTP Test"
    msg["From"] = f"{from_name} <{from_addr}>" if from_name else from_addr
    msg["To"] = to_addr
    msg.set_content(
        "This is a test email sent from DBVulScan Settings page.\n"
        f"Host: {host}\nPort: {port}\nSecurity: {security}\n"
    )

    try:
        if security == "ssl":
            server = smtplib.SMTP_SSL(host, port, timeout=15)
        else:
            server = smtplib.SMTP(host, port, timeout=15)
        try:
            server.ehlo()
            if security == "starttls":
                server.starttls()
                server.ehlo()
            if username:
                server.login(username, password)
            server.send_message(msg)
        finally:
            try:
                server.quit()
            except Exception:
                pass
        flash(f"Test mail sent to {to_addr}.", "success")
    except Exception as e:
        flash(f"Test mail failed: {str(e)}", "danger")

    return redirect(url_for("settings.list_settings"))


# DELETE
@settings_bp.route("/<setting_key>/delete", methods=["POST"])
@login_required
@admin_required
def delete_setting(setting_key: str):
    try:
        with get_db().cursor() as cur:
            cur.execute("DELETE FROM settings WHERE setting_key=%s", (setting_key,))
        flash("Setting deleted.", "info")
    except pymysql.MySQLError as e:
        flash(f"Delete failed: {str(e)}", "danger")

    return redirect(url_for("settings.list_settings"))
