# -*- coding: utf-8 -*-
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    flash, jsonify, session
)
import pymysql
import os
import socket
import subprocess
import re

datasources_bp = Blueprint("datasources", __name__, url_prefix="/datasources")

# ---------------------- MySQL repo connection ----------------------
def get_repo_conn():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "app_user"),
        password=os.getenv("MYSQL_PASSWORD", "app_user"),
        database=os.getenv("MYSQL_DB", "repo"),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


ALLOWED_DB_TYPES = {"oracle", "mssql", "postgres", "mysql"}
ALLOWED_AUTH = {"sql", "windows"}


def require_login():
    """Redirect to login if no session."""
    if "user" not in session:
        return redirect(url_for("auth.login"))


def _fetch_domain_users_active():
    """Active domain users for Datasource forms (dropdown).

    password_enc is intentionally NOT returned here.
    """
    sql = """
        SELECT id, name, domain_username, domain_fqdn, netbios_name
          FROM domain_users
         WHERE is_active=1
         ORDER BY name ASC
    """

    with get_repo_conn() as con, con.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall() or []


def _fetch_domain_user_for_connect(domain_user_id: int) -> dict:
    """Fetch a single active domain user including password_enc for connection tests."""
    sql = """
        SELECT id, name, domain_username, domain_fqdn, netbios_name, password_enc, is_active
          FROM domain_users
         WHERE id=%s
         LIMIT 1
    """

    with get_repo_conn() as con, con.cursor() as cur:
        cur.execute(sql, (domain_user_id,))
        row = cur.fetchone()

    if not row:
        raise RuntimeError("Domain user not found.")
    if int(row.get("is_active") or 0) != 1:
        raise RuntimeError("Selected domain user is inactive.")
    return row


# ---------------------- Kerberos helpers ----------------------
def _is_ip(host: str) -> bool:
    return bool(re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", (host or "").strip()))


def _kdestroy_silent():
    subprocess.run(["kdestroy"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _kinit(principal: str, password: str):
    """kinit with password via stdin. Raises RuntimeError on failure."""
    if not principal:
        raise RuntimeError("kinit principal is empty.")
    if password is None or password == "":
        raise RuntimeError("kinit password is empty.")

    _kdestroy_silent()

    p = subprocess.run(
        ["kinit", principal],
        input=(password + "\n").encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
        env=os.environ.copy(),
    )
    if p.returncode != 0:
        err = (p.stderr or b"").decode("utf-8", errors="ignore").strip()
        if not err:
            err = "Unknown error"
        raise RuntimeError(f"kinit failed for {principal}: {err}")


# ---------------------- LIST ----------------------
@datasources_bp.route("/", methods=["GET"])
def list_datasources():
    rl = require_login()
    if rl:
        return rl

    # ---- Search & pagination (session persistent) ----
    raw_search = request.args.get("search")
    raw_page = request.args.get("page", type=int)
    clear = request.args.get("clear")

    if clear:
        session.pop("datasources_search", None)
        session.pop("datasources_page", None)
        search = ""
        page = 1
    else:
        if raw_search is not None:
            search = (raw_search or "").strip()
            session["datasources_search"] = search
            page = 1
        else:
            search = session.get("datasources_search", "")

        if raw_page is not None:
            page = raw_page
        else:
            page = session.get("datasources_page", 1)

    if page is None or page < 1:
        page = 1

    per_page = 10

    base_sql = """
        FROM datasources
        WHERE 1=1
    """
    params = {}

    if search:
        base_sql += """
          AND (
                ds_name LIKE %(q)s
            OR  host LIKE %(q)s
            OR  username LIKE %(q)s
            OR  db_type LIKE %(q)s
            OR  IFNULL(instance_name,'') LIKE %(q)s
            OR  IFNULL(oracle_service_name,'') LIKE %(q)s
            OR  IFNULL(oracle_sid,'') LIKE %(q)s
          )
        """
        params["q"] = f"%{search}%"

    count_sql = "SELECT COUNT(*) AS total " + base_sql

    with get_repo_conn() as con, con.cursor() as cur:
        cur.execute(count_sql, params)
        total = cur.fetchone()["total"] if cur.rowcount else 0

        pages = (total + per_page - 1) // per_page if total else 1
        if page > pages:
            page = pages

        session["datasources_page"] = page

        offset = (page - 1) * per_page

        data_sql = """
            SELECT
                ds_id,
                ds_name,
                db_type,
                host,
                port,
                username,
                instance_name,
                oracle_service_name,
                oracle_sid
        """ + base_sql + """
            ORDER BY ds_name
            LIMIT %(limit)s OFFSET %(offset)s
        """

        params["limit"] = per_page
        params["offset"] = offset

        cur.execute(data_sql, params)
        rows = cur.fetchall()

    return render_template(
        "datasources/list.html",
        rows=rows,
        page=page,
        pages=pages,
        per_page=per_page,
        total=total,
        search=search,
    )


# ---------------------- NEW ----------------------
@datasources_bp.route("/new", methods=["GET", "POST"])
def new_datasource():
    rl = require_login()
    if rl:
        return rl

    if request.method == "POST":
        f = request.form
        db_type = (f.get("db_type") or "").lower()
        auth_mode = (f.get("auth_mode") or "sql").lower()

        if db_type not in ALLOWED_DB_TYPES:
            flash(f"Invalid db_type: {db_type}", "error")
            return render_template(
                "datasources/forms.html",
                item=None,
                domain_users=_fetch_domain_users_active(),
            )

        if auth_mode not in ALLOWED_AUTH:
            flash(f"Invalid auth_mode: {auth_mode}", "error")
            return render_template(
                "datasources/forms.html",
                item=None,
                domain_users=_fetch_domain_users_active(),
            )

        raw_duid = (f.get("domain_user_id") or "").strip()
        domain_user_id = int(raw_duid) if raw_duid.isdigit() else None
        if auth_mode != "windows":
            domain_user_id = None

        sql = """
            INSERT INTO datasources (
                ds_name, description,
                db_type, host, port,
                auth_mode, domain_user_id,
                domain, username, password,
                instance_name, database_name,
                oracle_service_name, oracle_sid,
                connection_property, custom_url
            )
            VALUES (
                %(ds_name)s, %(description)s,
                %(db_type)s, %(host)s, %(port)s,
                %(auth_mode)s, %(domain_user_id)s,
                %(domain)s, %(username)s, %(password)s,
                %(instance_name)s, %(database_name)s,
                %(oracle_service_name)s, %(oracle_sid)s,
                %(connection_property)s, %(custom_url)s
            )
        """

        params = {
            "ds_name": f.get("ds_name"),
            "description": f.get("description"),
            "db_type": db_type,
            "host": f.get("host"),
            "port": int(f.get("port") or 0),
            "auth_mode": auth_mode,
            "domain_user_id": domain_user_id,
            "domain": f.get("domain"),
            "username": f.get("username"),
            "password": f.get("password") or None,
            "instance_name": f.get("instance_name"),
            "database_name": f.get("database_name"),
            "oracle_service_name": f.get("oracle_service_name"),
            "oracle_sid": f.get("oracle_sid"),
            "connection_property": f.get("connection_property"),
            "custom_url": f.get("custom_url"),
        }

        try:
            with get_repo_conn() as con, con.cursor() as cur:
                cur.execute(sql, params)
                new_id = cur.lastrowid

            flash("Datasource saved.", "success")
            return redirect(url_for("datasources.edit_datasource", ds_id=new_id))

        except Exception as e:
            flash(f"Error while creating datasource: {e}", "error")

        return render_template(
            "datasources/forms.html",
            item=None,
            domain_users=_fetch_domain_users_active(),
        )

    return render_template(
        "datasources/forms.html",
        item=None,
        domain_users=_fetch_domain_users_active(),
    )


# ---------------------- EDIT ----------------------
@datasources_bp.route("/<int:ds_id>/edit", methods=["GET", "POST"])
def edit_datasource(ds_id):
    rl = require_login()
    if rl:
        return rl

    with get_repo_conn() as con, con.cursor() as cur:
        cur.execute("SELECT * FROM datasources WHERE ds_id=%s", (ds_id,))
        item = cur.fetchone()

    if not item:
        flash("Datasource not found.", "error")
        return redirect(url_for("datasources.list_datasources"))

    if request.method == "POST":
        f = request.form

        db_type = (f.get("db_type") or "").lower()
        auth_mode = (f.get("auth_mode") or "sql").lower()

        if db_type not in ALLOWED_DB_TYPES:
            flash(f"Invalid db_type: {db_type}", "error")
            return render_template(
                "datasources/forms.html",
                item=item,
                domain_users=_fetch_domain_users_active(),
            )

        if auth_mode not in ALLOWED_AUTH:
            flash(f"Invalid auth_mode: {auth_mode}", "error")
            return render_template(
                "datasources/forms.html",
                item=item,
                domain_users=_fetch_domain_users_active(),
            )

        new_pwd = f.get("password") or None

        raw_duid = (f.get("domain_user_id") or "").strip()
        domain_user_id = int(raw_duid) if raw_duid.isdigit() else None
        if auth_mode != "windows":
            domain_user_id = None

        sql = """
            UPDATE datasources
               SET ds_name=%(ds_name)s,
                   description=%(description)s,
                   db_type=%(db_type)s,
                   host=%(host)s,
                   port=%(port)s,
                   auth_mode=%(auth_mode)s,
                   domain_user_id=%(domain_user_id)s,
                   domain=%(domain)s,
                   username=%(username)s,
                   instance_name=%(instance_name)s,
                   database_name=%(database_name)s,
                   oracle_service_name=%(oracle_service_name)s,
                   oracle_sid=%(oracle_sid)s,
                   connection_property=%(connection_property)s,
                   custom_url=%(custom_url)s
             WHERE ds_id=%(ds_id)s
        """

        params = {
            "ds_id": ds_id,
            "ds_name": f.get("ds_name"),
            "description": f.get("description"),
            "db_type": db_type,
            "host": f.get("host"),
            "port": int(f.get("port") or 0),
            "auth_mode": auth_mode,
            "domain_user_id": domain_user_id,
            "domain": f.get("domain"),
            "username": f.get("username"),
            "instance_name": f.get("instance_name"),
            "database_name": f.get("database_name"),
            "oracle_service_name": f.get("oracle_service_name"),
            "oracle_sid": f.get("oracle_sid"),
            "connection_property": f.get("connection_property"),
            "custom_url": f.get("custom_url"),
        }

        try:
            with get_repo_conn() as con, con.cursor() as cur:
                cur.execute(sql, params)
                if new_pwd is not None and new_pwd != "":
                    cur.execute(
                        "UPDATE datasources SET password=%s WHERE ds_id=%s",
                        (new_pwd, ds_id),
                    )

            flash("Datasource saved.", "success")
            return redirect(url_for("datasources.edit_datasource", ds_id=ds_id))

        except Exception as e:
            flash(f"Error while updating datasource: {e}", "error")
            return render_template(
                "datasources/forms.html",
                item=item,
                domain_users=_fetch_domain_users_active(),
            )

    return render_template(
        "datasources/forms.html",
        item=item,
        domain_users=_fetch_domain_users_active(),
    )


# ---------------------- DELETE ----------------------
@datasources_bp.route("/<int:ds_id>/delete", methods=["POST"])
def delete_datasource(ds_id):
    rl = require_login()
    if rl:
        return rl

    with get_repo_conn() as con, con.cursor() as cur:
        cur.execute("DELETE FROM datasources WHERE ds_id=%s", (ds_id,))

    flash("Datasource deleted.", "success")
    return redirect(url_for("datasources.list_datasources"))


# ---------------------- DB CHECK (Oracle / MSSQL) ----------------------
@datasources_bp.route("/<int:ds_id>/check", methods=["POST"])
def check_datasource(ds_id):
    rl = require_login()
    if rl:
        if request.headers.get("X-Requested-With") == "fetch":
            return jsonify({"ok": False, "message": "Login required"}), 401
        return rl

    with get_repo_conn() as con, con.cursor() as cur:
        cur.execute("SELECT * FROM datasources WHERE ds_id=%s", (ds_id,))
        ds = cur.fetchone()

    if not ds:
        return jsonify({"ok": False, "message": "Datasource not found."}), 404

    try:
        msg = _do_check(ds)
        return jsonify({"ok": True, "message": msg})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)}), 500


def _do_check(ds: dict) -> str:
    db_type = (ds.get("db_type") or "").lower()
    host = (ds.get("host") or "").strip()
    port = int(ds.get("port") or 0)
    user = ds.get("username")
    pwd = ds.get("password")

    if db_type == "oracle":
        _check_oracle(
            host,
            port or 1521,
            user,
            pwd,
            ds.get("oracle_service_name"),
            ds.get("oracle_sid"),
        )
        return "Oracle connection OK"

    elif db_type == "mssql":
        mode = (ds.get("auth_mode") or "sql").lower()

        if mode == "windows":
            # Windows/Kerberos auth hostname ister
            if _is_ip(host):
                raise RuntimeError(
                    "Windows (Kerberos) authentication requires Host to be a hostname/FQDN "
                    "(e.g., WIN-...LAB.LOCAL). Do not use IP address."
                )

            du_id = ds.get("domain_user_id")
            if not du_id:
                raise RuntimeError(
                    "Windows (Domain) authentication requires a Domain User selection (domain_user_id)."
                )

            du = _fetch_domain_user_for_connect(int(du_id))
            netbios = (du.get("netbios_name") or "").strip()
            if not netbios:
                raise RuntimeError(
                    "Selected domain user has no NETBIOS name. Please set netbios_name (e.g., LAB)."
                )

            dom_user = (du.get("domain_username") or "").strip()
            if not dom_user:
                raise RuntimeError("Selected domain user has no domain_username.")

            realm = (du.get("domain_fqdn") or "").strip()
            if not realm:
                raise RuntimeError("Selected domain user has no domain_fqdn (e.g., lab.local).")

            enc = du.get("password_enc")
            if not enc:
                raise RuntimeError("Selected domain user has no password_enc.")

            # Şimdilik plaintext utf-8 bytes (domainusers kodunla uyumlu)
            if isinstance(enc, (bytes, bytearray)):
                pwd2 = enc.decode("utf-8", errors="strict")
            else:
                try:
                    pwd2 = bytes(enc).decode("utf-8", errors="strict")
                except Exception:
                    raise RuntimeError("Unable to decode password_enc for selected domain user.")

            principal = f"{dom_user}@{realm.upper()}"
            _kinit(principal, pwd2)

            _check_sqlserver(
                host,
                port or 1433,
                user=None,
                pwd=None,
                domain=None,
                auth_mode="windows",
            )
            return f"SQL Server (Windows auth) connection OK as {netbios}\\{dom_user} (KERBEROS)"

        # SQL Authentication
        _check_sqlserver(
            host,
            port or 1433,
            user,
            pwd,
            None,
            mode,
        )
        return "SQL Server (SQL auth) connection OK"

    elif db_type in {"postgres", "mysql"}:
        return f"{db_type} is not yet supported by 'Check' button."

    else:
        raise RuntimeError(f"Unsupported db_type: {db_type}")


def _check_oracle(host, port, user, pwd, service_name, sid):
    import oracledb

    if service_name:
        dsn = oracledb.makedsn(host=host, port=port, service_name=service_name)
    elif sid:
        dsn = oracledb.makedsn(host=host, port=port, sid=sid)
    else:
        raise RuntimeError("Oracle requires service_name or SID.")

    conn = oracledb.connect(user=user, password=pwd, dsn=dsn)
    cur = conn.cursor()
    cur.execute("select 1 from dual")
    cur.fetchone()
    cur.close()
    conn.close()


def _check_sqlserver(host, port, user, pwd, domain=None, auth_mode="sql"):
    import pyodbc

    server = f"{host},{port}" if port else host
    mode = (auth_mode or "sql").lower()

    if mode == "windows":
        # Kerberos SPN hostname ister (IP ile olmaz)
        if _is_ip(host):
            raise RuntimeError(
                "Windows (Kerberos) auth requires Host to be a hostname/FQDN (e.g., WIN-...LAB.LOCAL). "
                "Do not use IP address."
            )

        # UID/PWD YOK: ticket (kinit) ile bağlanıyoruz
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={server};"
            "DATABASE=master;"
            "Encrypt=Yes;"
            "TrustServerCertificate=Yes;"
            "Connection Timeout=5;"
            "Authentication=ActiveDirectoryIntegrated;"
        )

        conn = pyodbc.connect(conn_str, timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        return

    # SQL Authentication
    if not pwd:
        raise RuntimeError("SQL authentication requires a password in DBVulScan.")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        "Encrypt=Yes;"
        "TrustServerCertificate=Yes;"
        "Connection Timeout=5;"
        f"UID={user};PWD={pwd};"
        "DATABASE=master;"
    )

    conn = pyodbc.connect(conn_str, timeout=5)
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()
    cur.close()
    conn.close()


@datasources_bp.route("/<int:ds_id>/test-port", methods=["POST"])
def test_port(ds_id):
    """Host + port reachability using a TCP socket."""
    rl = require_login()
    if rl:
        if request.headers.get("X-Requested-With") == "fetch":
            return jsonify({"ok": False, "message": "Login required"}), 401
        return rl

    with get_repo_conn() as con, con.cursor() as cur:
        cur.execute("SELECT * FROM datasources WHERE ds_id=%s", (ds_id,))
        ds = cur.fetchone()

    if not ds:
        return jsonify({"ok": False, "message": "Datasource not found."}), 404

    try:
        msg = _socket_test_port(ds)
        return jsonify({"ok": True, "message": msg})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)}), 500


def _socket_test_port(ds: dict) -> str:
    """Plain TCP connect to host:port with friendly error messages."""
    host = (ds.get("host") or "").strip()
    port = int(ds.get("port") or 0)

    if not host:
        raise RuntimeError("Host is empty.")
    if not port:
        raise RuntimeError("Port is empty or invalid.")

    try:
        with socket.create_connection((host, port), timeout=3):
            return f"{host}:{port} is reachable over TCP."
    except socket.timeout:
        raise RuntimeError(
            "Connection timed out. Host or network may be unreachable, or a firewall is dropping packets."
        )
    except ConnectionRefusedError:
        raise RuntimeError(
            "Connection refused. Host is reachable but the port is closed or no service is listening."
        )
    except OSError as e:
        raise RuntimeError(f"Socket error while connecting to {host}:{port}: {e}")
