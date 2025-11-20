# -*- coding: utf-8 -*-
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    flash, jsonify, session
)
import pymysql
import os
import socket

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


# ---------------------- LIST ----------------------
@datasources_bp.route("/", methods=["GET"])
def list_datasources():
    rl = require_login()
    if rl:
        return rl

    sql = """
        SELECT ds_id, ds_name, db_type, host, port, username,
               oracle_service_name, oracle_sid
          FROM datasources
         ORDER BY ds_name
    """

    with get_repo_conn() as con, con.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return render_template("datasources/list.html", rows=rows)


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
            return render_template("datasources/forms.html", item=None)

        if auth_mode not in ALLOWED_AUTH:
            flash(f"Invalid auth_mode: {auth_mode}", "error")
            return render_template("datasources/forms.html", item=None)

        sql = """
            INSERT INTO datasources (
                ds_name, description,
                db_type, host, port,
                auth_mode, domain, username, password,
                instance_name, database_name,
                oracle_service_name, oracle_sid,
                connection_property, custom_url
            )
            VALUES (
                %(ds_name)s, %(description)s,
                %(db_type)s, %(host)s, %(port)s,
                %(auth_mode)s, %(domain)s, %(username)s, %(password)s,
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
            # Liste yerine direkt edit formuna dön
            return redirect(url_for("datasources.edit_datasource", ds_id=new_id))

        except Exception as e:
            flash(f"Error while creating datasource: {e}", "error")

        return render_template("datasources/forms.html", item=None)

    # GET
    return render_template("datasources/forms.html", item=None)


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
            return render_template("datasources/forms.html", item=item)

        if auth_mode not in ALLOWED_AUTH:
            flash(f"Invalid auth_mode: {auth_mode}", "error")
            return render_template("datasources/forms.html", item=item)

        new_pwd = f.get("password") or None

        sql = """
            UPDATE datasources
               SET ds_name=%(ds_name)s,
                   description=%(description)s,
                   db_type=%(db_type)s,
                   host=%(host)s,
                   port=%(port)s,
                   auth_mode=%(auth_mode)s,
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
            # Liste yerine aynı formda kal
            return redirect(url_for("datasources.edit_datasource", ds_id=ds_id))

        except Exception as e:
            flash(f"Error while updating datasource: {e}", "error")
            return render_template("datasources/forms.html", item=item)

    # GET
    return render_template("datasources/forms.html", item=item)


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
        # fetch ile geldiyse JSON; normal POST ise redirect
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
    host = ds.get("host")
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
        _check_sqlserver(
            host,
            port or 1433,
            user,
            pwd,
            ds.get("domain"),
            ds.get("auth_mode"),
        )
        return "SQL Server connection OK"

    elif db_type in {"postgres", "mysql"}:
        return f"{db_type} is not yet supported by 'Check' button."

    else:
        raise RuntimeError(f"Unsupported db_type: {db_type}")


def _check_oracle(host, port, user, pwd, service_name, sid):
    import oracledb

    # DSN oluşturma (service_name veya SID'e göre)
    if service_name:
        dsn = oracledb.makedsn(host=host, port=port, service_name=service_name)
    elif sid:
        dsn = oracledb.makedsn(host=host, port=port, sid=sid)
    else:
        raise RuntimeError("Oracle requires service_name or SID.")

    # NOT: oracledb.connect() fonksiyonunda 'timeout' parametresi yok
    conn = oracledb.connect(user=user, password=pwd, dsn=dsn)
    cur = conn.cursor()
    cur.execute("select 1 from dual")
    cur.fetchone()
    cur.close()
    conn.close()


def _check_sqlserver(host, port, user, pwd, domain=None, auth_mode="sql"):
    import pyodbc

    server = f"{host},{port}" if port else host
    if (auth_mode or "sql").lower() == "windows":
        uid = f"{domain}\\{user}" if domain else user
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={server};"
            "Encrypt=Yes;"
            "TrustServerCertificate=Yes;"
            "Connection Timeout=5;"
            f"UID={uid};"
            "DATABASE=master;"
        )
    else:
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
