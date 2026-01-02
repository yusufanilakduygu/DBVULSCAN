from flask import render_template, request, redirect, url_for, flash, session
from . import checkpoints_bp
from db import get_db
import subprocess
import os
import re


# =========================================================
# ------------------ KERBEROS HELPERS ----------------------
# =========================================================

def _is_ip(host: str) -> bool:
    return bool(re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", (host or "").strip()))

def _kdestroy():
    subprocess.run(["kdestroy"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def _kinit(principal: str, password: str):
    _kdestroy()
    p = subprocess.run(
        ["kinit", principal],
        input=(password + "\n").encode(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
        env=os.environ.copy(),
    )
    if p.returncode != 0:
        err = (p.stderr or b"").decode(errors="ignore")
        raise RuntimeError(f"kinit failed: {err}")


# =========================================================
# ------------------ DB CONNECTIONS ------------------------
# =========================================================

def get_oracle_connection(ds):
    try:
        import oracledb
    except ImportError:
        raise RuntimeError("python-oracledb module is not installed. Please install it in the virtualenv.")

    host = ds.get("host")
    port = int(ds.get("port") or 1521)
    user = ds.get("username")
    pwd = ds.get("password")
    service_name = ds.get("oracle_service_name")
    sid = ds.get("oracle_sid")

    if service_name:
        dsn = oracledb.makedsn(host=host, port=port, service_name=service_name)
    elif sid:
        dsn = oracledb.makedsn(host=host, port=port, sid=sid)
    else:
        raise RuntimeError("Oracle requires service_name or SID.")

    return oracledb.connect(user=user, password=pwd, dsn=dsn)


def get_mssql_connection(ds):
    try:
        import pyodbc
    except ImportError:
        raise RuntimeError("pyodbc module is not installed. Please install it in the virtualenv.")

    host = (ds.get("host") or "").strip()
    port = int(ds.get("port") or 1433)
    auth_mode = (ds.get("auth_mode") or "sql").strip().lower()
    username = ds.get("username")
    password = ds.get("password")

    database_raw = ds.get("database_name")
    database = (database_raw or "").strip()
    if database.lower() == "none":
        database = ""

    driver = "{ODBC Driver 18 for SQL Server}"
    db_part = f"DATABASE={database};" if database else ""

    if auth_mode == "windows":
        if _is_ip(host):
            raise RuntimeError("Windows (Kerberos) authentication requires Host to be FQDN, not IP.")

        domain_user_id = ds.get("domain_user_id")
        if not domain_user_id:
            raise RuntimeError("Windows authentication requires domain_user_id in datasource.")

        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            """
            SELECT domain_username, domain_fqdn, netbios_name, password_enc
            FROM domain_users
            WHERE id = %s AND is_active = 1
            """,
            (domain_user_id,),
        )
        du = cursor.fetchone()
        if not du:
            raise RuntimeError("Domain user not found or inactive.")

        pw = du.get("password_enc")
        if isinstance(pw, (bytes, bytearray)):
            pw = pw.decode("utf-8")

        principal = f"{du['domain_username']}@{du['domain_fqdn'].upper()}"
        _kinit(principal, pw)

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={host},{port};"
            f"{db_part}"
            "Encrypt=Yes;"
            "TrustServerCertificate=Yes;"
            "Authentication=ActiveDirectoryIntegrated;"
        )
        return pyodbc.connect(conn_str, timeout=5)

    if not password:
        raise RuntimeError("SQL authentication requires password.")

    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={host},{port};"
        f"{db_part}"
        f"UID={username};PWD={password};"
        "Encrypt=Yes;"
        "TrustServerCertificate=Yes;"
    )
    return pyodbc.connect(conn_str, timeout=5)


def evaluate_condition(result_value, condition_text):
    if not condition_text:
        return None, None

    expr = f"{repr(result_value)} {condition_text}"
    try:
        value = bool(eval(expr, {"__builtins__": {}}))
    except Exception as e:
        return None, f"Condition evaluation error: {e} (expr={expr})"

    return (value, expr), None


# =========================================================
# ------------- CHECKPOINT SQL RUN (UI HELPERS) -----------
# =========================================================

def _fetch_checkpoint(cursor, checkpoint_id: int):
    """Fetch checkpoint in the same shape used by templates."""
    cursor.execute(
        """
        SELECT 
            Id AS id, Name AS name, DB_Type AS db_type, Severity AS severity,
            Category AS category,
            Description AS description,
            Pre_SQL_Test AS pre_sql_test,
            SQL_Test AS sql_test,
            Test_Condition AS test_condition,
            Pre_SQL_Detail AS pre_sql_detail,
            SQL_Detail AS sql_detail,
            Text_Pass AS text_pass,
            Text_Fail AS text_fail,
            Notes AS notes
        FROM checkpoints
        WHERE Id = %s
        """,
        (checkpoint_id,),
    )
    return cursor.fetchone()


def _fetch_datasources_for_dbtype(cursor, db_type: str):
    """Return datasource list for select dropdown."""
    cursor.execute(
        """
        SELECT
            ds_id AS id,
            ds_name AS name,
            host,
            port,
            auth_mode,
            domain_user_id,
            username,
            password,
            database_name,
            oracle_service_name,
            oracle_sid
        FROM datasources
        WHERE db_type = %s
        ORDER BY ds_name
        """,
        (db_type,),
    )
    return cursor.fetchall() or []


def _fetch_datasource_by_id(cursor, datasource_id: int, db_type: str):
    cursor.execute(
        """
        SELECT
            ds_id AS id,
            ds_name AS name,
            host,
            port,
            auth_mode,
            domain_user_id,
            username,
            password,
            database_name,
            oracle_service_name,
            oracle_sid
        FROM datasources
        WHERE ds_id = %s AND db_type = %s
        """,
        (datasource_id, db_type),
    )
    return cursor.fetchone()


def _execute_sql(conn, sql_text: str):
    """Execute a SQL that may return rows. Returns (columns, rows)."""
    cur = conn.cursor()
    cur.execute(sql_text)

    # Some statements don't return a result set.
    if not getattr(cur, "description", None):
        return [], []

    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    return cols, rows


def _rows_to_dicts(columns, rows, limit: int = 200):
    """Convert DB rows to list[dict] for Jinja template."""
    out = []
    for i, r in enumerate(rows):
        if i >= limit:
            break
        # oracledb returns tuples, pyodbc can return tuples too.
        if isinstance(r, dict):
            out.append(r)
        else:
            out.append({columns[j]: r[j] for j in range(len(columns))})
    return out


# =========================================================
# -------------------------- LIST --------------------------
# =========================================================

@checkpoints_bp.route("/", methods=["GET"])
def list_checkpoints():
    db = get_db()
    cursor = db.cursor()

    # -------------------------
    # RESET (clear all filters)
    # -------------------------
    if request.args.get("reset") == "1":
        session.pop("cp_search", None)
        session.pop("cp_f_db_type", None)
        session.pop("cp_f_category", None)
        session.pop("cp_f_severity", None)
        return redirect(url_for("checkpoints.list_checkpoints"))

    # -------------------------
    # SEARCH (persist via session)
    # -------------------------
    q_param = request.args.get("q")
    if q_param is not None:
        search = q_param.strip()
        session["cp_search"] = search
    else:
        search = (session.get("cp_search") or "").strip()

    # -------------------------
    # FILTERS (persist via session)
    # -------------------------
    # db_type
    if "db_type" in request.args:
        f_db_type = (request.args.get("db_type") or "").strip().lower()
        if f_db_type not in {"", "oracle", "mssql"}:
            f_db_type = ""
        session["cp_f_db_type"] = f_db_type
    else:
        f_db_type = (session.get("cp_f_db_type") or "").strip().lower()
        if f_db_type not in {"", "oracle", "mssql"}:
            f_db_type = ""

    # category
    if "category" in request.args:
        f_category = (request.args.get("category") or "").strip().upper()
        if f_category not in {"", "AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"}:
            f_category = ""
        session["cp_f_category"] = f_category
    else:
        f_category = (session.get("cp_f_category") or "").strip().upper()
        if f_category not in {"", "AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"}:
            f_category = ""

    # severity
    if "severity" in request.args:
        f_severity = (request.args.get("severity") or "").strip().lower()
        if f_severity not in {"", "info", "caution", "minor", "major", "critical"}:
            f_severity = ""
        session["cp_f_severity"] = f_severity
    else:
        f_severity = (session.get("cp_f_severity") or "").strip().lower()
        if f_severity not in {"", "info", "caution", "minor", "major", "critical"}:
            f_severity = ""

    # -------------------------
    # PAGINATION
    # -------------------------
    try:
        page = int(request.args.get("page", 1))
    except (TypeError, ValueError):
        page = 1
    if page < 1:
        page = 1

    per_page = 15

    # -------------------------
    # BUILD WHERE (search + filters)
    # -------------------------
    where_parts = []
    params = []

    if search:
        where_parts.append("""
            (
                Name LIKE %s
                OR DB_Type LIKE %s
                OR Severity LIKE %s
                OR Category LIKE %s
            )
        """)
        like = f"%{search}%"
        params.extend([like, like, like, like])

    if f_db_type:
        where_parts.append("DB_Type = %s")
        params.append(f_db_type)

    if f_category:
        where_parts.append("Category = %s")
        params.append(f_category)

    if f_severity:
        where_parts.append("Severity = %s")
        params.append(f_severity)

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)

    # -------------------------
    # COUNT
    # -------------------------
    cursor.execute(f"SELECT COUNT(*) AS cnt FROM checkpoints {where_clause}", params)
    total_records = cursor.fetchone()["cnt"]

    if total_records == 0:
        total_pages = 1
        page = 1
        offset = 0
    else:
        total_pages = (total_records + per_page - 1) // per_page
        if page > total_pages:
            page = total_pages
        offset = (page - 1) * per_page

    # -------------------------
    # ROWS
    # -------------------------
    params_rows = params + [per_page, offset]
    cursor.execute(
        f"""
        SELECT
            Id AS id,
            Name AS name,
            DB_Type AS db_type,
            Severity AS severity,
            Category AS category
        FROM checkpoints
        {where_clause}
        ORDER BY Name ASC
        LIMIT %s OFFSET %s
        """,
        params_rows,
    )
    rows = cursor.fetchall()

    if total_records == 0:
        start_record = 0
        end_record = 0
    else:
        start_record = offset + 1
        end_record = min(offset + per_page, total_records)

    return render_template(
        "checkpoints/list.html",
        rows=rows,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        total_records=total_records,
        start_record=start_record,
        end_record=end_record,
        search=search,
        f_db_type=f_db_type,
        f_category=f_category,
        f_severity=f_severity,
    )


# =========================================================
# --------------------------- NEW --------------------------
# =========================================================

@checkpoints_bp.route('/new', methods=['GET', 'POST'])
def new_checkpoint():
    if request.method == 'POST':
        name = request.form.get('name', '').strip()

        db_type = (request.form.get('db_type', '') or '').strip().lower()
        if db_type not in {'oracle', 'mssql'}:
            db_type = 'oracle'

        # UPDATED SEVERITY ENUM
        severity = (request.form.get('severity', '') or '').strip().lower() or 'major'
        if severity not in {'info', 'caution', 'minor', 'major', 'critical'}:
            severity = 'major'

        category = (request.form.get('category', '') or '').strip().upper() or 'OTHER'
        if category not in {'AUTH','PRIV','CONFIG','PATCH','AUDIT','ENCRYPT','ACCOUNT','OTHER'}:
            category = 'OTHER'

        description = request.form.get('description')
        pre_sql_test = request.form.get('pre_sql_test')
        sql_test = request.form.get('sql_test')
        test_condition = request.form.get('test_condition')
        pre_sql_detail = request.form.get('pre_sql_detail')
        sql_detail = request.form.get('sql_detail')
        text_pass = request.form.get('text_pass')
        text_fail = request.form.get('text_fail')
        notes = request.form.get('notes')

        if not name or not db_type or not sql_test or not sql_detail or not test_condition:
            flash('Name, DB Type, SQL Test ve SQL Detail and condition field must be entered.', 'danger')
            return render_template('checkpoints/form.html', mode='new', checkpoint=request.form)

        db = get_db()
        cursor = db.cursor()

        cursor.execute("""
            INSERT INTO checkpoints (
                Name, DB_Type, Severity, Category, Description,
                Pre_SQL_Test, SQL_Test, Test_Condition,
                Pre_SQL_Detail, SQL_Detail,
                Text_Pass, Text_Fail, Notes
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            name, db_type, severity, category, description,
            pre_sql_test, sql_test, test_condition,
            pre_sql_detail, sql_detail,
            text_pass, text_fail, notes
        ))

        db.commit()
        new_id = cursor.lastrowid

        flash('Checkpoint başarıyla oluşturuldu.', 'success')
        return redirect(url_for('checkpoints.edit_checkpoint', checkpoint_id=new_id))

    checkpoint = {
        'name': '',
        'db_type': 'oracle',
        'severity': 'major',
        'category': 'OTHER',
        'description': '',
        'pre_sql_test': '',
        'sql_test': '',
        'test_condition': '',
        'pre_sql_detail': '',
        'sql_detail': '',
        'text_pass': '',
        'text_fail': '',
        'notes': ''
    }
    return render_template('checkpoints/form.html', mode='new', checkpoint=checkpoint)


# =========================================================
# --------------------------- EDIT -------------------------
# =========================================================

@checkpoints_bp.route('/<int:checkpoint_id>/edit', methods=['GET', 'POST'])
def edit_checkpoint(checkpoint_id):
    db = get_db()
    cursor = db.cursor()

    if request.method == 'POST':
        name = request.form.get('name', '').strip()

        db_type = (request.form.get('db_type', '') or '').strip().lower()
        if db_type not in {'oracle', 'mssql'}:
            db_type = 'oracle'

        # UPDATED SEVERITY ENUM
        severity = (request.form.get('severity', '') or '').strip().lower() or 'major'
        if severity not in {'info', 'caution', 'minor', 'major', 'critical'}:
            severity = 'major'

        category = (request.form.get('category', '') or '').strip().upper() or 'OTHER'
        if category not in {'AUTH','PRIV','CONFIG','PATCH','AUDIT','ENCRYPT','ACCOUNT','OTHER'}:
            category = 'OTHER'

        description = request.form.get('description')
        pre_sql_test = request.form.get('pre_sql_test')
        sql_test = request.form.get('sql_test')
        test_condition = request.form.get('test_condition')
        pre_sql_detail = request.form.get('pre_sql_detail')
        sql_detail = request.form.get('sql_detail')
        text_pass = request.form.get('text_pass')
        text_fail = request.form.get('text_fail')
        notes = request.form.get('notes')

        if not name or not db_type or not sql_test or not sql_detail or not test_condition:
            flash('Name, DB Type, SQL Test ve SQL Detail and condition must be entered', 'danger')
            checkpoint = dict(request.form)
            checkpoint['id'] = checkpoint_id
            return render_template('checkpoints/form.html', mode='edit', checkpoint=checkpoint)

        cursor.execute("""
            UPDATE checkpoints SET
                Name=%s, DB_Type=%s, Severity=%s, Category=%s, Description=%s,
                Pre_SQL_Test=%s, SQL_Test=%s, Test_Condition=%s,
                Pre_SQL_Detail=%s, SQL_Detail=%s,
                Text_Pass=%s, Text_Fail=%s, Notes=%s
            WHERE Id=%s
        """, (
            name, db_type, severity, category, description,
            pre_sql_test, sql_test, test_condition,
            pre_sql_detail, sql_detail,
            text_pass, text_fail, notes,
            checkpoint_id
        ))
        db.commit()

        flash('Checkpoint has been updated successfully.', 'success')
        return redirect(url_for('checkpoints.edit_checkpoint', checkpoint_id=checkpoint_id))

    cursor.execute("""
        SELECT 
            Id AS id, Name AS name, DB_Type AS db_type, Severity AS severity,
            Category AS category,
            Description AS description,
            Pre_SQL_Test AS pre_sql_test,
            SQL_Test AS sql_test,
            Test_Condition AS test_condition,
            Pre_SQL_Detail AS pre_sql_detail,
            SQL_Detail AS sql_detail,
            Text_Pass AS text_pass,
            Text_Fail AS text_fail,
            Notes AS notes
        FROM checkpoints
        WHERE Id = %s
    """, (checkpoint_id,))
    row = cursor.fetchone()

    if not row:
        flash('Checkpoint bulunamadı.', 'danger')
        return redirect(url_for('checkpoints.list_checkpoints'))

    return render_template('checkpoints/form.html', mode='edit', checkpoint=row)


# =========================================================
# --------------------- RUN TEST SQL ----------------------
# =========================================================

@checkpoints_bp.route('/<int:checkpoint_id>/run_test', methods=['GET', 'POST'])
def run_test_sql(checkpoint_id):
    """UI helper: run checkpoint SQL_Test against a chosen datasource."""
    db = get_db()
    cursor = db.cursor()

    checkpoint = _fetch_checkpoint(cursor, checkpoint_id)
    if not checkpoint:
        flash('Checkpoint bulunamadı.', 'danger')
        return redirect(url_for('checkpoints.list_checkpoints'))

    datasources = _fetch_datasources_for_dbtype(cursor, checkpoint['db_type'])

    selected_ds = None
    status = None
    result_value = None
    condition_expr = None
    error_message = None

    if request.method == 'POST':
        ds_id_raw = (request.form.get('datasource_id') or '').strip()
        if not ds_id_raw:
            flash('Please select a datasource.', 'warning')
        else:
            try:
                ds_id = int(ds_id_raw)
            except ValueError:
                ds_id = None

            if not ds_id:
                flash('Invalid datasource selection.', 'danger')
            else:
                selected_ds = _fetch_datasource_by_id(cursor, ds_id, checkpoint['db_type'])
                if not selected_ds:
                    flash('Datasource not found for this DB type.', 'danger')
                else:
                    try:
                        conn = None
                        # Connect
                        if checkpoint['db_type'] == 'oracle':
                            conn = get_oracle_connection(selected_ds)
                        elif checkpoint['db_type'] == 'mssql':
                            conn = get_mssql_connection(selected_ds)
                        else:
                            raise RuntimeError('Unsupported DB type for run_test.')

                        # Optional pre SQL
                        if checkpoint.get('pre_sql_test'):
                            _execute_sql(conn, checkpoint['pre_sql_test'])

                        # Main SQL
                        cols, rows = _execute_sql(conn, checkpoint['sql_test'])

                        # Pick first column of first row (the usual pattern)
                        if rows and len(rows) > 0:
                            first_row = rows[0]
                            if isinstance(first_row, dict):
                                # dict row (rare)
                                result_value = list(first_row.values())[0] if first_row else None
                            else:
                                result_value = first_row[0] if len(first_row) else None
                        else:
                            result_value = None

                        # Evaluate condition
                        cond_text = (checkpoint.get('test_condition') or '').strip()
                        if not cond_text:
                            status = 'NO_CONDITION'
                        else:
                            res, err = evaluate_condition(result_value, cond_text)
                            if err:
                                status = 'ERROR'
                                error_message = err
                            else:
                                passed, condition_expr = res
                                status = 'PASS' if passed else 'FAIL'

                    except Exception as e:
                        status = 'ERROR'
                        error_message = str(e)
                    finally:
                        try:
                            if conn:
                                conn.close()  # type: ignore
                        except Exception:
                            pass
                        # If Kerberos (kinit) was used, clean ticket to avoid surprises.
                        try:
                            _kdestroy()
                        except Exception:
                            pass

    return render_template(
        'checkpoints/run_test.html',
        checkpoint=checkpoint,
        datasources=datasources,
        selected_ds=selected_ds,
        status=status,
        result_value=result_value,
        condition_expr=condition_expr,
        error_message=error_message,
    )


# =========================================================
# -------------------- RUN DETAIL SQL ---------------------
# =========================================================

@checkpoints_bp.route('/<int:checkpoint_id>/run_detail', methods=['GET', 'POST'])
def run_detail_sql(checkpoint_id):
    """UI helper: run checkpoint SQL_Detail against a chosen datasource."""
    db = get_db()
    cursor = db.cursor()

    checkpoint = _fetch_checkpoint(cursor, checkpoint_id)
    if not checkpoint:
        flash('Checkpoint bulunamadı.', 'danger')
        return redirect(url_for('checkpoints.list_checkpoints'))

    datasources = _fetch_datasources_for_dbtype(cursor, checkpoint['db_type'])

    selected_ds = None
    status = None
    error_message = None
    detail_columns = []
    detail_rows = []

    if request.method == 'POST':
        ds_id_raw = (request.form.get('datasource_id') or '').strip()
        if not ds_id_raw:
            flash('Please select a datasource.', 'warning')
        else:
            try:
                ds_id = int(ds_id_raw)
            except ValueError:
                ds_id = None

            if not ds_id:
                flash('Invalid datasource selection.', 'danger')
            else:
                selected_ds = _fetch_datasource_by_id(cursor, ds_id, checkpoint['db_type'])
                if not selected_ds:
                    flash('Datasource not found for this DB type.', 'danger')
                else:
                    try:
                        conn = None
                        if checkpoint['db_type'] == 'oracle':
                            conn = get_oracle_connection(selected_ds)
                        elif checkpoint['db_type'] == 'mssql':
                            conn = get_mssql_connection(selected_ds)
                        else:
                            raise RuntimeError('Unsupported DB type for run_detail.')

                        if checkpoint.get('pre_sql_detail'):
                            _execute_sql(conn, checkpoint['pre_sql_detail'])

                        cols, rows = _execute_sql(conn, checkpoint['sql_detail'])
                        detail_columns = cols
                        detail_rows = _rows_to_dicts(cols, rows, limit=200)
                        status = 'OK'

                    except Exception as e:
                        status = 'ERROR'
                        error_message = str(e)
                    finally:
                        try:
                            if conn:
                                conn.close()  # type: ignore
                        except Exception:
                            pass
                        try:
                            _kdestroy()
                        except Exception:
                            pass

    return render_template(
        'checkpoints/run_detail.html',
        checkpoint=checkpoint,
        datasources=datasources,
        selected_ds=selected_ds,
        status=status,
        error_message=error_message,
        detail_columns=detail_columns,
        detail_rows=detail_rows,
    )


# =========================================================
# -------------------------- DELETE ------------------------
# =========================================================

@checkpoints_bp.route('/<int:checkpoint_id>/delete', methods=['POST'])
def delete_checkpoint(checkpoint_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute("DELETE FROM checkpoints WHERE Id=%s", (checkpoint_id,))
    db.commit()
    flash('Checkpoint deleted.', 'success')
    return redirect(url_for('checkpoints.list_checkpoints'))
