from flask import render_template, request, redirect, url_for, flash, session
from . import checkpoints_bp
from db import get_db


# ---------- Helper functions ---------- #

def get_oracle_connection(ds):
    """
    Oracle connection – datasources iş mantığıyla uyumlu.
    """
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

    conn = oracledb.connect(user=user, password=pwd, dsn=dsn)
    return conn


def get_mssql_connection(ds):
    """
    MSSQL connection – pyodbc + ODBC Driver 18 ile.
    """
    try:
        import pyodbc
    except ImportError:
        raise RuntimeError("pyodbc module is not installed. Please install it in the virtualenv.")

    host = ds.get("host")
    port = int(ds.get("port") or 1433)
    auth_mode = ds.get("auth_mode") or "sql"
    username = ds.get("username")
    password = ds.get("password")

    # DB alanını düzgün normalize edelim
    database_raw = ds.get("database_name")
    database = (database_raw or "").strip()
    if database.lower() == "none":
        database = ""

    driver = "{ODBC Driver 18 for SQL Server}"

    if auth_mode == "sql":
        db_part = f"DATABASE={database};" if database else ""
        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={host},{port};"
            f"{db_part}"
            f"UID={username};PWD={password};"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )
    else:
        db_part = f"DATABASE={database};" if database else ""
        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={host},{port};"
            f"{db_part}"
            "Trusted_Connection=yes;"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )

    return pyodbc.connect(conn_str)


def evaluate_condition(result_value, condition_text):
    """
    result_value: SQL_Test'ten dönen ilk kolon (int/float/str/None)
    condition_text: ör. '> 0', '== 0', "== 'OPEN'"
    """
    if not condition_text:
        return None, None

    expr = f"{repr(result_value)} {condition_text}"
    try:
        value = bool(eval(expr, {"__builtins__": {}}))
    except Exception as e:
        return None, f"Condition evaluation error: {e} (expr={expr})"

    return (value, expr), None


# ---------- LIST ---------- #
@checkpoints_bp.route('/')
def list_checkpoints():
    db = get_db()
    cursor = db.cursor()

    # --- Search param: URL varsa onu al, yoksa session'dan oku ---
    q_param = request.args.get('q')
    if q_param is not None:
        search = q_param.strip()
        session['cp_search'] = search  # boş da olsa güncel değeri yaz
    else:
        search = session.get('cp_search', '').strip()

    # --- Pagination params ---
    try:
        page = int(request.args.get('page', 1))
    except (TypeError, ValueError):
        page = 1

    if page < 1:
        page = 1

    per_page = 15  # her sayfada kaç checkpoint gösterileceği

    # --- Dinamik WHERE ---
    where_clause = ""
    params_count = []

    if search:
        where_clause = """
            WHERE
                Name LIKE %s
                OR DB_Type LIKE %s
                OR Severity LIKE %s
        """
        like = f"%{search}%"
        params_count = [like, like, like]

    # --- Toplam kayıt sayısı ---
    cursor.execute(f"SELECT COUNT(*) AS cnt FROM checkpoints {where_clause}", params_count)
    total_records = cursor.fetchone()["cnt"]

    # --- Sayfa / offset hesapla ---
    if total_records == 0:
        total_pages = 1
        page = 1
        offset = 0
    else:
        total_pages = (total_records + per_page - 1) // per_page
        if page > total_pages:
            page = total_pages
        offset = (page - 1) * per_page

    # --- İlgili sayfadaki kayıtlar ---
    params_rows = params_count + [per_page, offset]

    cursor.execute(
        f"""
        SELECT
            Id AS id,
            Name AS name,
            DB_Type AS db_type,
            Severity AS severity
        FROM checkpoints
        {where_clause}
        ORDER BY Name ASC
        LIMIT %s OFFSET %s
        """,
        params_rows,
    )

    rows = cursor.fetchall()

    # Gösterilen satır aralığı
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
    )


# ---------- NEW ---------- #

@checkpoints_bp.route('/new', methods=['GET', 'POST'])
def new_checkpoint():
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        db_type = request.form.get('db_type', '').strip()
        severity = request.form.get('severity', '').strip() or 'medium'
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
                Name, DB_Type, Severity, Description,
                Pre_SQL_Test, SQL_Test, Test_Condition,
                Pre_SQL_Detail, SQL_Detail,
                Text_Pass, Text_Fail, Notes
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            name, db_type, severity, description,
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
        'severity': 'medium',
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



# ---------- EDIT ---------- #

@checkpoints_bp.route('/<int:checkpoint_id>/edit', methods=['GET', 'POST'])
def edit_checkpoint(checkpoint_id):
    db = get_db()
    cursor = db.cursor()

    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        db_type = request.form.get('db_type', '').strip()
        severity = request.form.get('severity', '').strip() or 'medium'
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
                Name=%s, DB_Type=%s, Severity=%s, Description=%s,
                Pre_SQL_Test=%s, SQL_Test=%s, Test_Condition=%s,
                Pre_SQL_Detail=%s, SQL_Detail=%s,
                Text_Pass=%s, Text_Fail=%s, Notes=%s
            WHERE Id=%s
        """, (
            name, db_type, severity, description,
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



# =====================================================================
# -------------------------- RUN TEST --------------------------------
# =====================================================================

@checkpoints_bp.route('/<int:checkpoint_id>/run-test', methods=['GET', 'POST'])
def run_checkpoint_test(checkpoint_id):
    db = get_db()
    cursor = db.cursor()

    cursor.execute("""
        SELECT 
            Id AS id, Name AS name, DB_Type AS db_type,
            Severity AS severity, Description AS description,
            Pre_SQL_Test AS pre_sql_test,
            SQL_Test AS sql_test,
            Test_Condition AS test_condition
        FROM checkpoints
        WHERE Id=%s
    """, (checkpoint_id,))
    checkpoint = cursor.fetchone()

    if not checkpoint:
        flash('Checkpoint bulunamadı.', 'danger')
        return redirect(url_for('checkpoints.list_checkpoints'))

    cursor.execute("""
        SELECT 
            ds_id AS id, ds_name AS name,
            db_type, host, port,
            auth_mode, domain,
            username, password,
            database_name,
            oracle_service_name, oracle_sid
        FROM datasources
        WHERE db_type=%s
        ORDER BY ds_name
    """, (checkpoint['db_type'],))
    datasources = cursor.fetchall()

    selected_ds = None
    result_value = None
    condition_expr = None
    status = None
    error_message = None

    if request.method == 'POST':
        ds_id = request.form.get('datasource_id')

        if not ds_id:
            flash("Please select a datasource.", "danger")
        else:
            selected_ds = next((d for d in datasources if str(d["id"]) == ds_id), None)
            if not selected_ds:
                flash("Datasource not found.", "danger")
            else:

                # ---------- CONNECT ----------
                try:
                    if checkpoint["db_type"] == "oracle":
                        conn = get_oracle_connection(selected_ds)
                    elif checkpoint["db_type"] == "mssql":
                        conn = get_mssql_connection(selected_ds)
                    else:
                        raise RuntimeError("Unsupported DB")
                except Exception as e:
                    status = "ERROR"
                    error_message = str(e)
                else:
                    try:
                        cur = conn.cursor()

                        # ----------- PRE SQL -------------
                        pre_sql = checkpoint.get("pre_sql_test")
                        if pre_sql:
                            try:
                                for stmt in pre_sql.split(";"):
                                    if stmt.strip():
                                        cur.execute(stmt)
                                conn.commit()
                            except Exception as e:
                                status = "ERROR"
                                error_message = f"Pre SQL Test error: {e}"
                                cur.close()
                                conn.close()
                                return render_template(
                                    "checkpoints/run_test.html",
                                    checkpoint=checkpoint,
                                    datasources=datasources,
                                    selected_ds=selected_ds,
                                    status=status,
                                    error_message=error_message,
                                    result_value=None,
                                    condition_expr=None
                                )

                        # ----------- SQL TEST -------------
                        sql_test = checkpoint.get("sql_test")
                        try:
                            cur.execute(sql_test)
                            row = cur.fetchone()
                        except Exception as e:
                            status = "ERROR"
                            error_message = f"SQL Test error: {e}"
                            cur.close()
                            conn.close()
                            return render_template(
                                "checkpoints/run_test.html",
                                checkpoint=checkpoint,
                                datasources=datasources,
                                selected_ds=selected_ds,
                                status=status,
                                error_message=error_message,
                                result_value=None,
                                condition_expr=None
                            )

                        cur.close()
                        conn.close()

                        if not row:
                            status = "ERROR"
                            error_message = "SQL Test returned no rows."
                        else:
                            result_value = row[0]
                            cond_text = checkpoint.get("test_condition")

                            eval_result, eval_error = evaluate_condition(result_value, cond_text)
                            if eval_error:
                                status = "ERROR"
                                error_message = eval_error
                            else:
                                if eval_result is None:
                                    status = "NO_CONDITION"
                                else:
                                    ok, condition_expr = eval_result
                                    status = "PASS" if ok else "FAIL"

                    except Exception as e:
                        status = "ERROR"
                        error_message = str(e)

    return render_template(
        "checkpoints/run_test.html",
        checkpoint=checkpoint,
        datasources=datasources,
        selected_ds=selected_ds,
        status=status,
        error_message=error_message,
        result_value=result_value,
        condition_expr=condition_expr
    )



# =====================================================================
# ----------------------- RUN SQL DETAIL ------------------------------
# =====================================================================

@checkpoints_bp.route('/<int:checkpoint_id>/run-sql-detail', methods=['GET', 'POST'])
def run_checkpoint_detail(checkpoint_id):
    db = get_db()
    cursor = db.cursor()

    cursor.execute("""
        SELECT 
            Id AS id, Name AS name, DB_Type AS db_type,
            Severity AS severity, Description AS description,
            Pre_SQL_Detail AS pre_sql_detail,
            SQL_Detail AS sql_detail
        FROM checkpoints
        WHERE Id=%s
    """, (checkpoint_id,))
    checkpoint = cursor.fetchone()

    if not checkpoint:
        flash('Checkpoint bulunamadı.', 'danger')
        return redirect(url_for('checkpoints.list_checkpoints'))

    cursor.execute("""
        SELECT 
            ds_id AS id, ds_name AS name,
            db_type, host, port,
            auth_mode, domain,
            username, password,
            database_name,
            oracle_service_name, oracle_sid
        FROM datasources
        WHERE db_type=%s
        ORDER BY ds_name
    """, (checkpoint['db_type'],))
    datasources = cursor.fetchall()

    selected_ds = None
    detail_columns = []
    detail_rows = []
    status = None
    error_message = None

    if request.method == 'POST':
        ds_id = request.form.get('datasource_id')

        if not ds_id:
            flash("Please select a datasource.", "danger")
        else:
            selected_ds = next((d for d in datasources if str(d["id"]) == ds_id), None)

            try:
                if checkpoint["db_type"] == "oracle":
                    conn = get_oracle_connection(selected_ds)
                elif checkpoint["db_type"] == "mssql":
                    conn = get_mssql_connection(selected_ds)
                else:
                    raise RuntimeError("Unsupported DB")
            except Exception as e:
                status = "ERROR"
                error_message = str(e)
            else:
                try:
                    cur = conn.cursor()

                    # ------- PRE SQL DETAIL -------
                    pre_sql = checkpoint.get("pre_sql_detail")
                    if pre_sql:
                        try:
                            for stmt in pre_sql.split(";"):
                                if stmt.strip():
                                    cur.execute(stmt)
                            conn.commit()
                        except Exception as e:
                            status = "ERROR"
                            error_message = f"Pre SQL Detail error: {e}"
                            cur.close()
                            conn.close()
                            return render_template(
                                "checkpoints/run_detail.html",
                                checkpoint=checkpoint,
                                datasources=datasources,
                                selected_ds=selected_ds,
                                status=status,
                                error_message=error_message,
                                detail_columns=[],
                                detail_rows=[]
                            )

                    # ------- SQL DETAIL -------
                    try:
                        cur.execute(checkpoint["sql_detail"])
                        rows = cur.fetchall()
                        cols = [desc[0] for desc in cur.description] if cur.description else []
                    except Exception as e:
                        status = "ERROR"
                        error_message = f"SQL Detail error: {e}"
                        cur.close()
                        conn.close()
                        return render_template(
                            "checkpoints/run_detail.html",
                            checkpoint=checkpoint,
                            datasources=datasources,
                            selected_ds=selected_ds,
                            status=status,
                            error_message=error_message,
                            detail_columns=[],
                            detail_rows=[]
                        )

                    cur.close()
                    conn.close()

                    detail_columns = cols
                    detail_rows = [dict(zip(cols, r)) for r in rows]
                    status = "OK"

                except Exception as e:
                    status = "ERROR"
                    error_message = str(e)

    return render_template(
        "checkpoints/run_detail.html",
        checkpoint=checkpoint,
        datasources=datasources,
        selected_ds=selected_ds,
        status=status,
        error_message=error_message,
        detail_columns=detail_columns,
        detail_rows=detail_rows
    )


@checkpoints_bp.route('/<int:checkpoint_id>/delete', methods=['POST'])
def delete_checkpoint(checkpoint_id):
    db = get_db()
    cursor = db.cursor()

    # Kayıt gerçekten var mı diye kontrol etmek istersen:
    cursor.execute("""
        SELECT Id, Name
        FROM checkpoints
        WHERE Id = %s
    """, (checkpoint_id,))
    row = cursor.fetchone()

    if not row:
        flash('Checkpoint bulunamadı.', 'danger')
        return redirect(url_for('checkpoints.list_checkpoints'))

    # Silme işlemi
    cursor.execute("DELETE FROM checkpoints WHERE Id = %s", (checkpoint_id,))
    db.commit()

    flash(f"Checkpoint '{row['Name']}' silindi.", 'success')
    return redirect(url_for('checkpoints.list_checkpoints'))
