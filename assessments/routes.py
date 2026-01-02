from flask import flash, redirect, render_template, request, session, url_for
from db import get_db
from run_assessment import run_assessment
from . import assessments_bp


@assessments_bp.route("/")
def list_assessments():
    if request.args.get("reset") == "1":
        session.pop("assessments_q", None)
        session.pop("assessments_status", None)
        session.pop("assessments_db_type", None)
        return redirect(url_for("assessments.list_assessments"))

    def _get_persisted(key, default=""):
        if key in request.args:
            val = request.args.get(key, default) or ""
            session[f"assessments_{key}"] = val
            return val
        return session.get(f"assessments_{key}", default) or ""

    search = _get_persisted("q", "")
    f_status = _get_persisted("status", "")
    f_db_type = _get_persisted("db_type", "")

    db = get_db()
    cur = db.cursor()

    conditions = []
    params = []

    if search:
        conditions.append("name LIKE %s")
        params.append(f"%{search}%")

    if f_status:
        conditions.append("status = %s")
        params.append(f_status)

    if f_db_type:
        conditions.append("db_type = %s")
        params.append(f_db_type)

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    sql = f"""
        SELECT assessment_id, name, datasource_name,
               benchmark_name, db_type, status, updated_at
        FROM assessments
        {where_clause}
        ORDER BY updated_at DESC
    """
    cur.execute(sql, params)
    assessments = cur.fetchall()

    return render_template(
        "assessments/list.html",
        assessments=assessments,
        search=search,
        f_status=f_status,
        f_db_type=f_db_type
    )


@assessments_bp.route("/new", methods=["GET", "POST"])
def new_assessment():
    db = get_db()
    cur = db.cursor()

    if request.method == "POST":
        name = request.form["name"]
        datasource_id = request.form["datasource_id"]
        benchmark_id = request.form["benchmark_id"]
        status = request.form["status"]
        notes = request.form.get("notes")

        cur.execute("SELECT ds_name FROM datasources WHERE ds_id=%s", (datasource_id,))
        ds = cur.fetchone()

        cur.execute("SELECT name, db_type FROM benchmarks WHERE benchmark_id=%s", (benchmark_id,))
        bm = cur.fetchone()

        cur.execute(
            """
            INSERT INTO assessments
            (name, datasource_id, datasource_name,
             benchmark_id, benchmark_name, db_type,
             status, notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                name,
                datasource_id, ds["ds_name"],
                benchmark_id, bm["name"], bm["db_type"],
                status, notes
            )
        )
        db.commit()
        return redirect(url_for("assessments.list_assessments"))

    cur.execute("SELECT ds_id, ds_name FROM datasources ORDER BY ds_name")
    datasources = cur.fetchall()

    cur.execute("SELECT benchmark_id, name FROM benchmarks ORDER BY name")
    benchmarks = cur.fetchall()

    return render_template(
        "assessments/form.html",
        assessment=None,
        datasources=datasources,
        benchmarks=benchmarks
    )


@assessments_bp.route("/edit/<int:assessment_id>", methods=["GET", "POST"])
def edit_assessment(assessment_id):
    db = get_db()
    cur = db.cursor()

    if request.method == "POST":
        name = request.form["name"]
        status = request.form["status"]
        notes = request.form.get("notes")

        cur.execute(
            """
            UPDATE assessments
            SET name=%s, status=%s, notes=%s
            WHERE assessment_id=%s
            """,
            (name, status, notes, assessment_id)
        )
        db.commit()
        return redirect(url_for("assessments.list_assessments"))

    cur.execute("SELECT * FROM assessments WHERE assessment_id=%s", (assessment_id,))
    assessment = cur.fetchone()

    cur.execute("SELECT ds_id, ds_name FROM datasources ORDER BY ds_name")
    datasources = cur.fetchall()

    cur.execute("SELECT benchmark_id, name FROM benchmarks ORDER BY name")
    benchmarks = cur.fetchall()

    return render_template(
        "assessments/form.html",
        assessment=assessment,
        datasources=datasources,
        benchmarks=benchmarks
    )


# =========================================================
# ---------------------- RUN ASSESSMENT --------------------
# =========================================================

@assessments_bp.route("/run/<int:assessment_id>", methods=["POST"])
def run_assessment_action(assessment_id: int):
    """Run button handler (no extra pages).

    - Trigger run_assessment(assessment_id)
    - Stay on the same page (redirect back)
    - Show a flash message with the new run_id
    """

    if "user" not in session:
        return redirect(url_for("auth.login"))

    try:
        run_id, run_month = run_assessment(assessment_id)
        flash(f"Assessment executed. Run ID: {run_id} (month: {run_month})", "success")
    except Exception as e:
        flash(f"Run failed: {e}", "danger")

    # Go back to the list page (keep filters/search in URL if user came from there)
    return redirect(request.referrer or url_for("assessments.list_assessments"))
