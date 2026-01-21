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
        session.pop("assessments_asset_impact", None)
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
    f_asset_impact = _get_persisted("asset_impact", "")

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

    if f_asset_impact:
        conditions.append("asset_impact = %s")
        params.append(f_asset_impact)

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    # Include domain name (assessments are optionally grouped under domains)
    # IMPORTANT: last_run_id is needed for Show Results button state
    sql = f"""
        SELECT a.assessment_id,
               a.name,
               a.datasource_name,
               a.benchmark_name,
               a.db_type,
               a.asset_impact,
               a.status,
               a.last_run_id,
               d.name AS domain_name
        FROM assessments a
        LEFT JOIN domains d ON d.domain_id = a.domain_id
        {where_clause}
        ORDER BY a.updated_at DESC
    """
    cur.execute(sql, params)
    assessments = cur.fetchall()

    return render_template(
        "assessments/list.html",
        assessments=assessments,
        search=search,
        f_status=f_status,
        f_db_type=f_db_type,
        f_asset_impact=f_asset_impact
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
        asset_impact = request.form.get("asset_impact", "medium")
        domain_id = request.form.get("domain_id") or None
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
             asset_impact, status, domain_id, notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                name,
                datasource_id, ds["ds_name"],
                benchmark_id, bm["name"], bm["db_type"],
                asset_impact,
                status,
                domain_id,
                notes
            )
        )
        db.commit()
        return redirect(url_for("assessments.list_assessments"))

    cur.execute("SELECT ds_id, ds_name FROM datasources ORDER BY ds_name")
    datasources = cur.fetchall()

    cur.execute("SELECT benchmark_id, name, db_type FROM benchmarks ORDER BY name")
    benchmarks = cur.fetchall()

    cur.execute("SELECT domain_id, name FROM domains ORDER BY name")
    domains = cur.fetchall()

    return render_template(
        "assessments/form.html",
        assessment=None,
        datasources=datasources,
        benchmarks=benchmarks,
        domains=domains
    )


@assessments_bp.route("/edit/<int:assessment_id>", methods=["GET", "POST"])
def edit_assessment(assessment_id):
    db = get_db()
    cur = db.cursor()

    if request.method == "POST":
        name = request.form["name"]
        status = request.form["status"]
        asset_impact = request.form.get("asset_impact", "medium")
        domain_id = request.form.get("domain_id") or None
        notes = request.form.get("notes")

        cur.execute(
            """
            UPDATE assessments
            SET name=%s, asset_impact=%s, status=%s, domain_id=%s, notes=%s
            WHERE assessment_id=%s
            """,
            (name, asset_impact, status, domain_id, notes, assessment_id)
        )
        db.commit()
        return redirect(url_for("assessments.list_assessments"))

    cur.execute("SELECT * FROM assessments WHERE assessment_id=%s", (assessment_id,))
    assessment = cur.fetchone()

    cur.execute("SELECT ds_id, ds_name FROM datasources ORDER BY ds_name")
    datasources = cur.fetchall()

    cur.execute("SELECT benchmark_id, name, db_type FROM benchmarks ORDER BY name")
    benchmarks = cur.fetchall()

    cur.execute("SELECT domain_id, name FROM domains ORDER BY name")
    domains = cur.fetchall()

    return render_template(
        "assessments/form.html",
        assessment=assessment,
        datasources=datasources,
        benchmarks=benchmarks,
        domains=domains
    )


@assessments_bp.route("/run/<int:assessment_id>", methods=["POST"])
def run_assessment_action(assessment_id: int):
    if "user" not in session:
        return redirect(url_for("auth.login"))

    try:
        run_id, run_month = run_assessment(assessment_id)
        flash(f"Assessment executed. Run ID: {run_id} (month: {run_month})", "success")
    except Exception as e:
        flash(f"Run failed: {e}", "danger")

    return redirect(request.referrer or url_for("assessments.list_assessments"))


@assessments_bp.route("/delete/<int:assessment_id>", methods=["POST"])
def delete_assessment(assessment_id: int):
    """Delete assessment and its run history.

    Required delete order:
      - assessment_run_checkpoints  by run_id
      - assessment_run_metrics      by run_id
      - assessment_runs             by assessment_id
      - assessments                 by assessment_id
    """
    db = get_db()
    cur = db.cursor()

    # Get runs for this assessment
    cur.execute("SELECT run_id FROM assessment_runs WHERE assessment_id=%s", (assessment_id,))
    run_ids = [r["run_id"] for r in cur.fetchall()]

    # Delete child tables by run_id (if exist)
    if run_ids:
        cur.execute(
            "DELETE FROM assessment_run_checkpoints WHERE run_id IN (%s)" % ",".join(["%s"] * len(run_ids)),
            run_ids
        )
        cur.execute(
            "DELETE FROM assessment_run_metrics WHERE run_id IN (%s)" % ",".join(["%s"] * len(run_ids)),
            run_ids
        )

    # Delete runs
    cur.execute("DELETE FROM assessment_runs WHERE assessment_id=%s", (assessment_id,))

    # Delete assessment
    cur.execute("DELETE FROM assessments WHERE assessment_id=%s", (assessment_id,))

    db.commit()
    return redirect(url_for("assessments.list_assessments"))
