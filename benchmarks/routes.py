# -*- coding: utf-8 -*-
from flask import render_template, request, redirect, url_for, flash, session
from . import benchmarks_bp
from db import get_db
from security import login_required


# =========================================================
# Benchmarks CRUD
# =========================================================

@benchmarks_bp.route("/", methods=["GET"])
@login_required
def list_benchmarks():
    db = get_db()
    cur = db.cursor()

    # -------------------------
    # RESET (clear filters)
    # -------------------------
    if request.args.get("reset") == "1":
        session.pop("bm_search", None)
        session.pop("bm_f_db_type", None)
        session.pop("bm_f_status", None)
        session.pop("bm_f_level", None)
        return redirect(url_for("benchmarks.list_benchmarks"))

    # -------------------------
    # SEARCH (persist via session)
    # -------------------------
    q_param = request.args.get("q")
    if q_param is not None:
        search = (q_param or "").strip()
        session["bm_search"] = search
    else:
        search = (session.get("bm_search") or "").strip()

    # -------------------------
    # FILTERS (persist via session)
    # -------------------------
    if "db_type" in request.args:
        f_db_type = (request.args.get("db_type") or "").strip().lower()
        if f_db_type not in {"", "oracle", "mssql"}:
            f_db_type = ""
        session["bm_f_db_type"] = f_db_type
    else:
        f_db_type = (session.get("bm_f_db_type") or "").strip().lower()
        if f_db_type not in {"", "oracle", "mssql"}:
            f_db_type = ""

    if "status" in request.args:
        f_status = (request.args.get("status") or "").strip().upper()
        if f_status not in {"", "ACTIVE", "INACTIVE", "DRAFT"}:
            f_status = ""
        session["bm_f_status"] = f_status
    else:
        f_status = (session.get("bm_f_status") or "").strip().upper()
        if f_status not in {"", "ACTIVE", "INACTIVE", "DRAFT"}:
            f_status = ""

    if "level" in request.args:
        f_level = (request.args.get("level") or "").strip().upper()
        if f_level not in {"", "L1", "L2"}:
            f_level = ""
        session["bm_f_level"] = f_level
    else:
        f_level = (session.get("bm_f_level") or "").strip().upper()
        if f_level not in {"", "L1", "L2"}:
            f_level = ""

    # -------------------------
    # BUILD QUERY
    # -------------------------
    where = []
    params = []

    if search:
        where.append("(code LIKE %s OR name LIKE %s)")
        like = f"%{search}%"
        params.extend([like, like])

    if f_db_type:
        where.append("db_type=%s")
        params.append(f_db_type)

    if f_status:
        where.append("status=%s")
        params.append(f_status)

    if f_level:
        where.append("level=%s")
        params.append(f_level)

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    sql = (
        "SELECT benchmark_id, code, name, db_type, version, level, status, updated_at "
        "FROM benchmarks "
        f"{where_sql} "
        "ORDER BY updated_at DESC, benchmark_id DESC"
    )

    cur.execute(sql, params)
    rows = cur.fetchall() or []

    return render_template(
        "benchmarks/list.html",
        benchmarks=rows,
        search=search,
        f_db_type=f_db_type,
        f_status=f_status,
        f_level=f_level
    )


@benchmarks_bp.route("/new", methods=["GET", "POST"])
@login_required
def new_benchmark():
    if request.method == "POST":
        code = (request.form.get("code") or "").strip()
        name = (request.form.get("name") or "").strip()
        db_type = (request.form.get("db_type") or "").strip().lower()
        version = (request.form.get("version") or "").strip() or None
        level = (request.form.get("level") or "L1").strip().upper()
        status = (request.form.get("status") or "ACTIVE").strip().upper()
        description = (request.form.get("description") or "").strip() or None
        notes = (request.form.get("notes") or "").strip() or None

        # basic validation
        if not code or not name:
            flash("Code and Name are required.", "warning")
            return render_template("benchmarks/form.html", bm=request.form, is_new=True)

        if db_type not in {"oracle", "mssql"}:
            flash("Invalid db_type.", "warning")
            return render_template("benchmarks/form.html", bm=request.form, is_new=True)

        if level not in {"L1", "L2"}:
            flash("Invalid level.", "warning")
            return render_template("benchmarks/form.html", bm=request.form, is_new=True)

        if status not in {"ACTIVE", "INACTIVE", "DRAFT"}:
            flash("Invalid status.", "warning")
            return render_template("benchmarks/form.html", bm=request.form, is_new=True)

        db = get_db()
        cur = db.cursor()

        try:
            cur.execute(
                "INSERT INTO benchmarks (code, name, db_type, version, level, status, description, notes) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (code, name, db_type, version, level, status, description, notes),
            )
            flash("Benchmark created.", "success")
            return redirect(url_for("benchmarks.list_benchmarks"))
        except Exception as e:
            msg = str(e)
            if "Duplicate" in msg or "duplicate" in msg:
                flash("Benchmark code already exists. Please choose a unique code.", "danger")
            else:
                flash(f"Create failed: {msg}", "danger")
            return render_template("benchmarks/form.html", bm=request.form, is_new=True)

    return render_template("benchmarks/form.html", bm={}, is_new=True)


@benchmarks_bp.route("/<int:benchmark_id>/edit", methods=["GET", "POST"])
@login_required
def edit_benchmark(benchmark_id):
    db = get_db()
    cur = db.cursor()

    cur.execute("SELECT * FROM benchmarks WHERE benchmark_id=%s", (benchmark_id,))
    bm = cur.fetchone()
    if not bm:
        flash("Benchmark not found.", "danger")
        return redirect(url_for("benchmarks.list_benchmarks"))

    if request.method == "POST":
        code = (request.form.get("code") or "").strip()
        name = (request.form.get("name") or "").strip()
        db_type = (request.form.get("db_type") or "").strip().lower()
        version = (request.form.get("version") or "").strip() or None
        level = (request.form.get("level") or "L1").strip().upper()
        status = (request.form.get("status") or "ACTIVE").strip().upper()
        description = (request.form.get("description") or "").strip() or None
        notes = (request.form.get("notes") or "").strip() or None

        if not code or not name:
            flash("Code and Name are required.", "warning")
            bm.update(request.form)
            return render_template("benchmarks/form.html", bm=bm, is_new=False)

        if db_type not in {"oracle", "mssql"}:
            flash("Invalid db_type.", "warning")
            bm.update(request.form)
            return render_template("benchmarks/form.html", bm=bm, is_new=False)

        if level not in {"L1", "L2"}:
            flash("Invalid level.", "warning")
            bm.update(request.form)
            return render_template("benchmarks/form.html", bm=bm, is_new=False)

        if status not in {"ACTIVE", "INACTIVE", "DRAFT"}:
            flash("Invalid status.", "warning")
            bm.update(request.form)
            return render_template("benchmarks/form.html", bm=bm, is_new=False)

        try:
            cur.execute(
                "UPDATE benchmarks "
                "SET code=%s, name=%s, db_type=%s, version=%s, level=%s, status=%s, "
                "    description=%s, notes=%s "
                "WHERE benchmark_id=%s",
                (code, name, db_type, version, level, status, description, notes, benchmark_id),
            )
            flash("Benchmark updated.", "success")
            # Stay on the edit screen after Save
            return redirect(url_for("benchmarks.edit_benchmark", benchmark_id=benchmark_id))
        except Exception as e:
            msg = str(e)
            if "Duplicate" in msg or "duplicate" in msg:
                flash("Benchmark code already exists. Please choose a unique code.", "danger")
            else:
                flash(f"Update failed: {msg}", "danger")
            bm.update(request.form)
            return render_template("benchmarks/form.html", bm=bm, is_new=False)

    return render_template("benchmarks/form.html", bm=bm, is_new=False)


@benchmarks_bp.route("/<int:benchmark_id>/delete", methods=["POST"])
@login_required
def delete_benchmark(benchmark_id):
    db = get_db()
    cur = db.cursor()

    cur.execute("SELECT benchmark_id FROM benchmarks WHERE benchmark_id=%s", (benchmark_id,))
    row = cur.fetchone()
    if not row:
        flash("Benchmark not found.", "danger")
        return redirect(url_for("benchmarks.list_benchmarks"))

    try:
        cur.execute("DELETE FROM benchmarks WHERE benchmark_id=%s", (benchmark_id,))
        flash("Benchmark deleted.", "success")
    except Exception as e:
        flash(f"Delete failed: {e}", "danger")

    return redirect(url_for("benchmarks.list_benchmarks"))


@benchmarks_bp.route("/<int:benchmark_id>/add-checkpoints", methods=["GET"])
@login_required
def add_checkpoints_placeholder(benchmark_id):
    # Placeholder only — real feature will be added later.
    flash("Add Checkpoints screen will be added later (not implemented yet).", "info")
    return redirect(url_for("benchmarks.edit_benchmark", benchmark_id=benchmark_id))
