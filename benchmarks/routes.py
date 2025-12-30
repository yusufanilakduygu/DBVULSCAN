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

    # RESET (clear filters)
    if request.args.get("reset") == "1":
        session.pop("bm_search", None)
        session.pop("bm_f_db_type", None)
        session.pop("bm_f_status", None)
        session.pop("bm_f_level", None)
        return redirect(url_for("benchmarks.list_benchmarks"))

    # SEARCH (persist via session)
    q_param = request.args.get("q")
    if q_param is not None:
        search = (q_param or "").strip()
        session["bm_search"] = search
    else:
        search = (session.get("bm_search") or "").strip()

    # FILTERS (persist via session)
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

    def _load_mapped_checkpoints():
        cur.execute(
            "SELECT c.Id, c.Name, c.Category, c.Severity, bc.sort_order "
            "FROM benchmark_checkpoints bc "
            "JOIN checkpoints c ON c.Id = bc.checkpoint_id "
            "WHERE bc.benchmark_id=%s "
            "ORDER BY bc.sort_order ASC, c.Id ASC",
            (benchmark_id,),
        )
        return cur.fetchall() or []

    cur.execute("SELECT * FROM benchmarks WHERE benchmark_id=%s", (benchmark_id,))
    bm = cur.fetchone()
    if not bm:
        flash("Benchmark not found.", "danger")
        return redirect(url_for("benchmarks.list_benchmarks"))

    mapped_checkpoints = _load_mapped_checkpoints()

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
            return render_template("benchmarks/form.html", bm=bm, is_new=False, mapped_checkpoints=mapped_checkpoints)

        if db_type not in {"oracle", "mssql"}:
            flash("Invalid db_type.", "warning")
            bm.update(request.form)
            return render_template("benchmarks/form.html", bm=bm, is_new=False, mapped_checkpoints=mapped_checkpoints)

        if level not in {"L1", "L2"}:
            flash("Invalid level.", "warning")
            bm.update(request.form)
            return render_template("benchmarks/form.html", bm=bm, is_new=False, mapped_checkpoints=mapped_checkpoints)

        if status not in {"ACTIVE", "INACTIVE", "DRAFT"}:
            flash("Invalid status.", "warning")
            bm.update(request.form)
            return render_template("benchmarks/form.html", bm=bm, is_new=False, mapped_checkpoints=mapped_checkpoints)

        try:
            cur.execute(
                "UPDATE benchmarks "
                "SET code=%s, name=%s, db_type=%s, version=%s, level=%s, status=%s, "
                "    description=%s, notes=%s "
                "WHERE benchmark_id=%s",
                (code, name, db_type, version, level, status, description, notes, benchmark_id),
            )

            # Optional: update checkpoint order from edit screen
            raw_order = (request.form.get("checkpoint_order") or "").strip()
            if raw_order:
                order_ids = []
                seen = set()
                for part in raw_order.split(","):
                    part = part.strip()
                    if not part:
                        continue
                    try:
                        cid = int(part)
                    except Exception:
                        continue
                    if cid not in seen:
                        seen.add(cid)
                        order_ids.append(cid)

                # Keep only IDs currently mapped to this benchmark
                cur.execute(
                    "SELECT checkpoint_id FROM benchmark_checkpoints WHERE benchmark_id=%s",
                    (benchmark_id,),
                )
                current_ids = [r.get("checkpoint_id") if isinstance(r, dict) else r[0] for r in (cur.fetchall() or [])]
                current_set = set(current_ids)

                ordered = [cid for cid in order_ids if cid in current_set]

                # Append missing ones at end (safety)
                ordered_set = set(ordered)
                for cid in current_ids:
                    if cid not in ordered_set:
                        ordered.append(cid)

                upd_sql = "UPDATE benchmark_checkpoints SET sort_order=%s WHERE benchmark_id=%s AND checkpoint_id=%s"
                cur.executemany(
                    upd_sql,
                    [(i + 1, benchmark_id, cid) for i, cid in enumerate(ordered)],
                )

            flash("Benchmark updated.", "success")
            return redirect(url_for("benchmarks.edit_benchmark", benchmark_id=benchmark_id))
        except Exception as e:
            msg = str(e)
            if "Duplicate" in msg or "duplicate" in msg:
                flash("Benchmark code already exists. Please choose a unique code.", "danger")
            else:
                flash(f"Update failed: {msg}", "danger")
            bm.update(request.form)
            mapped_checkpoints = _load_mapped_checkpoints()
            return render_template("benchmarks/form.html", bm=bm, is_new=False, mapped_checkpoints=mapped_checkpoints)

    return render_template("benchmarks/form.html", bm=bm, is_new=False, mapped_checkpoints=mapped_checkpoints)


# ✅ NEW: quick delete a mapped checkpoint from edit page
@benchmarks_bp.route("/<int:benchmark_id>/checkpoints/<int:checkpoint_id>/delete", methods=["POST"])
@login_required
def delete_benchmark_checkpoint(benchmark_id, checkpoint_id):
    db = get_db()
    cur = db.cursor()

    # delete mapping
    cur.execute(
        "DELETE FROM benchmark_checkpoints WHERE benchmark_id=%s AND checkpoint_id=%s",
        (benchmark_id, checkpoint_id),
    )

    # re-pack sort_order 1..n
    cur.execute(
        "SELECT checkpoint_id "
        "FROM benchmark_checkpoints "
        "WHERE benchmark_id=%s "
        "ORDER BY sort_order ASC, checkpoint_id ASC",
        (benchmark_id,),
    )
    remaining = cur.fetchall() or []
    remaining_ids = [r.get("checkpoint_id") if isinstance(r, dict) else r[0] for r in remaining]

    if remaining_ids:
        upd_sql = "UPDATE benchmark_checkpoints SET sort_order=%s WHERE benchmark_id=%s AND checkpoint_id=%s"
        cur.executemany(
            upd_sql,
            [(i + 1, benchmark_id, cid) for i, cid in enumerate(remaining_ids)],
        )

    flash("Checkpoint removed from benchmark.", "success")
    return redirect(url_for("benchmarks.edit_benchmark", benchmark_id=benchmark_id))


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


# =========================================================
# Checkpoints mapping screen (dual list)
# =========================================================

@benchmarks_bp.route("/<int:benchmark_id>/checkpoints", methods=["GET", "POST"])
@login_required
def edit_benchmark_checkpoints(benchmark_id):
    db = get_db()
    cur = db.cursor()

    # Load benchmark
    cur.execute(
        "SELECT benchmark_id, code, name, db_type, level, status, version FROM benchmarks WHERE benchmark_id=%s",
        (benchmark_id,),
    )
    bm = cur.fetchone()
    if not bm:
        flash("Benchmark not found.", "danger")
        return redirect(url_for("benchmarks.list_benchmarks"))

    # Persist search/filter in session
    if request.args.get("reset") == "1":
        session.pop("bmcp_search", None)
        session.pop("bmcp_category", None)
        session.pop("bmcp_severity", None)
        return redirect(url_for("benchmarks.edit_benchmark_checkpoints", benchmark_id=benchmark_id))

    q_param = request.args.get("q")
    if q_param is not None:
        search = (q_param or "").strip()
        session["bmcp_search"] = search
    else:
        search = (session.get("bmcp_search") or "").strip()

    if "category" in request.args:
        f_category = (request.args.get("category") or "").strip().upper()
        allowed_cat = {"", "AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"}
        if f_category not in allowed_cat:
            f_category = ""
        session["bmcp_category"] = f_category
    else:
        f_category = (session.get("bmcp_category") or "").strip().upper()

    if "severity" in request.args:
        f_severity = (request.args.get("severity") or "").strip().lower()
        allowed_sev = {"", "info", "caution", "minor", "major", "critical"}
        if f_severity not in allowed_sev:
            f_severity = ""
        session["bmcp_severity"] = f_severity
    else:
        f_severity = (session.get("bmcp_severity") or "").strip().lower()

    # POST: save mapping
    if request.method == "POST":
        raw_ids = request.form.getlist("selected_ids")
        selected_ids = []
        seen = set()
        for x in raw_ids:
            try:
                cid = int(x)
            except Exception:
                continue
            if cid not in seen:
                seen.add(cid)
                selected_ids.append(cid)

        # (Optional) db_type validation can stay as-is in your project
        try:
            cur.execute("DELETE FROM benchmark_checkpoints WHERE benchmark_id=%s", (benchmark_id,))

            if selected_ids:
                insert_sql = (
                    "INSERT INTO benchmark_checkpoints (benchmark_id, checkpoint_id, sort_order, notes, added_at) "
                    "VALUES (%s,%s,%s,%s,NOW())"
                )
                data = [(benchmark_id, cid, i + 1, None) for i, cid in enumerate(selected_ids)]
                cur.executemany(insert_sql, data)

            flash("Checkpoints mapped to benchmark.", "success")
            return redirect(url_for("benchmarks.edit_benchmark_checkpoints", benchmark_id=benchmark_id))
        except Exception as e:
            flash(f"Save failed: {e}", "danger")
            return redirect(url_for("benchmarks.edit_benchmark_checkpoints", benchmark_id=benchmark_id))

    # GET: load selected + available lists
    cur.execute(
        "SELECT c.Id, c.Name, c.Severity, c.Category "
        "FROM benchmark_checkpoints bc "
        "JOIN checkpoints c ON c.Id = bc.checkpoint_id "
        "WHERE bc.benchmark_id=%s "
        "ORDER BY bc.sort_order ASC, c.Id ASC",
        (benchmark_id,),
    )
    selected = cur.fetchall() or []
    selected_ids = [r.get("Id") if isinstance(r, dict) else r[0] for r in selected]

    where = ["c.DB_Type=%s"]
    params = [bm.get("db_type") if isinstance(bm, dict) else bm["db_type"]]

    if search:
        where.append("(c.Name LIKE %s OR c.Id LIKE %s)")
        like = f"%{search}%"
        params.extend([like, like])

    if f_category:
        where.append("c.Category=%s")
        params.append(f_category)

    if f_severity:
        where.append("c.Severity=%s")
        params.append(f_severity)

    if selected_ids:
        placeholders = ",".join(["%s"] * len(selected_ids))
        where.append(f"c.Id NOT IN ({placeholders})")
        params.extend(selected_ids)

    where_sql = " AND ".join(where)
    cur.execute(
        "SELECT c.Id, c.Name, c.Severity, c.Category "
        "FROM checkpoints c "
        f"WHERE {where_sql} "
        "ORDER BY c.Category ASC, c.Severity DESC, c.Id ASC",
        params,
    )
    available = cur.fetchall() or []

    return render_template(
        "benchmarks/checkpoints_map.html",
        bm=bm,
        available=available,
        selected=selected,
        search=search,
        f_category=f_category,
        f_severity=f_severity,
    )
