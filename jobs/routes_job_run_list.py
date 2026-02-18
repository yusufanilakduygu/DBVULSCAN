# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from flask import render_template, session, redirect, url_for, flash, request
from db import get_db
from . import jobs_bp


def _require_admin():
    if "user" not in session:
        return redirect(url_for("auth.login"))
    if session.get("role") != "admin":
        flash("Admins only.", "error")
        return redirect(url_for("home"))
    return None


def _to_int(val, default=None):
    try:
        if val is None or val == "":
            return default
        return int(val)
    except Exception:
        return default


def _parse_date_yyyy_mm_dd(s: str):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


@jobs_bp.route("/jobs/job-runs")
def job_run_list():
    guard = _require_admin()
    if guard:
        return guard

    # filters (querystring)
    page = max(1, _to_int(request.args.get("page"), 1) or 1)
    job_id = _to_int(request.args.get("job_id"), None)
    job_name = (request.args.get("job_name") or "").strip()
    status = (request.args.get("status") or "").strip()
    start_date_s = (request.args.get("start_date") or "").strip()
    end_date_s = (request.args.get("end_date") or "").strip()

    start_date = _parse_date_yyyy_mm_dd(start_date_s)
    end_date = _parse_date_yyyy_mm_dd(end_date_s)

    # datetime window: [start_dt, end_dt_next)
    start_dt = datetime.combine(start_date, datetime.min.time()) if start_date else None
    end_dt_next = (
        datetime.combine(end_date, datetime.min.time()) + timedelta(days=1)
        if end_date
        else None
    )

    where = []
    params = []

    # NEW: job_id filter (for per-job run list)
    if job_id is not None:
        where.append("jr.job_id = %s")
        params.append(job_id)

    if job_name:
        where.append("j.job_name LIKE %s")
        params.append(f"%{job_name}%")

    if status and status in ("started", "success", "error"):
        where.append("jr.status = %s")
        params.append(status)

    if start_dt:
        where.append("jr.started_at >= %s")
        params.append(start_dt)

    if end_dt_next:
        where.append("jr.started_at < %s")
        params.append(end_dt_next)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    page_size = 10
    offset = (page - 1) * page_size

    con = get_db()
    try:
        with con.cursor() as cur:
            # total count
            cur.execute(
                f"""
                SELECT COUNT(*) AS cnt
                FROM job_runs jr
                JOIN jobs j ON j.job_id = jr.job_id
                {where_sql}
                """,
                tuple(params),
            )
            total = (cur.fetchone() or {}).get("cnt", 0)

            # page rows (newest first)
            cur.execute(
                f"""
                SELECT
                  jr.job_run_id,
                  jr.job_id,
                  j.job_name,
                  jr.started_at,
                  jr.finished_at,
                  jr.status
                FROM job_runs jr
                JOIN jobs j ON j.job_id = jr.job_id
                {where_sql}
                ORDER BY jr.job_run_id DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params) + (page_size, offset),
            )
            rows = cur.fetchall()
    finally:
        con.close()

    total_pages = max(1, (total + page_size - 1) // page_size)
    if page > total_pages and total_pages > 0:
        # redirect to last valid page with same filters
        args = dict(request.args)
        args["page"] = total_pages
        return redirect(url_for("jobs.job_run_list", **args))

    start_no = (offset + 1) if total > 0 else 0
    end_no = min(offset + page_size, total) if total > 0 else 0

    return render_template(
        "jobs/job_run_list.html",
        rows=rows,
        page=page,
        page_size=page_size,
        total=total,
        total_pages=total_pages,
        start_no=start_no,
        end_no=end_no,
        filters={
            "job_id": job_id,
            "job_name": job_name,
            "status": status,
            "start_date": start_date_s,
            "end_date": end_date_s,
        },
    )


@jobs_bp.route("/jobs/job-runs/<int:job_run_id>")
def job_run_detail(job_run_id: int):
    guard = _require_admin()
    if guard:
        return guard

    con = get_db()
    try:
        with con.cursor() as cur:
            cur.execute(
                """
                SELECT
                  jr.*,
                  j.job_name
                FROM job_runs jr
                JOIN jobs j ON j.job_id = jr.job_id
                WHERE jr.job_run_id=%s
                LIMIT 1
                """,
                (job_run_id,),
            )
            row = cur.fetchone()
    finally:
        con.close()

    if not row:
        flash("Job run not found.", "error")
        return redirect(url_for("jobs.job_run_list"))

    return render_template("jobs/job_run_detail.html", r=row)
