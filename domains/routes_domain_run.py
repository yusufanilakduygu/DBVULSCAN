# -*- coding: utf-8 -*-

from math import ceil
import os

from flask import redirect, render_template, request, session, url_for, send_file, abort

from db import get_db

from . import domains_bp


def _get_persisted(key: str, default: str = "") -> str:
    """Get a query param and persist it in session (domain_runs screen)."""
    if key in request.args:
        val = request.args.get(key, default) or ""
        session[f"domain_runs_{key}"] = val
        return val
    return session.get(f"domain_runs_{key}", default) or ""


@domains_bp.route("/runs/<int:domain_id>")
def list_domain_runs(domain_id: int):
    # reset filters + page
    if request.args.get("reset") == "1":
        session.pop("domain_runs_status", None)
        session.pop("domain_runs_page", None)
        return redirect(url_for("domains.list_domain_runs", domain_id=domain_id))

    f_status = _get_persisted("status", "")

    # page (persist)
    page_str = _get_persisted("page", "1")
    try:
        page = int(page_str)
    except Exception:
        page = 1
    if page < 1:
        page = 1

    per_page = 10
    offset = (page - 1) * per_page

    conditions = ["domain_id = %s"]
    params = [domain_id]

    # DB enum
    if f_status in ("incomplete", "success", "partial"):
        conditions.append("status = %s")
        params.append(f_status)

    where_clause = "WHERE " + " AND ".join(conditions)

    db = get_db()
    cur = db.cursor()

    # Domain header
    cur.execute(
        "SELECT domain_id, name FROM domains WHERE domain_id=%s",
        (domain_id,),
    )
    domain = cur.fetchone()

    # total records (for pagination)
    cur.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM domain_runs
        {where_clause}
        """,
        params,
    )
    row = cur.fetchone()
    total_records = int(row["cnt"] if isinstance(row, dict) else row[0])

    total_pages = max(1, int(ceil(total_records / per_page))) if total_records else 1
    if page > total_pages:
        page = total_pages
        offset = (page - 1) * per_page

    # page data
    cur.execute(
        f"""
        SELECT domain_run_id, domain_id, started_at, status
        FROM domain_runs
        {where_clause}
        ORDER BY started_at DESC, domain_run_id DESC
        LIMIT %s OFFSET %s
        """,
        params + [per_page, offset],
    )
    runs = cur.fetchall()

    return render_template(
        "domains/domain_runs.html",
        domain=domain,
        runs=runs,
        domain_id=domain_id,
        f_status=f_status,
        page=page,
        total_pages=total_pages,
        total_records=total_records,
        per_page=per_page,
    )


@domains_bp.route("/runs/report/summary/<int:domain_run_id>")
def report_domain_summary(domain_run_id: int):
    """
    Default: inline preview (browser shows PDF)
    Optional: ?download=1 -> attachment download
    """
    try:
        from report_service.report_domain_summary import generate
    except Exception:
        abort(500, description="report_service module not found / import error.")

    try:
        pdf_path = generate(domain_run_id)
    except FileNotFoundError as e:
        abort(404, description=str(e))
    except Exception as e:
        abort(500, description=str(e))

    filename = os.path.basename(pdf_path)
    download = request.args.get("download") == "1"

    return send_file(
        pdf_path,
        as_attachment=download,
        download_name=filename,
        mimetype="application/pdf",
    )


@domains_bp.route("/runs/report/detail/<int:domain_run_id>")
def report_domain_detail(domain_run_id: int):
    """
    Default: inline preview (browser shows PDF)
    Optional: ?download=1 -> attachment download
    """
    try:
        from report_service.report_domain_detail import generate
    except Exception:
        abort(500, description="report_service module not found / import error.")

    try:
        pdf_path = generate(domain_run_id)
    except FileNotFoundError as e:
        abort(404, description=str(e))
    except Exception as e:
        abort(500, description=str(e))

    filename = os.path.basename(pdf_path)
    download = request.args.get("download") == "1"

    return send_file(
        pdf_path,
        as_attachment=download,
        download_name=filename,
        mimetype="application/pdf",
    )
