# -*- coding: utf-8 -*-

from flask import redirect, render_template, request, session, url_for

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
    """List runs for a specific domain (domain_runs table)."""

    if request.args.get("reset") == "1":
        session.pop("domain_runs_status", None)
        return redirect(url_for("domains.list_domain_runs", domain_id=domain_id))

    f_status = _get_persisted("status", "")

    conditions = ["domain_id = %s"]
    params = [domain_id]

    if f_status in ("started", "completed", "partial"):
        conditions.append("status = %s")
        params.append(f_status)

    where_clause = "WHERE " + " AND ".join(conditions)

    db = get_db()
    cur = db.cursor()

    # Domain header (name is useful in the runs screen title)
    cur.execute(
        "SELECT domain_id, name FROM domains WHERE domain_id=%s",
        (domain_id,),
    )
    domain = cur.fetchone()

    cur.execute(
        f"""
        SELECT domain_run_id, domain_id, started_at, status
        FROM domain_runs
        {where_clause}
        ORDER BY started_at DESC, domain_run_id DESC
        """,
        params,
    )
    runs = cur.fetchall()

    return render_template(
        "domains/domain_runs.html",
        domain=domain,
        runs=runs,
        domain_id=domain_id,
        f_status=f_status,
    )
