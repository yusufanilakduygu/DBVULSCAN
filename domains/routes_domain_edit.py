# -*- coding: utf-8 -*-

from flask import flash, redirect, render_template, request, url_for

from db import get_db

from . import domains_bp


def _fetch_domain(cur, domain_id: int):
    cur.execute(
        """
        SELECT domain_id, name, description, is_active, created_at, updated_at
        FROM domains
        WHERE domain_id=%s
        """,
        (domain_id,),
    )
    return cur.fetchone()


def _fetch_domain_assessments(cur, domain_id: int):
    """Assessments linked to this domain (read-only list on edit screen)."""
    cur.execute(
        """
        SELECT
            assessment_id,
            name,
            datasource_name,
            benchmark_name,
            db_type,
            status
        FROM assessments
        WHERE domain_id=%s
        ORDER BY assessment_id DESC
        """,
        (domain_id,),
    )
    return cur.fetchall()


@domains_bp.route("/edit/<int:domain_id>", methods=["GET", "POST"])
def edit_domain(domain_id: int):
    """Edit an existing domain + show its assessments list."""
    db = get_db()
    cur = db.cursor()

    domain = _fetch_domain(cur, domain_id)
    if not domain:
        flash("Domain not found.", "error")
        return redirect(url_for("domains.list_domains"))

    assessments = _fetch_domain_assessments(cur, domain_id)

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        description = (request.form.get("description") or "").strip()
        is_active = 1 if request.form.get("is_active") == "1" else 0

        if not name:
            flash("Name is required.", "error")
            return render_template(
                "domains/form.html",
                domain=domain,
                assessments=assessments,
            )

        try:
            cur.execute(
                """
                UPDATE domains
                SET name=%s,
                    description=%s,
                    is_active=%s,
                    updated_at=NOW()
                WHERE domain_id=%s
                """,
                (name, description, is_active, domain_id),
            )
            db.commit()

            flash("Domain updated.", "success")

            # ✅ Save sonrası aynı ekranda kalmak için: redirect YOK
            # Güncel veriyi tekrar çekip aynı template'i render ediyoruz.
            domain = _fetch_domain(cur, domain_id)
            assessments = _fetch_domain_assessments(cur, domain_id)

            return render_template(
                "domains/form.html",
                domain=domain,
                assessments=assessments,
            )

        except Exception as e:
            db.rollback()
            flash(f"Update failed: {e}", "error")

            # Hata sonrası ekranda kal + güncel snapshot
            domain = _fetch_domain(cur, domain_id)
            assessments = _fetch_domain_assessments(cur, domain_id)

            return render_template(
                "domains/form.html",
                domain=domain,
                assessments=assessments,
            )

    return render_template(
        "domains/form.html",
        domain=domain,
        assessments=assessments,
    )
