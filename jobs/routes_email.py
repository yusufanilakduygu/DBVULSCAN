# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import render_template, redirect, url_for, flash, request
from db import get_db

from . import jobs_bp
from .routes import _require_admin, _to_int  # mevcut helper'ları kullanıyoruz


def _get_job_name(job_id: int) -> str:
    con = get_db()
    with con.cursor() as cur:
        cur.execute("SELECT job_name FROM jobs WHERE job_id=%s LIMIT 1", (job_id,))
        row = cur.fetchone()
    con.close()
    return (row.get("job_name") if row else "") or ""


@jobs_bp.route("/jobs/<int:job_id>/emails")
def email_list(job_id: int):
    guard = _require_admin()
    if guard:
        return guard

    con = get_db()
    with con.cursor() as cur:
        cur.execute(
            """
            SELECT job_id, email, note
            FROM job_email_list
            WHERE job_id=%s
            ORDER BY email
            """,
            (job_id,),
        )
        rows = cur.fetchall()
    con.close()

    return render_template(
        "jobs/email_list.html",
        job_id=job_id,
        job_name=_get_job_name(job_id),
        emails=rows,
    )


@jobs_bp.route("/jobs/<int:job_id>/emails/new", methods=["GET", "POST"])
def email_new(job_id: int):
    guard = _require_admin()
    if guard:
        return guard

    if request.method == "POST":
        f = request.form
        email = (f.get("email") or "").strip()
        note = (f.get("note") or "").strip() or None

        if not email:
            flash("Email is required.", "error")
            return render_template(
                "jobs/email_edit.html",
                mode="new",
                job_id=job_id,
                job_name=_get_job_name(job_id),
                row={"job_id": job_id, "email": "", "note": note},
            )

        con = get_db()
        try:
            with con.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO job_email_list (job_id, email, note)
                    VALUES (%s, %s, %s)
                    """,
                    (job_id, email, note),
                )
            con.commit()
        except Exception:
            # PK çakışması gibi durumlarda user-friendly mesaj
            con.rollback()
            flash("This email already exists for this job.", "error")
            return render_template(
                "jobs/email_edit.html",
                mode="new",
                job_id=job_id,
                job_name=_get_job_name(job_id),
                row={"job_id": job_id, "email": email, "note": note},
            )
        finally:
            con.close()

        flash("Email added.", "success")
        return redirect(url_for("jobs.email_list", job_id=job_id))

    return render_template(
        "jobs/email_edit.html",
        mode="new",
        job_id=job_id,
        job_name=_get_job_name(job_id),
        row={"job_id": job_id, "email": "", "note": ""},
    )


@jobs_bp.route("/jobs/<int:job_id>/emails/<path:email>/edit", methods=["GET", "POST"])
def email_edit(job_id: int, email: str):
    guard = _require_admin()
    if guard:
        return guard

    email = (email or "").strip()

    con = get_db()
    with con.cursor() as cur:
        cur.execute(
            """
            SELECT job_id, email, note
            FROM job_email_list
            WHERE job_id=%s AND email=%s
            LIMIT 1
            """,
            (job_id, email),
        )
        row = cur.fetchone()
    con.close()

    if not row:
        flash("Email record not found.", "error")
        return redirect(url_for("jobs.email_list", job_id=job_id))

    if request.method == "POST":
        f = request.form
        new_email = (f.get("email") or "").strip()
        note = (f.get("note") or "").strip() or None

        if not new_email:
            flash("Email is required.", "error")
            row["note"] = note
            return render_template(
                "jobs/email_edit.html",
                mode="edit",
                job_id=job_id,
                job_name=_get_job_name(job_id),
                row=row,
            )

        con = get_db()
        try:
            with con.cursor() as cur:
                # email PK olduğu için değişebilir: eski kaydı sil + yeni ekle gibi değil,
                # tek adım UPDATE deneyelim (PK değişimi desteklenir).
                cur.execute(
                    """
                    UPDATE job_email_list
                    SET email=%s, note=%s
                    WHERE job_id=%s AND email=%s
                    """,
                    (new_email, note, job_id, email),
                )
            con.commit()
        except Exception:
            con.rollback()
            flash("Could not update (maybe duplicate email).", "error")
            row["note"] = note
            row["email"] = new_email
            return render_template(
                "jobs/email_edit.html",
                mode="edit",
                job_id=job_id,
                job_name=_get_job_name(job_id),
                row=row,
            )
        finally:
            con.close()

        flash("Email updated.", "success")
        return redirect(url_for("jobs.email_list", job_id=job_id))

    return render_template(
        "jobs/email_edit.html",
        mode="edit",
        job_id=job_id,
        job_name=_get_job_name(job_id),
        row=row,
    )


@jobs_bp.route("/jobs/<int:job_id>/emails/<path:email>/delete", methods=["POST"])
def email_delete(job_id: int, email: str):
    guard = _require_admin()
    if guard:
        return guard

    email = (email or "").strip()

    con = get_db()
    with con.cursor() as cur:
        cur.execute(
            "DELETE FROM job_email_list WHERE job_id=%s AND email=%s",
            (job_id, email),
        )
    con.commit()
    con.close()

    flash("Email deleted.", "success")
    return redirect(url_for("jobs.email_list", job_id=job_id))
