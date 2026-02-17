# -*- coding: utf-8 -*-
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


def _parse_hour_parameter(hp: str):
    hp = (hp or "").strip()
    if not hp:
        return (0, 0)

    if ":" in hp:
        h, m = hp.split(":", 1)
        return (
            max(0, min(_to_int(h, 0), 23)),
            max(0, min(_to_int(m, 0), 59)),
        )

    return (max(0, min(_to_int(hp, 0), 23)), 0)


def _weekday_to_cron(weekday: str):
    return {
        "sunday": 0,
        "monday": 1,
        "tuesday": 2,
        "wednesday": 3,
        "thursday": 4,
        "friday": 5,
        "saturday": 6,
    }.get((weekday or "").lower())


def _get_settings_map():
    # Tüm settings'i okuyalım; hem cron template hem cron_name lazım.
    con = get_db()
    out = {}
    with con.cursor() as cur:
        cur.execute("SELECT setting_key, setting_value FROM settings")
        for r in cur.fetchall():
            out[r["setting_key"]] = (r["setting_value"] or "").strip()
    con.close()
    return out


def _generate_schedule(job_period, hour, weekday, monthday):
    hh, mm = _parse_hour_parameter(hour)
    dom, mon, dow = "*", "*", "*"

    if job_period == "weekly":
        dow = str(_weekday_to_cron(weekday) or 1)
    elif job_period == "monthly":
        d = _to_int(monthday, 1)
        dom = str(d if 1 <= d <= 28 else 1)

    return f"{mm} {hh} {dom} {mon} {dow}"


def _build_full_cron_entry(schedule_5: str, job_id: int, settings: dict):
    """
    Yeni cron formatı:
    {cron_expression} {cron_user} cd {project_root} && {python_full_path} -m jobs.run_job {job_id} --quiet >> {report_dir}/logs/job_{job_id}.log 2>&1
    """

    cron_user = (settings.get("cron_user") or "").strip()
    project_root = (settings.get("project_root") or "").strip()
    python_full_path = (settings.get("python_full_path") or "").strip()
    report_dir = (settings.get("report_dir") or "").strip()

    # Komutun sabit kısmı
    cmd = (
        f"cd {project_root} && {python_full_path} -m jobs.run_job {job_id} --quiet "
        f">> {report_dir}/logs/job_{job_id}.log 2>&1"
    ).strip()

    parts = [schedule_5, cron_user, cmd]
    return " ".join(p for p in parts if p)


def _load_job(job_id: int):
    con = get_db()
    with con.cursor() as cur:
        cur.execute("SELECT * FROM jobs WHERE job_id=%s LIMIT 1", (job_id,))
        row = cur.fetchone()
    con.close()
    return row


def _load_domains_and_assessments():
    """
    Formda job_type'a göre dropdown doldurmak için:
    - domains: domain_id, name
    - assessments: assessment_id, name
    """
    con = get_db()
    with con.cursor() as cur:
        cur.execute("SELECT domain_id, name FROM domains ORDER BY name")
        domains = cur.fetchall()

        cur.execute("SELECT assessment_id, name FROM assessments ORDER BY name")
        assessments = cur.fetchall()
    con.close()
    return domains, assessments


@jobs_bp.route("/jobs")
def list_jobs():
    guard = _require_admin()
    if guard:
        return guard

    con = get_db()
    with con.cursor() as cur:
        cur.execute(
            """
            SELECT
              j.*,
              CASE
                WHEN j.job_type='assessment_run' THEN a.name
                WHEN j.job_type='domain_run' THEN d.name
                ELSE NULL
              END AS parameter_name
            FROM jobs j
            LEFT JOIN assessments a
              ON j.job_type='assessment_run'
             AND a.assessment_id = j.parameter
            LEFT JOIN domains d
              ON j.job_type='domain_run'
             AND d.domain_id = j.parameter
            ORDER BY j.job_id DESC
            """
        )
        jobs = cur.fetchall()
    con.close()

    return render_template("jobs/list.html", jobs=jobs)


@jobs_bp.route("/jobs/new", methods=["GET", "POST"])
def new_job():
    guard = _require_admin()
    if guard:
        return guard

    settings = _get_settings_map()

    if request.method == "POST":
        f = request.form
        parameter = _to_int(f.get("parameter"), 0)
        is_active = 1 if f.get("is_active") else 0

        schedule = _generate_schedule(
            f.get("job_period"),
            f.get("hour_parameter"),
            f.get("weekday_parameter"),
            f.get("monthday_parameter"),
        )

        con = get_db()
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO jobs
                (job_name, job_type, parameter, job_period,
                 hour_parameter, weekday_parameter, monthday_parameter,
                 send_mail, is_active, note, cron_entry)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    f.get("job_name"),
                    f.get("job_type"),
                    parameter,
                    f.get("job_period"),
                    f.get("hour_parameter"),
                    f.get("weekday_parameter"),
                    _to_int(f.get("monthday_parameter"), None),
                    1 if f.get("send_mail") else 0,
                    is_active,
                    f.get("note"),
                    "",  # job_id INSERT sonrası oluşacağı için cron_entry sonra güncellenir
                ),
            )
            new_id = cur.lastrowid

            # Save sırasında her zaman üret (is_active üretimi etkilemez)
            cron_entry = _build_full_cron_entry(schedule, new_id, settings)
            cur.execute(
                "UPDATE jobs SET cron_entry=%s WHERE job_id=%s",
                (cron_entry, new_id),
            )
        con.commit()
        con.close()

        flash("Job created.", "success")
        return redirect(url_for("jobs.edit_job", job_id=new_id))

    # NEW form açıldığında cron boş
    domains, assessments = _load_domains_and_assessments()
    return render_template(
        "jobs/form.html",
        mode="new",
        job=None,
        cron_preview="",
        domains=domains,
        assessments=assessments,
    )


@jobs_bp.route("/jobs/<int:job_id>/edit", methods=["GET", "POST"])
def edit_job(job_id):
    guard = _require_admin()
    if guard:
        return guard

    settings = _get_settings_map()
    job = _load_job(job_id)

    if not job:
        flash("Job not found.", "error")
        return redirect(url_for("jobs.list_jobs"))

    if request.method == "POST":
        f = request.form
        parameter = _to_int(f.get("parameter"), 0)
        is_active = 1 if f.get("is_active") else 0

        schedule = _generate_schedule(
            f.get("job_period"),
            f.get("hour_parameter"),
            f.get("weekday_parameter"),
            f.get("monthday_parameter"),
        )

        cron_entry = _build_full_cron_entry(schedule, job_id, settings)

        con = get_db()
        with con.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs SET
                  job_name=%s,
                  job_type=%s,
                  parameter=%s,
                  job_period=%s,
                  hour_parameter=%s,
                  weekday_parameter=%s,
                  monthday_parameter=%s,
                  send_mail=%s,
                  is_active=%s,
                  note=%s,
                  cron_entry=%s
                WHERE job_id=%s
                """,
                (
                    f.get("job_name"),
                    f.get("job_type"),
                    parameter,
                    f.get("job_period"),
                    f.get("hour_parameter"),
                    f.get("weekday_parameter"),
                    _to_int(f.get("monthday_parameter"), None),
                    1 if f.get("send_mail") else 0,
                    is_active,
                    f.get("note"),
                    cron_entry,
                    job_id,
                ),
            )
        con.commit()
        con.close()

        flash("Job updated.", "success")
        job = _load_job(job_id)

    # Edit ekranda preview her zaman üretilebilir (schedule + settings + parameter)
    schedule = _generate_schedule(
        job.get("job_period"),
        job.get("hour_parameter"),
        job.get("weekday_parameter"),
        job.get("monthday_parameter"),
    )
    preview = _build_full_cron_entry(schedule, job_id, settings)

    domains, assessments = _load_domains_and_assessments()
    return render_template(
        "jobs/form.html",
        mode="edit",
        job=job,
        cron_preview=preview,
        domains=domains,
        assessments=assessments,
    )


@jobs_bp.route("/jobs/<int:job_id>/delete", methods=["POST"])
def delete_job(job_id):
    guard = _require_admin()
    if guard:
        return guard

    con = get_db()
    with con.cursor() as cur:
        cur.execute("DELETE FROM jobs WHERE job_id=%s", (job_id,))
    con.commit()
    con.close()

    flash("Job deleted.", "success")
    return redirect(url_for("jobs.list_jobs"))
