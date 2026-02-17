# -*- coding: utf-8 -*-
"""jobs/run_job.py

Cron tarafından çağrılacak job runner.

Kullanım (ÖNERİLEN):
    cd /home/anil/dbvulscan
    /home/anil/dbvulscan/venv/bin/python -m jobs.run_job <job_id>

Notlar:
- Rapor üretimi Flask render_template kullandığı için app_context + request_context gerekir
  (app.py içindeki inject_user -> session kullanıyor).
- job_runs insert/update için commit zorunlu (autocommit kapalı olabiliyor).
- Debug için stdout/stderr hem console'a hem DB'ye yazılır (TEE).
"""

from __future__ import annotations

import argparse
import io
import smtplib
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import IO, List, Optional, Tuple

from db import get_db


# -----------------------------
# TEE (console + buffer)
# -----------------------------
class TeeWriter:
    def __init__(self, console: IO[str], buffer: io.StringIO) -> None:
        self._console = console
        self._buffer = buffer

    def write(self, s: str) -> int:
        try:
            self._console.write(s)
        except Exception:
            pass
        try:
            return self._buffer.write(s)
        except Exception:
            return 0

    def flush(self) -> None:
        try:
            self._console.flush()
        except Exception:
            pass
        try:
            self._buffer.flush()
        except Exception:
            pass


class _Redirect:
    def __init__(self, stdout_obj: IO[str], stderr_obj: IO[str]) -> None:
        self._stdout_obj = stdout_obj
        self._stderr_obj = stderr_obj
        self._old_out: Optional[IO[str]] = None
        self._old_err: Optional[IO[str]] = None

    def __enter__(self):
        self._old_out = sys.stdout
        self._old_err = sys.stderr
        sys.stdout = self._stdout_obj  # type: ignore
        sys.stderr = self._stderr_obj  # type: ignore
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._old_out is not None:
            sys.stdout = self._old_out  # type: ignore
        if self._old_err is not None:
            sys.stderr = self._old_err  # type: ignore
        return False


# -----------------------------
# DB HELPERS
# -----------------------------
def _get_setting_value(setting_key: str) -> Optional[str]:
    con = None
    try:
        con = get_db()
        with con.cursor() as cur:
            cur.execute(
                """
                SELECT setting_value
                FROM settings
                WHERE setting_key=%s
                LIMIT 1
                """,
                (setting_key,),
            )
            row = cur.fetchone()
            return row["setting_value"] if row else None
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass


def _truthy(val: Optional[str]) -> bool:
    if val is None:
        return False
    v = str(val).strip().lower()
    return v in {"1", "true", "yes", "on", "enabled"}


@dataclass
class JobRow:
    job_id: int
    job_name: str
    job_type: str
    parameter: int
    send_mail: int
    is_active: int


def _load_job(job_id: int) -> Optional[JobRow]:
    con = None
    try:
        con = get_db()
        with con.cursor() as cur:
            cur.execute(
                """
                SELECT job_id, job_name, job_type, parameter, send_mail, is_active
                FROM jobs
                WHERE job_id=%s
                LIMIT 1
                """,
                (job_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return JobRow(
                job_id=int(row["job_id"]),
                job_name=str(row.get("job_name") or ""),
                job_type=str(row.get("job_type") or ""),
                parameter=int(row.get("parameter") or 0),
                send_mail=int(row.get("send_mail") or 0),
                is_active=int(row.get("is_active") or 0),
            )
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass


def _get_job_emails(job_id: int) -> List[str]:
    con = None
    try:
        con = get_db()
        with con.cursor() as cur:
            cur.execute(
                """
                SELECT email
                FROM job_email_list
                WHERE job_id=%s
                ORDER BY email
                """,
                (job_id,),
            )
            rows = cur.fetchall() or []
            emails: List[str] = []
            for r in rows:
                e = (r.get("email") if isinstance(r, dict) else None) or ""
                e = str(e).strip()
                if e:
                    emails.append(e)
            return emails
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass


# -----------------------------
# JOB_RUNS LOGGING (COMMIT ✅)
# -----------------------------
def _insert_job_run(job_id: int) -> int:
    con = None
    try:
        con = get_db()
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO job_runs (job_id, started_at, status)
                VALUES (%s, NOW(), 'started')
                """,
                (job_id,),
            )
            job_run_id = cur.lastrowid

        con.commit()
        return int(job_run_id)
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass


def _finalize_job_run(
    job_run_id: int,
    status: str,
    filename: Optional[str],
    stdout_text: Optional[str],
    stderr_text: Optional[str],
) -> None:
    """
    job_runs tablosu:
      PRIMARY KEY (job_run_id)
      status ENUM('started','success','error')  (disabled isteniyorsa DB ALTER gerekir)

    - 'id' kolonu yok => WHERE id fallback'i YOK.
    - status='disabled' DB enum yüzünden hata verirse: aynı kaydı status='error' ile finalize etmeyi dener
      (en azından 'started' kalmasın).
    """
    con = None

    def _do_update(cur, st: str) -> None:
        cur.execute(
            """
            UPDATE job_runs
               SET finished_at = NOW(),
                   status      = %s,
                   filename    = %s,
                   stdout      = %s,
                   stderr      = %s
             WHERE job_run_id  = %s
             LIMIT 1
            """,
            (st, filename, stdout_text, stderr_text, job_run_id),
        )

    try:
        con = get_db()
        with con.cursor() as cur:
            try:
                _do_update(cur, status)
            except Exception:
                # Eğer disabled DB enum'unda yoksa fallback
                if str(status).lower() == "disabled":
                    # retry with 'error' to avoid leaving it started
                    _do_update(cur, "error")
                else:
                    raise

        con.commit()
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass


# -----------------------------
# FLASK APP CONTEXT ✅
# -----------------------------
def _get_app():
    from app import create_app

    return create_app()


def _run_with_flask_context(fn):
    """
    Rapor üretimi için:
    - app_context
    - test_request_context (session gerektiren inject_user vb. için)
    """
    from flask import session

    app = _get_app()
    with app.app_context():
        with app.test_request_context("/"):
            if "user" not in session:
                session["user"] = {}
            return fn()


# -----------------------------
# SMTP
# -----------------------------
def _send_mail(
    to_list: List[str],
    subject: str,
    body_text: str,
    attachments: Optional[List[Path]] = None,
) -> None:
    smtp_enabled = _get_setting_value("smtp_enabled")
    if smtp_enabled is not None and not _truthy(smtp_enabled):
        return

    host = (_get_setting_value("smtp_host") or "").strip()
    port_raw = (_get_setting_value("smtp_port") or "").strip()
    security = (_get_setting_value("smtp_security") or "none").strip().lower()
    username = (_get_setting_value("smtp_username") or "").strip()
    password = (_get_setting_value("smtp_password") or "").strip()
    from_addr = (_get_setting_value("smtp_from_address") or username).strip()
    from_name = (_get_setting_value("smtp_from_name") or "OmniRiskDB").strip()

    if not host or not port_raw:
        raise RuntimeError("SMTP host/port eksik (settings: smtp_host, smtp_port)")
    try:
        port = int(port_raw)
    except ValueError as e:
        raise RuntimeError("SMTP port numeric olmalı (settings: smtp_port)") from e

    if not from_addr:
        raise RuntimeError("From address eksik (settings: smtp_from_address veya smtp_username)")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{from_addr}>" if from_name else from_addr
    msg["To"] = ", ".join(to_list)
    msg.set_content(body_text)

    for p in attachments or []:
        if not p.exists() or not p.is_file():
            continue
        data = p.read_bytes()
        msg.add_attachment(data, maintype="application", subtype="pdf", filename=p.name)

    if security == "ssl":
        server = smtplib.SMTP_SSL(host, port, timeout=30)
    else:
        server = smtplib.SMTP(host, port, timeout=30)
    try:
        server.ehlo()
        if security == "starttls":
            server.starttls()
            server.ehlo()

        if username:
            server.login(username, password)

        server.send_message(msg)
    finally:
        try:
            server.quit()
        except Exception:
            pass


# -----------------------------
# JOB EXECUTION
# -----------------------------
def _run_assessment_job(assessment_id: int) -> Tuple[str, str]:
    from run_assessment import run_assessment
    from report_service import report_assessment_detail

    run_id, run_month = run_assessment(int(assessment_id))

    def _gen():
        return report_assessment_detail.generate(int(run_id))

    pdf_path = _run_with_flask_context(_gen)

    return f"assessment_run run_id={run_id} run_month={run_month}", str(pdf_path)


def _run_domain_job(domain_id: int) -> Tuple[str, str]:
    from domains.domain_run import run_domain
    from report_service import report_domain_detail

    domain_run_id, final_status = run_domain(int(domain_id))

    def _gen():
        return report_domain_detail.generate(int(domain_run_id))

    pdf_path = _run_with_flask_context(_gen)

    return f"domain_run domain_run_id={domain_run_id} status={final_status}", str(pdf_path)


def run_job(job_id: int, quiet: bool = False) -> int:
    job_run_id = _insert_job_run(job_id)

    out_buf = io.StringIO()
    err_buf = io.StringIO()

    if quiet:
        out_console: IO[str] = io.StringIO()
        err_console: IO[str] = io.StringIO()
    else:
        out_console = sys.__stdout__
        err_console = sys.__stderr__

    tee_out = TeeWriter(out_console, out_buf)
    tee_err = TeeWriter(err_console, err_buf)

    final_status = "success"
    final_file: Optional[str] = None
    already_finalized = False

    try:
        with _Redirect(tee_out, tee_err):
            print(f"[run_job] started job_id={job_id} job_run_id={job_run_id}")

            job = _load_job(job_id)
            if not job:
                raise RuntimeError(f"jobs tablosunda job_id={job_id} yok")

            print(f"[run_job] job_id={job.job_id} name='{job.job_name}' type={job.job_type} param={job.parameter}")

            # Inactive ise: job çalıştırma yok; status=disabled olmalı.
            if int(job.is_active) != 1:
                print("[run_job] job is inactive (is_active!=1). skipped.")
                final_status = "disabled"

                # İstersen hemen finalize dene ama hata olursa ERROR'a düşürme:
                try:
                    try:
                        tee_out.flush()
                        tee_err.flush()
                    except Exception:
                        pass

                    _finalize_job_run(
                        job_run_id=job_run_id,
                        status=final_status,
                        filename=None,
                        stdout_text=out_buf.getvalue() or None,
                        stderr_text=err_buf.getvalue() or None,
                    )
                    already_finalized = True
                except Exception:
                    # Hata olursa sadece logla; status'u error'a çevirmiyoruz.
                    try:
                        tee_err.write("[run_job] WARNING: early finalize failed (inactive job)\n")
                        tee_err.write(traceback.format_exc() + "\n")
                        tee_err.flush()
                    except Exception:
                        pass

                return 0

            status_line = ""
            pdf_path = ""

            if job.job_type == "assessment_run":
                status_line, pdf_path = _run_assessment_job(job.parameter)
            elif job.job_type == "domain_run":
                status_line, pdf_path = _run_domain_job(job.parameter)
            else:
                raise RuntimeError(f"Unsupported job_type: {job.job_type}")

            print(f"[run_job] {status_line}")
            final_file = pdf_path

            if int(job.send_mail) == 1:
                emails = _get_job_emails(job.job_id)
                if not emails:
                    print("[run_job] send_mail=1 ama job_email_list boş. mail atlanıyor.")
                else:
                    subject = f"OmniRiskDB Job Result - {job.job_name}"
                    body = (
                        f"Job: {job.job_name} (job_id={job.job_id})\n"
                        f"Type: {job.job_type}\n"
                        f"Parameter: {job.parameter}\n"
                        f"Result: {status_line}\n"
                        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    )
                    print(f"[run_job] sending mail to: {', '.join(emails)}")
                    _send_mail(emails, subject, body, attachments=[Path(pdf_path)] if pdf_path else None)
            else:
                print("[run_job] send_mail=0. mail atlanıyor.")

        return 0

    except Exception:
        final_status = "error"
        tb = traceback.format_exc()
        try:
            tee_err.write("\n" + tb + "\n")
            tee_err.flush()
        except Exception:
            pass
        return 2

    finally:
        # ✅ finally içinde kesinlikle return yok!
        if already_finalized:
            pass
        else:
            try:
                _finalize_job_run(
                    job_run_id=job_run_id,
                    status=final_status,
                    filename=final_file,
                    stdout_text=out_buf.getvalue() or None,
                    stderr_text=err_buf.getvalue() or None,
                )
                if not quiet:
                    sys.__stdout__.write(f"[run_job] finalized job_run_id={job_run_id} status={final_status}\n")
                    sys.__stdout__.flush()
            except Exception:
                # QUIET olsa bile cron log'una düşsün diye sys.__stderr__ yazıyoruz
                try:
                    sys.__stderr__.write("[run_job] WARNING: job_runs finalize failed\n")
                    sys.__stderr__.write(traceback.format_exc() + "\n")
                    sys.__stderr__.flush()
                except Exception:
                    pass


def _parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DBVulScan cron job runner")
    p.add_argument("job_id", type=int, help="jobs.job_id")
    p.add_argument("--quiet", action="store_true", help="console output kapat (cron için)")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    ns = _parse_args(argv or sys.argv[1:])
    return run_job(int(ns.job_id), quiet=bool(ns.quiet))


if __name__ == "__main__":
    raise SystemExit(main())
