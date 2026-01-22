# /home/anil/dbvulscan/domains/domain_run.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import List, Tuple

from db import get_db
from run_assessment import run_assessment


def _get_domain_assessment_ids(domain_id: int) -> List[int]:
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """
        SELECT assessment_id
        FROM assessments
        WHERE domain_id = %s
        ORDER BY assessment_id
        """,
        (domain_id,),
    )
    rows = cur.fetchall() or []
    return [int(r[0]) for r in rows]


def _create_domain_run(domain_id: int) -> int:
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """
        INSERT INTO domain_runs (domain_id, status)
        VALUES (%s, 'incomplete')
        """,
        (domain_id,),
    )
    domain_run_id = cur.lastrowid
    db.commit()
    return int(domain_run_id)


def _link_domain_run_assessment(domain_run_id: int, assessment_run_id: int) -> None:
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """
        INSERT INTO domain_run_assessments (domain_run_id, assessment_run_id)
        VALUES (%s, %s)
        """,
        (domain_run_id, assessment_run_id),
    )
    db.commit()


def _get_assessment_run_status(run_id: int, run_month: int) -> str:
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """
        SELECT status
        FROM assessment_runs
        WHERE run_id = %s AND run_month = %s
        LIMIT 1
        """,
        (run_id, run_month),
    )
    row = cur.fetchone()
    if not row:
        # Normalde olmamalı; ama fail-safe:
        return "incomplete"
    return str(row[0])


def _finalize_domain_run(domain_run_id: int, status: str) -> None:
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """
        UPDATE domain_runs
        SET status = %s
        WHERE domain_run_id = %s
        """,
        (status, domain_run_id),
    )
    db.commit()


def run_domain(domain_id: int) -> Tuple[int, str]:
    """
    domain_id altındaki tüm assessment'ları sırayla çalıştırır.

    Kurallar:
    - domain_run başlangıçta 'incomplete'
    - assessment_run status DB'den okunur (run_assessment status döndürmez)
    - herhangi bir assessment status != 'success' ise domain_run = 'partial'
      (özellikle 'incomplete' veya 'unreachable' ise)
    - tüm assessment'lar 'success' ise domain_run = 'success'

    Returns:
        (domain_run_id, final_status)
    """
    domain_run_id = _create_domain_run(domain_id)

    assessment_ids = _get_domain_assessment_ids(domain_id)

    # Eğer domain altında hiç assessment yoksa "partial" bırakıyoruz.
    # (İstersen bunu 'success' yapabiliriz ama kuralı sen netleştirmen gerekir.)
    final_status = "partial" if len(assessment_ids) == 0 else "success"

    try:
        for assessment_id in assessment_ids:
            run_id, run_month = run_assessment(int(assessment_id))

            # domain_run -> assessment_run bağla
            _link_domain_run_assessment(domain_run_id, int(run_id))

            # assessment status DB'den okunur
            a_status = _get_assessment_run_status(int(run_id), int(run_month))

            # Kurala göre:
            # incomplete/unreachable (veya error) görürsek domain_run artık success olamaz.
            if a_status != "success":
                final_status = "partial"

        _finalize_domain_run(domain_run_id, final_status)
        return domain_run_id, final_status

    except Exception:
        # Domain_run tablosunda 'error' enum'u yok; güvenli şekilde partial'a çekiyoruz.
        try:
            _finalize_domain_run(domain_run_id, "partial")
        except Exception:
            pass
        raise
