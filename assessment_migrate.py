# -*- coding: utf-8 -*-
"""
assessment_migrate.py

DBVulScan - assessment_migrate tablosundan geçmiş tarihli assessment run simulasyonu üretir.

KAYNAK:
- assessment_migrate (ASSESSMENT_DESCRIPTION, EXECUTION_DATE, TEST_DESCRIPTION, SCORE_DESCRIPTION, RESULT_DETAILS)

HEDEF:
- assessment_runs
- assessment_run_checkpoints
- assessment_run_metrics
- assessment_run_category_metrics
- assessments.last_run_id güncelle (EN SONDA tarihe göre düzeltme yapılır)

KURALLAR (kullanıcı onaylı):
- TEST_DESCRIPTION == checkpoints.Name (birebir)
- SCORE_DESCRIPTION: Pass->pass, Fail->fail (başka çeşit yok)
- RESULT_DETAILS: sadece fail'de evidence_text'e yazılacak
- executed_at = assessment_migrate.EXECUTION_DATE
- Aynı (assessment, date) varsa SKIP

EK KONTROLLER:
1) assessment_migrate.ASSESSMENT_DESCRIPTION -> assessments.name var mı?
2) assessment_migrate.TEST_DESCRIPTION -> checkpoints.Name var mı?
3) assessment'ın benchmark'ında (benchmark_checkpoints) bu checkpoint var mı?

CLI:
- -u / --verbose : tüm detay logları göster
- (default)      : sadece ERROR logları göster
"""

from __future__ import annotations

import argparse
import sys
import traceback
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Set, Tuple

from db import get_db

# -----------------------------
# CONSTANTS
# -----------------------------
SEVERITY_WEIGHT = {"caution": 1, "minor": 2, "major": 3, "critical": 4}
ASSET_IMPACT_WEIGHT = {"very_low": 1, "low": 2, "medium": 3, "high": 4, "critical": 5}

MAX_ASSET_IMPACT = 5
MAX_EVIDENCE_CHARS = 200_000

# -----------------------------
# LOGGING
# -----------------------------
VERBOSE = False


def log(msg: str) -> None:
    """INFO log (sadece -u/--verbose ile basılır)."""
    if VERBOSE:
        print(f"[assessment_migrate] {msg}", flush=True)


def log_error(msg: str) -> None:
    """ERROR log (her zaman basılır)."""
    print(f"[assessment_migrate][ERROR] {msg}", file=sys.stderr, flush=True)


# -----------------------------
# HELPERS
# -----------------------------
def run_month(dt: datetime) -> int:
    return int(dt.strftime("%Y%m"))


def risk_level(score: float) -> str:
    if score <= 10:
        return "low"
    if score <= 30:
        return "medium"
    if score <= 60:
        return "high"
    return "critical"


def safe_limit_text(text: Optional[str], limit: int) -> Optional[str]:
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[:limit] + "\n... [TRUNCATED] ..."


@dataclass
class CheckpointSnapshot:
    checkpoint_id: int
    name: str
    severity: str
    category: str
    description: Optional[str]
    pre_sql_test: Optional[str]
    sql_test: Optional[str]
    test_condition: Optional[str]
    pre_sql_detail: Optional[str]
    sql_detail: Optional[str]
    text_pass: Optional[str]
    text_fail: Optional[str]


def calculate_metrics(rows: List[Dict[str, Any]], asset_impact_value: int) -> Dict[str, Any]:
    total = len(rows)
    success = sum(1 for r in rows if r["test_result"] == "pass")
    fail = sum(1 for r in rows if r["test_result"] == "fail")
    error = sum(1 for r in rows if r["test_result"] == "error")

    success_pct = (success * 100.0 / total) if total else 0.0
    fail_pct = (fail * 100.0 / total) if total else 0.0
    error_pct = (error * 100.0 / total) if total else 0.0

    severity_sum = 0
    failed_severity_sum = 0

    for r in rows:
        sev = (r.get("checkpoint_severity") or "").lower()
        w = SEVERITY_WEIGHT.get(sev, 3)
        severity_sum += w

        score = 0 if r["test_result"] == "pass" else 1  # pass=0, fail/error=1
        failed_severity_sum += w * score

    risk = (failed_severity_sum * 100.0 / severity_sum) if severity_sum else 0.0
    rlevel = risk_level(risk)

    asset_adj_risk = risk * (asset_impact_value / MAX_ASSET_IMPACT)
    asset_adj_level = risk_level(asset_adj_risk)

    return {
        "total": int(total),
        "success": int(success),
        "fail": int(fail),
        "error": int(error),
        "success_pct": round(success_pct, 2),
        "fail_pct": round(fail_pct, 2),
        "error_pct": round(error_pct, 2),
        "risk": round(risk, 2),
        "severity_sum": int(severity_sum),
        "failed_severity_sum": int(failed_severity_sum),
        "risk_level": rlevel,
        "asset_adjusted_risk": round(asset_adj_risk, 2),
        "asset_adjusted_risk_level": asset_adj_level,
    }


# -----------------------------
# LOADERS
# -----------------------------
def load_assessment_by_name(cur, assessment_name: str) -> Dict[str, Any]:
    # (1) kontrol burada: yoksa RuntimeError
    cur.execute("SELECT * FROM assessments WHERE name=%s", (assessment_name,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Assessment not found in assessments: {assessment_name}")
    return row


def load_checkpoint_by_name(cur, checkpoint_name: str) -> CheckpointSnapshot:
    # (2) kontrol burada: yoksa RuntimeError
    cur.execute(
        """
        SELECT
            Id AS checkpoint_id,
            Name AS checkpoint_name,
            Severity,
            Category,
            Description,
            Pre_SQL_Test,
            SQL_Test,
            Test_Condition,
            Pre_SQL_Detail,
            SQL_Detail,
            Text_Pass,
            Text_Fail
        FROM checkpoints
        WHERE Name=%s
        """,
        (checkpoint_name,),
    )
    r = cur.fetchone()
    if not r:
        raise RuntimeError(f"Checkpoint not found in checkpoints by Name: {checkpoint_name}")

    sev = (r["Severity"] or "").lower()
    if sev == "info":
        sev = "caution"
    if sev not in SEVERITY_WEIGHT:
        sev = "major"

    cat = (r["Category"] or "OTHER").upper()
    if cat not in ["AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"]:
        cat = "OTHER"

    return CheckpointSnapshot(
        checkpoint_id=int(r["checkpoint_id"]),
        name=r["checkpoint_name"],
        severity=sev,
        category=cat,
        description=r["Description"],
        pre_sql_test=r["Pre_SQL_Test"],
        sql_test=r["SQL_Test"],
        test_condition=r["Test_Condition"],
        pre_sql_detail=r["Pre_SQL_Detail"],
        sql_detail=r["SQL_Detail"],
        text_pass=r["Text_Pass"],
        text_fail=r["Text_Fail"],
    )


def already_migrated(cur, assessment_id: int, exec_date: date) -> bool:
    cur.execute(
        """
        SELECT 1
          FROM assessment_runs
         WHERE assessment_id=%s
           AND DATE(executed_at)=%s
         LIMIT 1
        """,
        (assessment_id, exec_date),
    )
    return cur.fetchone() is not None


def load_benchmark_checkpoint_ids(cur, benchmark_id: int) -> Set[int]:
    """
    (3) Benchmark -> checkpoint membership kontrolü için:
    benchmark_checkpoints tablosundan benchmark_id'nin checkpoint listesi.
    """
    cur.execute(
        """
        SELECT checkpoint_id
          FROM benchmark_checkpoints
         WHERE benchmark_id=%s
        """,
        (benchmark_id,),
    )
    rows = cur.fetchall()
    return {int(r["checkpoint_id"]) for r in rows}


def recompute_last_run_ids(cur) -> None:
    """
    last_run_id'yi tarihe göre garanti altına al:
    - her assessment için max(executed_at) gününü bul
    - o günde birden fazla run varsa max(run_id) seç
    """
    log("-> Son adım: assessments.last_run_id tarihe göre yeniden hesaplanıyor...")
    cur.execute(
        """
        UPDATE assessments a
        LEFT JOIN (
          SELECT r.assessment_id,
                 MAX(r.run_id) AS last_run_id
          FROM assessment_runs r
          JOIN (
            SELECT assessment_id, MAX(executed_at) AS max_exec
            FROM assessment_runs
            GROUP BY assessment_id
          ) mx ON mx.assessment_id = r.assessment_id AND r.executed_at = mx.max_exec
          GROUP BY r.assessment_id
        ) x ON x.assessment_id = a.assessment_id
        SET a.last_run_id = x.last_run_id
        """
    )
    log("-> last_run_id re-compute tamam.")


# -----------------------------
# MAIN
# -----------------------------
def migrate() -> Dict[str, int]:
    db = get_db()
    cur = db.cursor()

    log("DB bağlantısı açıldı.")
    cur.execute("SELECT DATABASE() AS dbname")
    log(f"Bağlı DB: {cur.fetchone().get('dbname')}")

    assessment_cache: Dict[str, Dict[str, Any]] = {}
    checkpoint_cache: Dict[str, CheckpointSnapshot] = {}
    benchmark_cp_cache: Dict[int, Set[int]] = {}  # benchmark_id -> {checkpoint_id}

    log("assessment_migrate okunuyor (ORDER BY assessment_description, execution_date)...")
    cur.execute(
        """
        SELECT
            ASSESSMENT_DESCRIPTION,
            EXECUTION_DATE,
            TEST_DESCRIPTION,
            SCORE_DESCRIPTION,
            RESULT_DETAILS
        FROM assessment_migrate
        ORDER BY ASSESSMENT_DESCRIPTION, EXECUTION_DATE
        """
    )
    src_rows = cur.fetchall()
    log(f"Kaynak satır sayısı: {len(src_rows)}")

    stats = {
        "groups_total": 0,
        "groups_inserted": 0,
        "groups_skipped": 0,
        "checkpoints_inserted": 0,
        "unknown_score_mapped_to_error": 0,
    }

    def get_assessment(a_name: str) -> Dict[str, Any]:
        if a_name not in assessment_cache:
            log(f"Assessment cache miss: {a_name} -> assessments tablosundan okunuyor")
            assessment_cache[a_name] = load_assessment_by_name(cur, a_name)
        return assessment_cache[a_name]

    def get_checkpoint(cp_name: str) -> CheckpointSnapshot:
        if cp_name not in checkpoint_cache:
            checkpoint_cache[cp_name] = load_checkpoint_by_name(cur, cp_name)
        return checkpoint_cache[cp_name]

    def get_benchmark_cp_ids(benchmark_id: int) -> Set[int]:
        if benchmark_id not in benchmark_cp_cache:
            log(f"Benchmark checkpoint cache miss: benchmark_id={benchmark_id} -> benchmark_checkpoints okunuyor")
            benchmark_cp_cache[benchmark_id] = load_benchmark_checkpoint_ids(cur, benchmark_id)
        return benchmark_cp_cache[benchmark_id]

    current_key: Optional[Tuple[str, date]] = None
    bucket: List[Dict[str, Any]] = []

    def flush_bucket() -> None:
        nonlocal bucket, current_key

        if not bucket or not current_key:
            bucket = []
            current_key = None
            return

        stats["groups_total"] += 1

        assessment_name, exec_date = current_key
        assessment = get_assessment(assessment_name)  # (1) burada garanti
        assessment_id = int(assessment["assessment_id"])
        benchmark_id = int(assessment["benchmark_id"])

        log(f"Grup işleniyor: {assessment_name} | {exec_date} | satır={len(bucket)}")

        if already_migrated(cur, assessment_id, exec_date):
            stats["groups_skipped"] += 1
            log(f"SKIP: Zaten var (assessment_id={assessment_id}, date={exec_date})")
            bucket = []
            current_key = None
            return

        executed_at = datetime.combine(exec_date, time(0, 0, 0))
        rmonth = run_month(executed_at)

        asset_impact_enum = assessment["asset_impact"]
        asset_impact_value = ASSET_IMPACT_WEIGHT[asset_impact_enum]

        log("-> assessment_runs insert (status=incomplete)...")
        cur.execute(
            """
            INSERT INTO assessment_runs
            (run_month, assessment_id, assessment_name, db_type, status,
             datasource_id, datasource_name, benchmark_id, benchmark_name, asset_impact, executed_at)
            VALUES (%s,%s,%s,%s,'incomplete',%s,%s,%s,%s,%s,%s)
            """,
            (
                rmonth,
                assessment_id,
                assessment["name"],
                assessment["db_type"],
                assessment["datasource_id"],
                assessment["datasource_name"],
                assessment["benchmark_id"],
                assessment["benchmark_name"],
                asset_impact_enum,
                executed_at,
            ),
        )
        run_id = cur.lastrowid
        log(f"-> run_id: {run_id}")

        out_rows_for_metrics: List[Dict[str, Any]] = []
        log("-> assessment_run_checkpoints insert (snapshot + pass/fail)...")

        # (3) için: bu benchmark'ın allowed checkpoint id set'i
        allowed_cp_ids = get_benchmark_cp_ids(benchmark_id)

        for r in bucket:
            cp_name = r["TEST_DESCRIPTION"]
            cp = get_checkpoint(cp_name)  # (2) burada garanti

            # (3) benchmark membership check
            if cp.checkpoint_id not in allowed_cp_ids:
                raise RuntimeError(
                    "Checkpoint benchmark'a bağlı değil: "
                    f"assessment={assessment_name}, benchmark_id={benchmark_id}, "
                    f"checkpoint_name={cp.name}, checkpoint_id={cp.checkpoint_id}"
                )

            score_raw = (r.get("SCORE_DESCRIPTION") or "").strip().lower()
            if score_raw == "pass":
                test_result = "pass"
            elif score_raw == "fail":
                test_result = "fail"
            else:
                test_result = "error"
                stats["unknown_score_mapped_to_error"] += 1

            evidence_text = None
            error_text = None
            if test_result == "fail":
                evidence_text = safe_limit_text(r.get("RESULT_DETAILS"), MAX_EVIDENCE_CHARS)
            elif test_result == "error":
                error_text = safe_limit_text(r.get("RESULT_DETAILS"), MAX_EVIDENCE_CHARS)

            cur.execute(
                """
                INSERT INTO assessment_run_checkpoints
                (run_month, run_id, checkpoint_id, checkpoint_name,
                 checkpoint_severity, checkpoint_category,
                 checkpoint_description,
                 checkpoint_pre_sql_test, checkpoint_sql_test, checkpoint_test_condition,
                 checkpoint_pre_sql_detail, checkpoint_sql_detail,
                 checkpoint_text_pass, checkpoint_text_fail,
                 test_result, evidence_text, error_text, executed_at)
                VALUES (%s,%s,%s,%s,
                        %s,%s,
                        %s,
                        %s,%s,%s,
                        %s,%s,
                        %s,%s,
                        %s,%s,%s,%s)
                """,
                (
                    rmonth,
                    run_id,
                    cp.checkpoint_id,
                    cp.name,
                    cp.severity,
                    cp.category,
                    cp.description,
                    cp.pre_sql_test,
                    cp.sql_test,
                    cp.test_condition,
                    cp.pre_sql_detail,
                    cp.sql_detail,
                    cp.text_pass,
                    cp.text_fail,
                    test_result,
                    evidence_text,
                    error_text,
                    executed_at,
                ),
            )
            stats["checkpoints_inserted"] += 1

            out_rows_for_metrics.append(
                {"checkpoint_severity": cp.severity, "checkpoint_category": cp.category, "test_result": test_result}
            )

        log(f"-> checkpoint insert tamam: {len(bucket)}")

        log("-> assessment_run_metrics insert...")

        def insert_metric(dim_type: str, dim_value: str, subset: List[Dict[str, Any]]) -> Dict[str, Any]:
            m = calculate_metrics(subset, asset_impact_value)
            cur.execute(
                """
                INSERT INTO assessment_run_metrics
                (run_month, run_id, dimension_type, dimension_value,
                 total_count, success_count, fail_count, error_count,
                 success_pct, fail_pct, error_pct,
                 risk, severity_sum, failed_severity_sum, risk_level,
                 asset_adjusted_risk, asset_adjusted_risk_level, executed_at)
                VALUES (%s,%s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s)
                """,
                (
                    rmonth,
                    run_id,
                    dim_type,
                    dim_value,
                    m["total"],
                    m["success"],
                    m["fail"],
                    m["error"],
                    m["success_pct"],
                    m["fail_pct"],
                    m["error_pct"],
                    m["risk"],
                    m["severity_sum"],
                    m["failed_severity_sum"],
                    m["risk_level"],
                    m["asset_adjusted_risk"],
                    m["asset_adjusted_risk_level"],
                    executed_at,
                ),
            )
            return m

        overall = insert_metric("all", "all", out_rows_for_metrics)

        for sev in SEVERITY_WEIGHT.keys():
            insert_metric(
                "severity",
                sev,
                [x for x in out_rows_for_metrics if (x.get("checkpoint_severity") or "").lower() == sev],
            )

        for cat in ["AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"]:
            insert_metric(
                "category",
                cat,
                [x for x in out_rows_for_metrics if (x.get("checkpoint_category") or "").upper() == cat],
            )

        log("-> assessment_runs update (final snapshot)...")
        cur.execute(
            """
            UPDATE assessment_runs
               SET total_count=%s,
                   success_count=%s,
                   fail_count=%s,
                   error_count=%s,
                   success_pct=%s,
                   risk=%s,
                   risk_level=%s,
                   asset_adjusted_risk=%s,
                   asset_adjusted_risk_level=%s,
                   status='success',
                   executed_at=%s
             WHERE run_id=%s AND run_month=%s
            """,
            (
                overall["total"],
                overall["success"],
                overall["fail"],
                overall["error"],
                overall["success_pct"],
                overall["risk"],
                overall["risk_level"],
                overall["asset_adjusted_risk"],
                overall["asset_adjusted_risk_level"],
                executed_at,
                run_id,
                rmonth,
            ),
        )

        log("-> assessment_run_category_metrics insert (matrix)...")
        categories = ["AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"]
        severities = ["caution", "minor", "major", "critical"]
        matrix: Dict[Tuple[str, str], Dict[str, int]] = {(c, s): {"t": 0, "p": 0, "f": 0, "e": 0} for c in categories for s in severities}

        for x in out_rows_for_metrics:
            cat = (x.get("checkpoint_category") or "OTHER").upper()
            sev = (x.get("checkpoint_severity") or "major").lower()
            res = x.get("test_result")

            if cat not in categories:
                cat = "OTHER"
            if sev not in severities:
                sev = "major"

            cell = matrix[(cat, sev)]
            cell["t"] += 1
            if res == "pass":
                cell["p"] += 1
            elif res == "fail":
                cell["f"] += 1
            else:
                cell["e"] += 1

        for cat in categories:
            for sev in severities:
                cell = matrix[(cat, sev)]
                cur.execute(
                    """
                    INSERT INTO assessment_run_category_metrics
                    (run_month, run_id, category, severity,
                     total_count, pass_count, fail_count, error_count, executed_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (rmonth, run_id, cat, sev, cell["t"], cell["p"], cell["f"], cell["e"], executed_at),
                )

        # last_run_id burada da set ediliyor ama en sonda yeniden hesaplanacak.
        cur.execute("UPDATE assessments SET last_run_id=%s WHERE assessment_id=%s", (run_id, assessment_id))

        stats["groups_inserted"] += 1
        log(f"Grup tamam ✅ run_id={run_id} | risk={overall['risk']}")

        bucket = []
        current_key = None

    log("Gruplama başlıyor (assessment_description + execution_date)...")
    for r in src_rows:
        a_name = r["ASSESSMENT_DESCRIPTION"]
        exec_dt = r["EXECUTION_DATE"]
        exec_date = exec_dt.date() if isinstance(exec_dt, datetime) else exec_dt
        key = (a_name, exec_date)

        if current_key is None:
            current_key = key

        if key != current_key:
            flush_bucket()
            current_key = key

        bucket.append(r)

    flush_bucket()

    recompute_last_run_ids(cur)

    log("Migration tamamlandı.")
    db.close()
    return stats


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DBVulScan assessment migration runner")
    p.add_argument(
        "-u",
        "--verbose",
        action="store_true",
        help="Verbose output (tüm detay logları gösterir). Default: sadece hatalar.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    VERBOSE = bool(args.verbose)

    try:
        s = migrate()
        # Özet: verbose olmasa bile kısa özet kalsın (istersen bunu da sadece -u yaparız)
        print(
            "[assessment_migrate] ÖZET => "
            f"groups_total={s['groups_total']}, "
            f"inserted={s['groups_inserted']}, "
            f"skipped={s['groups_skipped']}, "
            f"checkpoints_inserted={s['checkpoints_inserted']}, "
            f"unknown_score_to_error={s['unknown_score_mapped_to_error']}",
            flush=True,
        )
    except Exception as e:
        log_error(str(e))
        traceback.print_exc()
        sys.exit(1)