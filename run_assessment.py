# -*- coding: utf-8 -*-
"""
DBVulScan - run_assessment.py (FINAL + ASSET IMPACT)

YAZILAN TABLOLAR:
- assessment_runs
- assessment_run_checkpoints
- assessment_run_metrics   <-- TAM ve DOĞRU (run_id VAR)

KRİTİK KURALLAR:
- assessment_run_metrics:
    * all/all
    * severity/*
    * category/*
    * HER SATIR için:
        risk
        risk_level
        asset_adjusted_risk
        asset_adjusted_risk_level
    * Ayrıca tablo zorunlulukları için:
        severity_sum, failed_severity_sum
        success_pct, fail_pct, error_pct
- assessment_runs:
    * SADECE all/all snapshot
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from db import get_db
from checkpoints.routes import (
    get_oracle_connection,
    get_mssql_connection,
    evaluate_condition,
)

# -----------------------------
# CONSTANTS
# -----------------------------
SEVERITY_WEIGHT = {
    "caution": 1,
    "minor": 2,
    "major": 3,
    "critical": 4,
}

ASSET_IMPACT_WEIGHT = {
    "very_low": 1,
    "low": 2,
    "medium": 3,
    "high": 4,
    "critical": 5,
}

MAX_ASSET_IMPACT = 5


# -----------------------------
# HELPERS
# -----------------------------
def _now() -> datetime:
    return datetime.now()


def _run_month(dt: datetime) -> int:
    return int(dt.strftime("%Y%m"))


def _risk_level(score: float) -> str:
    if score <= 10:
        return "low"
    if score <= 30:
        return "medium"
    if score <= 60:
        return "high"
    return "critical"


def _fetch_first_cell(rows):
    if not rows or not rows[0]:
        return None
    return rows[0][0]


def _execute_sql(conn, sql):
    sql = (sql or "").strip()
    if not sql:
        return [], []

    cur = conn.cursor()
    cur.execute(sql)
    try:
        rows = cur.fetchall()
    except Exception:
        rows = []
    cols = [d[0] for d in cur.description] if cur.description else []
    return cols, rows


# -----------------------------
# SNAPSHOT MODEL
# -----------------------------
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


# -----------------------------
# LOADERS
# -----------------------------
def _load_assessment(cur, assessment_id: int) -> Dict[str, Any]:
    cur.execute("SELECT * FROM assessments WHERE assessment_id=%s", (assessment_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Assessment not found")
    return row


def _load_datasource(cur, ds_id: int) -> Dict[str, Any]:
    cur.execute("SELECT * FROM datasources WHERE ds_id=%s", (ds_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Datasource not found")
    return row


def _load_checkpoint_ids(cur, benchmark_id: int) -> List[int]:
    cur.execute(
        """
        SELECT checkpoint_id
          FROM benchmark_checkpoints
         WHERE benchmark_id=%s
         ORDER BY sort_order, checkpoint_id
        """,
        (benchmark_id,),
    )
    return [r["checkpoint_id"] for r in cur.fetchall()]


def _load_checkpoint(cur, checkpoint_id: int) -> CheckpointSnapshot:
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
        WHERE Id=%s
        """,
        (checkpoint_id,),
    )
    r = cur.fetchone()
    if not r:
        raise RuntimeError("Checkpoint not found")

    sev = (r["Severity"] or "").lower()
    if sev == "info":
        sev = "caution"
    if sev not in SEVERITY_WEIGHT:
        # fail-safe
        sev = "major"

    return CheckpointSnapshot(
        checkpoint_id=r["checkpoint_id"],
        name=r["checkpoint_name"],
        severity=sev,
        category=(r["Category"] or "OTHER").upper(),
        description=r["Description"],
        pre_sql_test=r["Pre_SQL_Test"],
        sql_test=r["SQL_Test"],
        test_condition=r["Test_Condition"],
        pre_sql_detail=r["Pre_SQL_Detail"],
        sql_detail=r["SQL_Detail"],
        text_pass=r["Text_Pass"],
        text_fail=r["Text_Fail"],
    )


# -----------------------------
# METRIC CALCULATION (FINAL)
# -----------------------------
def _calculate_metrics(rows, asset_impact_value: int) -> Dict[str, Any]:
    total = len(rows)
    success = sum(1 for r in rows if r["test_result"] == "pass")
    fail = sum(1 for r in rows if r["test_result"] == "fail")
    error = sum(1 for r in rows if r["test_result"] == "error")

    success_pct = (success * 100.0 / total) if total else 0.0
    fail_pct = (fail * 100.0 / total) if total else 0.0
    error_pct = (error * 100.0 / total) if total else 0.0

    # FINAL risk model:
    # pass=0, fail=1, error=1
    severity_sum = 0
    failed_severity_sum = 0

    for r in rows:
        sev = (r.get("checkpoint_severity") or "").lower()
        w = SEVERITY_WEIGHT.get(sev, 3)
        severity_sum += w

        score = 0 if r["test_result"] == "pass" else 1
        failed_severity_sum += w * score

    risk = (failed_severity_sum * 100.0 / severity_sum) if severity_sum else 0.0
    risk_level = _risk_level(risk)

    asset_adj_risk = risk * (asset_impact_value / MAX_ASSET_IMPACT)
    asset_adj_level = _risk_level(asset_adj_risk)

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
        "risk_level": risk_level,
        "asset_adjusted_risk": round(asset_adj_risk, 2),
        "asset_adjusted_risk_level": asset_adj_level,
    }


# =========================================================
# PUBLIC API
# =========================================================
def run_assessment(assessment_id: int) -> Tuple[int, int]:
    db = get_db()
    cur = db.cursor()

    run_month = _run_month(_now())

    assessment = _load_assessment(cur, assessment_id)
    asset_impact_enum = assessment["asset_impact"]
    asset_impact_value = ASSET_IMPACT_WEIGHT[asset_impact_enum]

    # -------------------------
    # RUN MASTER
    # -------------------------
    cur.execute(
        """
        INSERT INTO assessment_runs
        (run_month, assessment_id, assessment_name, db_type, status,
         datasource_id, datasource_name, benchmark_id, benchmark_name, asset_impact)
        VALUES (%s,%s,%s,%s,'started',%s,%s,%s,%s,%s)
        """,
        (
            run_month,
            assessment_id,
            assessment["name"],
            assessment["db_type"],
            assessment["datasource_id"],
            assessment["datasource_name"],
            assessment["benchmark_id"],
            assessment["benchmark_name"],
            asset_impact_enum,
        ),
    )
    run_id = cur.lastrowid

    had_error = False

    ds = _load_datasource(cur, assessment["datasource_id"])
    conn = (
        get_oracle_connection(ds)
        if assessment["db_type"] == "oracle"
        else get_mssql_connection(ds)
    )

    # -------------------------
    # CHECKPOINT EXECUTION
    # -------------------------
    for cp_id in _load_checkpoint_ids(cur, assessment["benchmark_id"]):
        cp = _load_checkpoint(cur, cp_id)

        try:
            if cp.pre_sql_test:
                _execute_sql(conn, cp.pre_sql_test)

            _, rows = _execute_sql(conn, cp.sql_test)
            value = _fetch_first_cell(rows)
            (ok, _), err = evaluate_condition(value, cp.test_condition)

            if err:
                raise RuntimeError(err)

            result = "pass" if ok else "fail"
            error_text = None

        except Exception as e:
            result = "error"
            error_text = str(e)
            had_error = True

        cur.execute(
            """
            INSERT INTO assessment_run_checkpoints
            (run_month, run_id, checkpoint_id, checkpoint_name,
             checkpoint_severity, checkpoint_category,
             checkpoint_description,
             checkpoint_pre_sql_test, checkpoint_sql_test, checkpoint_test_condition,
             checkpoint_pre_sql_detail, checkpoint_sql_detail,
             checkpoint_text_pass, checkpoint_text_fail,
             test_result, error_text)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                run_month,
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
                result,
                error_text,
            ),
        )

    conn.close()

    # -------------------------
    # METRICS SOURCE
    # -------------------------
    cur.execute(
        """
        SELECT checkpoint_severity, checkpoint_category, test_result
        FROM assessment_run_checkpoints
        WHERE run_id=%s AND run_month=%s
        """,
        (run_id, run_month),
    )
    rows = cur.fetchall()

    def insert_metric(dim_type, dim_value, subset):
        m = _calculate_metrics(subset, asset_impact_value)
        cur.execute(
            """
            INSERT INTO assessment_run_metrics
            (run_month, run_id, dimension_type, dimension_value,
             total_count, success_count, fail_count, error_count,
             success_pct, fail_pct, error_pct,
             risk, severity_sum, failed_severity_sum, risk_level,
             asset_adjusted_risk, asset_adjusted_risk_level)
            VALUES (%s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s)
            """,
            (
                run_month,
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
            ),
        )
        return m

    # ALL / ALL
    overall = insert_metric("all", "all", rows)

    # SEVERITY
    for sev in SEVERITY_WEIGHT.keys():
        insert_metric(
            "severity",
            sev,
            [r for r in rows if (r["checkpoint_severity"] or "").lower() == sev],
        )

    # CATEGORY
    for cat in ["AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"]:
        insert_metric(
            "category",
            cat,
            [r for r in rows if (r["checkpoint_category"] or "").upper() == cat],
        )

    # -------------------------
    # SNAPSHOT TO assessment_runs (all/all)
    # -------------------------
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
            status=%s
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
            "error" if had_error else "success",
            run_id,
            run_month,
        ),
    )

    db.commit()
    db.close()

    return run_id, run_month
