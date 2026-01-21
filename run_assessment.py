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

STATUS AKIŞI (ANLAŞTIĞIMIZ FINAL):
- Run başlar başlamaz: incomplete
- DB'ye bağlanamazsa: unreachable ve return (exception yok)
- SQL testleri sırasında hata olursa: error (run devam eder, final error)
- Sorunsuz biterse: success
- Kill/crash olursa: incomplete kalır (handle edilemez)

EK KRİTİK KURAL (GERİ GELDİ):
- assessment_run_checkpoints insert edilirken:
    * test_result='fail' ise:
        - önce checkpoint_pre_sql_detail (varsa) çalıştır
        - ardından checkpoint_sql_detail çalıştır
        - checkpoint_sql_detail çıktısını evidence_text içine yaz
    * test_result='error' ise:
        - error_text doldur (zaten vardı)
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

# evidence_text / error_text çok büyümesin diye soft limit
MAX_EVIDENCE_CHARS = 200_000
MAX_ERROR_CHARS = 50_000


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


def _safe_limit_text(text: Optional[str], limit: int) -> Optional[str]:
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[:limit] + "\n... [TRUNCATED] ..."


def _format_sql_result_for_evidence(cols: List[str], rows: List[tuple], max_rows: int = 200) -> str:
    """
    evidence_text için SQLCMD / SQLPLUS tarzı tablo çıktısı üretir.

    KRİTİK FIX:
    - Bazı durumlarda hücre değeri string olarak "('text',)" formatında geliyor.
      Bu durumda parantez/virgül aslında string'in kendisi.
      Çözüm: Bu tuple-repr string'i sanitize edip iç metni çıkar.
    """

    from collections.abc import Sequence

    def _is_row_like(x: Any) -> bool:
        return isinstance(x, Sequence) and not isinstance(x, (str, bytes, bytearray))

    def _unwrap_singleton(v: Any) -> Any:
        while _is_row_like(v) and len(v) == 1:
            v = v[0]
        return v

    # ==============================
    # SADECE BURASI VAR (KALDI) + KULLANILACAK
    # ==============================
    def _sanitize_tuple_repr_string(s: str) -> str:
        """
        Minimum garanti kural:
        - string başı '(' ise kaldır
        - string sonu ',)' ise kaldır
        İçerideki tırnaklar/boşluklar korunur.
        NOT: sonda padding olabileceği için wrapper kontrolünü strip() ile yapıyoruz.
        """
        if not s:
            return s

        ss = s.strip()

        # ( ile başlıyorsa at
        if ss.startswith("("):
            ss = ss[1:]

        # ,) ile bitiyorsa at
        if ss.endswith(",)"):
            ss = ss[:-2]

        return ss

    def _cell_to_str(v: Any) -> str:
        # unwrap singleton row/cell
        v = _unwrap_singleton(v)
        if v is None:
            return ""
        # >>> KRİTİK DÜZELTME:
        # v string olmasa bile str(v) -> "('sa',)" geliyor.
        # Bu yüzden HER ZAMAN str() yapıp sanitize ediyoruz.
        s = str(v)
        return _sanitize_tuple_repr_string(s)

    if not cols:
        return ""

    take_rows = (rows or [])[:max_rows]

    # ==========================
    # TEK KOLON
    # ==========================
    if len(cols) == 1:
        col_name = str(cols[0])

        values: List[str] = []
        for r in take_rows:
            if r is None:
                values.append("")
                continue

            if _is_row_like(r):
                values.append(_cell_to_str(r[0]) if len(r) else "")
            else:
                values.append(_cell_to_str(r))

        width = max(len(col_name), max((len(v) for v in values), default=0))

        lines = [col_name, "-" * width]
        lines.extend(values)

        if rows and len(rows) > max_rows:
            lines.append(f"... ({len(rows) - max_rows} more rows truncated) ...")

        return "\n".join(lines)

    # ==========================
    # ÇOKLU KOLON
    # ==========================
    str_rows: List[List[str]] = []
    for r in take_rows:
        if r is None:
            str_rows.append([""] * len(cols))
            continue

        if not _is_row_like(r):
            r = (r,)

        row_cells = [_cell_to_str(v) for v in r]
        str_rows.append(row_cells)

    # eksik kolon varsa doldur, fazla varsa kes
    norm_rows: List[List[str]] = []
    for r in str_rows:
        if len(r) < len(cols):
            r = r + ([""] * (len(cols) - len(r)))
        elif len(r) > len(cols):
            r = r[: len(cols)]
        norm_rows.append(r)

    widths: List[int] = []
    for i, c in enumerate(cols):
        max_val_len = 0
        for rr in norm_rows:
            max_val_len = max(max_val_len, len(rr[i]))
        widths.append(max(len(str(c)), max_val_len))

    header_parts = [str(cols[i]).ljust(widths[i]) for i in range(len(cols))]
    header = "  ".join(header_parts).rstrip()
    sep = "-" * max(1, len(header))

    lines: List[str] = [header, sep]
    for rr in norm_rows:
        parts = [rr[i].ljust(widths[i]) for i in range(len(cols))]
        lines.append("  ".join(parts).rstrip())

    if rows and len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} more rows truncated) ...")

    return "\n".join(lines)



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
    # RUN MASTER (başlar başlamaz incomplete)
    # -------------------------
    cur.execute(
        """
        INSERT INTO assessment_runs
        (run_month, assessment_id, assessment_name, db_type, status,
         datasource_id, datasource_name, benchmark_id, benchmark_name, asset_impact)
        VALUES (%s,%s,%s,%s,'incomplete',%s,%s,%s,%s,%s)
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
    db.commit()  # run kaydı mühürlensin (kill olursa incomplete kalması tasarım)

    had_error = False

    # -------------------------
    # TARGET DB CONNECT (bağlanamazsa unreachable ve return)
    # -------------------------
    ds = _load_datasource(cur, assessment["datasource_id"])
    try:
        conn = (
            get_oracle_connection(ds)
            if assessment["db_type"] == "oracle"
            else get_mssql_connection(ds)
        )
    except Exception:
        # DB'ye bağlanamadı -> unreachable, exception yok
        cur.execute(
            """
            UPDATE assessment_runs
               SET status='unreachable'
             WHERE run_id=%s AND run_month=%s
            """,
            (run_id, run_month),
        )
        db.commit()
        db.close()
        return run_id, run_month

    # -------------------------
    # CHECKPOINT EXECUTION
    # -------------------------
    try:
        for cp_id in _load_checkpoint_ids(cur, assessment["benchmark_id"]):
            cp = _load_checkpoint(cur, cp_id)

            evidence_text: Optional[str] = None
            error_text: Optional[str] = None

            try:
                # pre_sql_test (varsa) koş
                if cp.pre_sql_test:
                    _execute_sql(conn, cp.pre_sql_test)

                # sql_test koş ve koşul değerlendir
                _, rows = _execute_sql(conn, cp.sql_test)
                value = _fetch_first_cell(rows)
                (ok, _), err = evaluate_condition(value, cp.test_condition)

                if err:
                    raise RuntimeError(err)

                result = "pass" if ok else "fail"

                # -----------------------------
                # KRİTİK: fail ise DETAIL SQL çalıştır + evidence_text doldur
                # -----------------------------
                if result == "fail":
                    # 1) pre_sql_detail (varsa) önce çalıştır
                    if (cp.pre_sql_detail or "").strip():
                        _execute_sql(conn, cp.pre_sql_detail)

                    # 2) sql_detail çalıştır (varsa sonuçtan evidence üret)
                    detail_sql = (cp.sql_detail or "").strip()
                    if detail_sql:
                        d_cols, d_rows = _execute_sql(conn, detail_sql)
                        evidence_text = _format_sql_result_for_evidence(d_cols, d_rows)
                        evidence_text = _safe_limit_text(evidence_text, MAX_EVIDENCE_CHARS)
                    else:
                        evidence_text = ""

            except Exception as e:
                result = "error"
                error_text = _safe_limit_text(str(e), MAX_ERROR_CHARS)
                had_error = True
                evidence_text = None  # error'da evidence yazma

            # INSERT snapshot + result
            cur.execute(
                """
                INSERT INTO assessment_run_checkpoints
                (run_month, run_id, checkpoint_id, checkpoint_name,
                 checkpoint_severity, checkpoint_category,
                 checkpoint_description,
                 checkpoint_pre_sql_test, checkpoint_sql_test, checkpoint_test_condition,
                 checkpoint_pre_sql_detail, checkpoint_sql_detail,
                 checkpoint_text_pass, checkpoint_text_fail,
                 test_result, evidence_text, error_text)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                    evidence_text,
                    error_text,
                ),
            )
    finally:
        try:
            conn.close()
        except Exception:
            pass

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

    overall = insert_metric("all", "all", rows)

    for sev in SEVERITY_WEIGHT.keys():
        insert_metric(
            "severity",
            sev,
            [r for r in rows if (r["checkpoint_severity"] or "").lower() == sev],
        )

    for cat in ["AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"]:
        insert_metric(
            "category",
            cat,
            [r for r in rows if (r["checkpoint_category"] or "").upper() == cat],
        )

    # -------------------------
    # SNAPSHOT TO assessment_runs (all/all) + FINAL STATUS
    # -------------------------
    final_status = "error" if had_error else "success"

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
            final_status,
            run_id,
            run_month,
        ),
    )

    # -------------------------
    # assessment_run_category_metrics (Category × Severity matrix) - 32 satır
    # -------------------------
    categories = ["AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"]
    severities = ["caution", "minor", "major", "critical"]

    matrix: Dict[Tuple[str, str], Dict[str, int]] = {}
    for cat in categories:
        for sev in severities:
            matrix[(cat, sev)] = {"total": 0, "pass": 0, "fail": 0, "error": 0}

    for r in rows:
        cat = (r.get("checkpoint_category") or "OTHER").upper()
        sev = (r.get("checkpoint_severity") or "major").lower()
        res = r.get("test_result")

        if cat not in categories:
            cat = "OTHER"
        if sev not in severities:
            sev = "major"

        cell = matrix[(cat, sev)]
        cell["total"] += 1
        if res == "pass":
            cell["pass"] += 1
        elif res == "fail":
            cell["fail"] += 1
        else:
            cell["error"] += 1

    for cat in categories:
        for sev in severities:
            cell = matrix[(cat, sev)]
            cur.execute(
                """
                INSERT INTO assessment_run_category_metrics
                (run_month, run_id, category, severity,
                 total_count, pass_count, fail_count, error_count)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    run_month,
                    run_id,
                    cat,
                    sev,
                    cell["total"],
                    cell["pass"],
                    cell["fail"],
                    cell["error"],
                ),
            )

    # -------------------------
    # NEW: assessments.last_run_id = run_id (ONLY normal end)
    # -------------------------
    cur.execute(
        """
        UPDATE assessments
           SET last_run_id=%s
         WHERE assessment_id=%s
        """,
        (run_id, assessment_id),
    )

    db.commit()
    db.close()

    return run_id, run_month
