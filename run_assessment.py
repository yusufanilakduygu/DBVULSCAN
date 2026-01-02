# -*- coding: utf-8 -*-
"""DBVulScan - run_assessment.py

Bu dosya, senin FINAL tasarımına göre *assessment çalıştırma* işini yapar.

ÖNEMLİ PRENSİPLER (FINAL DESIGN):
- Versiyon tutulmaz. (benchmark/checkpoint/app version yok)
- Run anındaki tüm bilgiler snapshot olarak run tablolarına kopyalanır.
- 3 tablo büyür:
    * assessment_runs
    * assessment_run_checkpoints
    * assessment_run_metrics
- Partition anahtarı run_month (YYYYMM) ve PK run_month içerir (MySQL gereği).
- Risk modeli:
    severity weight: caution=1, minor=2, major=3, critical=4
    result score  : pass=0, fail=1, error=1   (Error = Fail sayılır)
    RiskScore     : 100 * failed_severity_sum / severity_sum
    RiskLevel map :
        0..10   -> low
        10..30  -> medium
        30..60  -> high
        >60     -> critical

Bu modül *kafadan yeni bağlantı yöntemi uydurmaz*:
- Oracle/MSSQL target DB bağlantısı ve condition eval mantığı,
  checkpoints bölümünde test ettiğin koddan reuse edilir:
    checkpoints/routes.py:
      - get_oracle_connection(ds)
      - get_mssql_connection(ds)
      - evaluate_condition(result_value, condition_text)

KULLANIM:
- assessments ekranındaki "Run Assessment" butonu POST eder,
  assessments/routes.py içindeki run_assessment_action çalışır.
- Bu fonksiyon run_assessment(assessment_id) çağırır ve aynı sayfaya döner.

Not:
- username / triggered_by gibi alanlar DB'de olmadığı için yazılmıyor.

EK (SENİN SON İSTEĞİN):
- assessment_run_metrics içinde hesaplanan all/all (dimension_type='all', dimension_value='all')
  metriğinin özet alanları, assessment_runs tablosuna snapshot olarak yazılır:
    total_count, success_count, fail_count, error_count, success_pct, risk, risk_level
  Böylece assessment run listelerinde JOIN ihtiyacı azalır.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from db import get_db

# Checkpoints modülündeki (halihazırda test edilmiş) bağlantı/eval fonksiyonları
from checkpoints.routes import (  # type: ignore
    get_oracle_connection,
    get_mssql_connection,
    evaluate_condition,
)

# FINAL severity ağırlıkları (Info yok)
SEVERITY_WEIGHT: Dict[str, int] = {
    "caution": 1,
    "minor": 2,
    "major": 3,
    "critical": 4,
}


# -----------------------------
# Helper fonksiyonlar
# -----------------------------
def _now() -> datetime:
    return datetime.now()


def _run_month(dt: datetime) -> int:
    """YYYYMM (partition key)."""
    return int(dt.strftime("%Y%m"))


def _risk_level(risk: float) -> str:
    """FINAL risk level mapping."""
    if risk <= 10:
        return "low"
    if risk <= 30:
        return "medium"
    if risk <= 60:
        return "high"
    return "critical"


def _safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def _fetch_first_cell(rows: Sequence[Sequence[Any]]) -> Any:
    """Test condition karşılaştırması için standart: first row, first column."""
    if not rows or not rows[0]:
        return None
    return rows[0][0]


def _cursor_columns(cur) -> List[str]:
    desc = getattr(cur, "description", None)
    if not desc:
        return []
    return [d[0] for d in desc]


def _execute_sql(conn, sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    """Target DB üzerinde SQL çalıştır ve (columns, rows) döndür.

    - sql boşsa: ([], [])
    - tek statement bekleniyor (checkpoint ekranında olduğu gibi)
    """
    sql = (sql or "").strip()
    if not sql:
        return [], []

    cur = conn.cursor()
    cur.execute(sql)

    # Bazı driver'larda (özellikle DDL/DML) fetchall hata verebilir,
    # bu yüzden try/except ile güvene alıyoruz.
    try:
        rows = cur.fetchall()
    except Exception:
        rows = []

    cols = _cursor_columns(cur)
    return cols, list(rows or [])


def _format_result(columns: List[str], rows: List[Tuple[Any, ...]], max_rows: int = 200) -> str:
    """Fail evidence için okunabilir çıktı üret."""
    if not columns and not rows:
        return "(no output)"

    out: List[str] = []
    if columns:
        out.append("\t".join(columns))
        out.append("\t".join(["-" * max(3, min(20, len(c))) for c in columns]))

    for r in rows[:max_rows]:
        out.append("\t".join(_safe_str(v) for v in r))

    if len(rows) > max_rows:
        out.append(f"... ({len(rows) - max_rows} more rows)")

    return "\n".join(out)


# -----------------------------
# Snapshot model (checkpoints tablosundan çekiyoruz)
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


def _load_assessment(repo_cur, assessment_id: int) -> Dict[str, Any]:
    repo_cur.execute("SELECT * FROM assessments WHERE assessment_id=%s", (assessment_id,))
    a = repo_cur.fetchone()
    if not a:
        raise RuntimeError(f"Assessment not found: {assessment_id}")
    return a


def _load_datasource(repo_cur, datasource_id: int) -> Dict[str, Any]:
    # Datasources modülündeki tablo: datasources (ds_id ...)
    repo_cur.execute("SELECT * FROM datasources WHERE ds_id=%s", (datasource_id,))
    ds = repo_cur.fetchone()
    if not ds:
        raise RuntimeError(f"Datasource not found: {datasource_id}")
    return ds


def _load_benchmark_checkpoint_ids(repo_cur, benchmark_id: int) -> List[int]:
    """Benchmark -> checkpoint listesi (sort_order ile)."""
    repo_cur.execute(
        """
        SELECT checkpoint_id
          FROM benchmark_checkpoints
         WHERE benchmark_id = %s
         ORDER BY sort_order ASC, checkpoint_id ASC
        """,
        (benchmark_id,),
    )
    rows = repo_cur.fetchall() or []
    return [int(r["checkpoint_id"]) for r in rows]


def _load_checkpoint(repo_cur, checkpoint_id: int) -> CheckpointSnapshot:
    """Checkpoint snapshot'ını checkpoints tablosundan al.

    Not: checkpoints tablosunda kolon isimleri CamelCase (Id/Name/SQL_Test...) olduğu için
    SELECT'te alias kullanıyoruz ve run tablolarına bu alias'larla yazıyoruz.
    """
    repo_cur.execute(
        """
        SELECT
            Id AS checkpoint_id,
            Name AS checkpoint_name,
            Severity AS checkpoint_severity,
            Category AS checkpoint_category,
            Description AS checkpoint_description,
            Pre_SQL_Test AS checkpoint_pre_sql_test,
            SQL_Test AS checkpoint_sql_test,
            Test_Condition AS checkpoint_test_condition,
            Pre_SQL_Detail AS checkpoint_pre_sql_detail,
            SQL_Detail AS checkpoint_sql_detail,
            Text_Pass AS checkpoint_text_pass,
            Text_Fail AS checkpoint_text_fail
        FROM checkpoints
        WHERE Id = %s
        """,
        (checkpoint_id,),
    )
    r = repo_cur.fetchone()
    if not r:
        raise RuntimeError(f"Checkpoint not found: {checkpoint_id}")

    # FINAL: info severity yok. Eğer legacy data'da info varsa, crash olmasın diye caution'a mapliyoruz.
    sev = (r.get("checkpoint_severity") or "").lower()
    if sev == "info":
        sev = "caution"
    if sev not in SEVERITY_WEIGHT:
        # bilinmeyen değer gelirse major kabul et (fail-safe)
        sev = "major"

    cat = (r.get("checkpoint_category") or "OTHER").upper()

    return CheckpointSnapshot(
        checkpoint_id=int(r["checkpoint_id"]),
        name=r.get("checkpoint_name") or "",
        severity=sev,
        category=cat,
        description=r.get("checkpoint_description"),
        pre_sql_test=r.get("checkpoint_pre_sql_test"),
        sql_test=r.get("checkpoint_sql_test"),
        test_condition=r.get("checkpoint_test_condition"),
        pre_sql_detail=r.get("checkpoint_pre_sql_detail"),
        sql_detail=r.get("checkpoint_sql_detail"),
        text_pass=r.get("checkpoint_text_pass"),
        text_fail=r.get("checkpoint_text_fail"),
    )


def _open_target_connection(db_type: str, datasource: Dict[str, Any]):
    """Target DB bağlantısını aç.

    db_type assessment'tan gelir: 'oracle' veya 'mssql'
    Bağlantı detayları datasource kaydından gelir.
    """
    db_type = (db_type or "").lower()
    if db_type == "oracle":
        return get_oracle_connection(datasource)
    if db_type == "mssql":
        return get_mssql_connection(datasource)
    raise RuntimeError(f"Unsupported db_type: {db_type}")


def _evaluate_checkpoint(conn, cp: CheckpointSnapshot) -> Tuple[str, Optional[str], Optional[str]]:
    """Tek bir checkpoint'i çalıştır.

    Dönenler:
      test_result  : 'pass' | 'fail' | 'error'
      evidence_text: Fail ise evidence (SQL_detail çıktıları)
      error_text   : Error ise exception mesajı

    FINAL kurallar:
      - Error = Fail sayılır (riskte score=1)
      - Pass ise evidence boş/None olabilir
      - Fail ise evidence_text'e detail query output yazılır (varsa)
    """
    try:
        # 1) Pre test (varsa) - bazı kontrollerde önce hazırlık/validate
        if (cp.pre_sql_test or "").strip():
            _execute_sql(conn, cp.pre_sql_test or "")

        # 2) Asıl test
        cols, rows = _execute_sql(conn, cp.sql_test or "")
        result_value = _fetch_first_cell(rows)

        # 3) Condition evaluation (checkpoints modülündeki evaluate_condition kullanılır)
        if not cp.test_condition:
            return "error", None, "Missing test_condition"

        (cond_eval, expr), cond_err = evaluate_condition(result_value, cp.test_condition)
        if cond_err:
            return "error", None, cond_err

        passed = bool(cond_eval)
        if passed:
            return "pass", None, None

        # 4) FAIL: Evidence (detail query output) - FINAL: fail'de evidence_text dolabilir
        parts: List[str] = []
        if (cp.pre_sql_detail or "").strip():
            dcols, drows = _execute_sql(conn, cp.pre_sql_detail or "")
            parts.append("PRE_SQL_DETAIL OUTPUT")
            parts.append(_format_result(dcols, drows))
        if (cp.sql_detail or "").strip():
            dcols, drows = _execute_sql(conn, cp.sql_detail or "")
            parts.append("SQL_DETAIL OUTPUT")
            parts.append(_format_result(dcols, drows))

        evidence = "\n\n".join(parts).strip() if parts else None
        return "fail", evidence, None

    except Exception as e:
        # 5) ERROR: exception yakalanır, run devam eder (run sonunda status=error)
        return "error", None, str(e)


def _compute_metrics(rows: List[Dict[str, Any]]) -> Tuple[int, int, int, int, float, float, float, float, int, int, str]:
    """Bir satır kümesi için metrikleri hesapla.

    rows: assessment_run_checkpoints'ten gelen dict listesi.
    """
    total = len(rows)
    success = sum(1 for r in rows if r["test_result"] == "pass")
    fail = sum(1 for r in rows if r["test_result"] == "fail")
    error = sum(1 for r in rows if r["test_result"] == "error")

    success_pct = (success * 100.0 / total) if total else 0.0
    fail_pct = (fail * 100.0 / total) if total else 0.0
    error_pct = (error * 100.0 / total) if total else 0.0

    # Risk hesap bileşenleri
    severity_sum = 0
    failed_severity_sum = 0

    for r in rows:
        sev = (r.get("checkpoint_severity") or "").lower()
        w = SEVERITY_WEIGHT.get(sev, 3)  # bilinmeyen -> major
        severity_sum += w

        # FINAL: pass=0, fail=1, error=1
        score = 0 if r["test_result"] == "pass" else 1
        failed_severity_sum += w * score

    risk = (failed_severity_sum * 100.0 / severity_sum) if severity_sum else 0.0
    level = _risk_level(risk)

    return (
        total,
        success,
        fail,
        error,
        round(success_pct, 2),
        round(fail_pct, 2),
        round(error_pct, 2),
        round(risk, 2),
        int(severity_sum),
        int(failed_severity_sum),
        level,
    )


def _insert_metric(
    repo_cur,
    run_id: int,
    run_month: int,
    dim_type: str,
    dim_value: str,
    metric: Tuple[int, int, int, int, float, float, float, float, int, int, str],
) -> None:
    """assessment_run_metrics insert (DB kolonlarına birebir)."""
    (
        total,
        success,
        fail,
        error,
        success_pct,
        fail_pct,
        error_pct,
        risk,
        severity_sum,
        failed_severity_sum,
        risk_level,
    ) = metric

    repo_cur.execute(
        """
        INSERT INTO assessment_run_metrics
        (run_month, run_id, dimension_type, dimension_value,
         total_count, success_count, fail_count, error_count,
         success_pct, fail_pct, error_pct,
         risk, severity_sum, failed_severity_sum, risk_level)
        VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            run_month,
            run_id,
            dim_type,
            dim_value,
            total,
            success,
            fail,
            error,
            success_pct,
            fail_pct,
            error_pct,
            risk,
            severity_sum,
            failed_severity_sum,
            risk_level,
        ),
    )


# =========================================================
# Public API
# =========================================================
def run_assessment(assessment_id: int) -> Tuple[int, int]:
    """Assessment çalıştır ve sonuçları run tablolarına yaz.

    Dönüş:
      (run_id, run_month)

    Akış (FINAL):
      1) assessment_runs insert (status=started)
      2) Benchmark -> checkpoint listesi çekilir
      3) Her checkpoint:
           - Target DB'de SQL test çalıştırılır
           - Sonuç snapshot + outcome: assessment_run_checkpoints insert
      4) Tüm checkpoint'ler bitince:
           - metrics hesaplanır ve assessment_run_metrics doldurulur
           - ayrıca ALL/ALL metriği assessment_runs tablosuna snapshot olarak UPDATE edilir
      5) Run status:
           - hiç exception yoksa success
           - en az 1 checkpoint error olduysa error
         (kod crash olursa started kalması tasarım gereği; ama biz try/except ile status set etmeye çalışıyoruz)
    """
    started_at = _now()
    run_month = _run_month(started_at)

    repo_db = get_db()
    repo_cur = repo_db.cursor()

    run_id: Optional[int] = None
    had_error = False

    # 1) Assessment metadata (datasource/benchmark adı vs assessment tablosunda snapshot için var)
    a = _load_assessment(repo_cur, assessment_id)

    # 2) Run master insert (DB'de olan kolonlara göre)
    repo_cur.execute(
        """
        INSERT INTO assessment_runs
          (run_month, assessment_id, assessment_name, db_type, status,
           datasource_id, datasource_name, benchmark_id, benchmark_name)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            run_month,
            a["assessment_id"],
            a["name"],
            a["db_type"],
            "started",
            a["datasource_id"],
            a["datasource_name"],
            a["benchmark_id"],
            a["benchmark_name"],
        ),
    )
    run_id = int(repo_cur.lastrowid)

    def _mark_run_status(status: str) -> None:
        """Run status update (en iyi gayret)."""
        try:
            repo_cur.execute(
                "UPDATE assessment_runs SET status=%s WHERE run_id=%s AND run_month=%s",
                (status, run_id, run_month),
            )
        except Exception:
            # Status update başarısız olursa bile run kayıtları zaten DB'de kalır.
            pass

    try:
        # 3) Target DB connection (Oracle/MSSQL)
        ds = _load_datasource(repo_cur, int(a["datasource_id"]))
        target_conn = _open_target_connection(a["db_type"], ds)

        try:
            # 4) Benchmark -> checkpoint id listesi
            checkpoint_ids = _load_benchmark_checkpoint_ids(repo_cur, int(a["benchmark_id"]))

            for cp_id in checkpoint_ids:
                cp = _load_checkpoint(repo_cur, cp_id)

                test_result, evidence_text, error_text = _evaluate_checkpoint(target_conn, cp)

                # Error = fail sayılır ama run status açısından "had_error" flag'i tutuyoruz
                if test_result == "error":
                    had_error = True

                # 5) Snapshot + result insert
                repo_cur.execute(
                    """
                    INSERT INTO assessment_run_checkpoints
                    (run_month, run_id,
                     checkpoint_id, checkpoint_name,
                     checkpoint_severity, checkpoint_category,
                     checkpoint_description,
                     checkpoint_pre_sql_test, checkpoint_sql_test, checkpoint_test_condition,
                     checkpoint_pre_sql_detail, checkpoint_sql_detail,
                     checkpoint_text_pass, checkpoint_text_fail,
                     test_result, evidence_text, error_text)
                    VALUES
                    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                        test_result,
                        evidence_text,
                        error_text,
                    ),
                )

        finally:
            # Target DB bağlantısını her durumda kapat
            try:
                target_conn.close()
            except Exception:
                pass

        # 6) Metrics hesapla: run_checkpoints tablosundan oku (tek truth)
        repo_cur.execute(
            """
            SELECT checkpoint_severity, checkpoint_category, test_result
              FROM assessment_run_checkpoints
             WHERE run_id=%s AND run_month=%s
            """,
            (run_id, run_month),
        )
        run_rows = repo_cur.fetchall() or []

        # 6a) Overall (all/all) -> HEM metrics tablosuna yaz, HEM assessment_runs'a snapshot update edeceğiz
        overall_metric = _compute_metrics(run_rows)
        _insert_metric(repo_cur, run_id, run_month, "all", "all", overall_metric)

        # 6b) Severity breakdown
        for sev in ["caution", "minor", "major", "critical"]:
            subset = [r for r in run_rows if (r.get("checkpoint_severity") or "").lower() == sev]
            _insert_metric(repo_cur, run_id, run_month, "severity", sev, _compute_metrics(subset))

        # 6c) Category breakdown
        for cat in ["AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"]:
            subset = [r for r in run_rows if (r.get("checkpoint_category") or "").upper() == cat]
            _insert_metric(repo_cur, run_id, run_month, "category", cat, _compute_metrics(subset))

        # 6d) SENİN İSTEĞİN: overall(all/all) metriğini assessment_runs tablosuna snapshot olarak yaz
        # Böylece ileride assessment run listesinde kompleks JOIN ihtiyacı azalır.
        (
            total,
            success,
            fail,
            error,
            success_pct,
            _fail_pct,        # assessment_runs'ta kolon yok
            _error_pct,       # assessment_runs'ta kolon yok
            risk,
            _severity_sum,    # assessment_runs'ta kolon yok
            _failed_sev_sum,  # assessment_runs'ta kolon yok
            risk_level,
        ) = overall_metric

        repo_cur.execute(
            """
            UPDATE assessment_runs
               SET total_count=%s,
                   success_count=%s,
                   fail_count=%s,
                   error_count=%s,
                   success_pct=%s,
                   risk=%s,
                   risk_level=%s
             WHERE run_id=%s AND run_month=%s
            """,
            (
                total,
                success,
                fail,
                error,
                success_pct,
                risk,
                risk_level,
                run_id,
                run_month,
            ),
        )

        # 7) Run status final
        _mark_run_status("error" if had_error else "success")

        # Repo transaction commit
        repo_db.commit()

        return run_id, run_month

    except Exception:
        # Buraya düşerse run'da ciddi bir exception var.
        # Status'u error yapmaya çalışıyoruz. (Crash olursa started kalabilir; bu tasarım gereği kabul)
        _mark_run_status("error")
        try:
            repo_db.commit()
        except Exception:
            pass
        raise

    finally:
        try:
            repo_db.close()
        except Exception:
            pass
