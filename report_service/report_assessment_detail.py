# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import render_template
from weasyprint import CSS, HTML

from db import get_db


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _css_path() -> Path:
    return _project_root() / "static" / "reports" / "assessment_run_detail.css"


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


def _report_dir(output_dir: Optional[str] = None) -> Path:
    if output_dir:
        p = Path(output_dir)
    else:
        v = _get_setting_value("report_dir")
        if not v:
            raise RuntimeError("settings tablosunda setting_key='report_dir' bulunamadı.")
        p = Path(v)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _pdf_path(report_dir: Path, run_id: int) -> Path:
    return report_dir / f"assessment_run_{run_id}_detail.pdf"


def _safe_filename_part(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return "unknown"
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_.-]", "", s)
    return s or "unknown"


def _format_exec_date(dt_val: Any) -> str:
    if isinstance(dt_val, datetime):
        return dt_val.strftime("%Y-%m-%d")
    if isinstance(dt_val, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(dt_val, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
    return "unknown-date"


def generate(run_id: int, output_dir: Optional[str] = None, force: bool = False) -> str:
    """
    Assessment Run Detail PDF üretir.

    - report_dir: settings(setting_key='report_dir') veya output_dir override
    - cache: assessment_run_{run_id}_detail.pdf
    - force=False ve dosya varsa: reuse
    - dönüş: pdf_path (str)
    """
    rep_dir = _report_dir(output_dir=output_dir)
    pdf_file = _pdf_path(rep_dir, run_id)

    if pdf_file.exists() and not force:
        return str(pdf_file)

    con = None
    try:
        con = get_db()
        with con.cursor() as cur:
            # Run snapshot (+ asset impact)
            cur.execute(
                """
                SELECT
                  r.run_id,
                  r.assessment_id,
                  r.executed_at,
                  r.status,
                  r.assessment_name,
                  r.db_type,
                  r.datasource_name,
                  r.benchmark_name,

                  a.asset_impact AS asset_impact,

                  r.total_count,
                  r.success_count,
                  r.fail_count,
                  r.error_count,
                  r.success_pct,
                  r.risk,
                  r.asset_adjusted_risk,

                  r.risk_level,
                  r.asset_adjusted_risk_level
                FROM assessment_runs r
                LEFT JOIN assessments a ON a.assessment_id = r.assessment_id
                WHERE r.run_id=%s
                LIMIT 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if not r:
                raise RuntimeError(f"assessment_runs içinde run_id={run_id} yok")

            # Risk level descriptions
            cur.execute(
                "SELECT description FROM risk_levels WHERE risk_level=%s LIMIT 1",
                (r["risk_level"],),
            )
            row1 = cur.fetchone()
            r["risk_level_desc"] = (row1.get("description") or "") if row1 else ""

            cur.execute(
                "SELECT description FROM risk_levels WHERE risk_level=%s LIMIT 1",
                (r["asset_adjusted_risk_level"],),
            )
            row2 = cur.fetchone()
            r["asset_risk_level_desc"] = (row2.get("description") or "") if row2 else ""

            # Category risk map (assessment_run_metrics)
            cur.execute(
                """
                SELECT
                  dimension_value,
                  risk,
                  risk_level,
                  asset_adjusted_risk,
                  asset_adjusted_risk_level
                FROM assessment_run_metrics
                WHERE run_id=%s AND dimension_type='category'
                ORDER BY dimension_value ASC
                """,
                (run_id,),
            )
            cat_metric_rows = cur.fetchall() or []
            cat_risk_map: Dict[str, Dict[str, Any]] = {}
            for cm in cat_metric_rows:
                dv = cm.get("dimension_value")
                if dv:
                    cat_risk_map[str(dv)] = cm

            # Category x Severity matrix (assessment_run_category_metrics)
            cur.execute(
                """
                SELECT category, severity, total_count, pass_count, fail_count, error_count
                FROM assessment_run_category_metrics
                WHERE run_id=%s
                """,
                (run_id,),
            )
            cat_rows = cur.fetchall() or []

            cat_matrix: Dict[str, Dict[str, Dict[str, Any]]] = {}
            for cr in cat_rows:
                c = cr.get("category")
                s = cr.get("severity")
                if not c or not s:
                    continue
                cat_matrix.setdefault(str(c), {})[str(s)] = cr

            # Checkpoints (grouped by Category)  ✅ test_message YOK -> doğru kolonlar
            cur.execute(
                """
                SELECT
                  run_checkpoint_id,
                  checkpoint_name,
                  checkpoint_description,
                  checkpoint_severity,
                  checkpoint_category,
                  test_result,
                  checkpoint_text_pass,
                  checkpoint_text_fail,
                  evidence_text,
                  error_text
                FROM assessment_run_checkpoints
                WHERE run_id=%s
                """,
                (run_id,),
            )
            all_cp_rows = cur.fetchall() or []

            cp_by_category: Dict[str, List[Dict[str, Any]]] = {}
            for row in all_cp_rows:
                cat = (row.get("checkpoint_category") or "").strip()
                if not cat:
                    continue
                cp_by_category.setdefault(cat, []).append(row)

            # Sort inside each category: FAIL -> ERROR -> PASS, then Severity, then id
            res_pri = {"fail": 1, "error": 2, "pass": 3}
            sev_pri = {"critical": 1, "major": 2, "minor": 3, "caution": 4}

            def _key(x: Dict[str, Any]):
                tr = (x.get("test_result") or "").lower()
                sv = (x.get("checkpoint_severity") or "").lower()
                return (
                    res_pri.get(tr, 9),
                    sev_pri.get(sv, 9),
                    int(x.get("run_checkpoint_id") or 0),
                )

            for _, items in cp_by_category.items():
                items.sort(key=_key)

            categories_order = ["AUTH", "PRIV", "CONFIG", "PATCH", "AUDIT", "ENCRYPT", "ACCOUNT", "OTHER"]
            report_categories = categories_order
            severities = ["critical", "major", "minor", "caution"]

        html_str = render_template(
            "assessment_runs/report_assessment_run_detail.html",
            r=r,
            report_categories=report_categories,
            cp_by_category=cp_by_category,
            cat_risk_map=cat_risk_map,
            cat_matrix=cat_matrix,
            severities=severities,
            css_href="",  # PDF uses CSS file directly
        )

        css_file = _css_path()
        if not css_file.exists():
            raise RuntimeError(f"CSS bulunamadı: {css_file}")

        base_url = _project_root().resolve().as_uri() + "/"
        pdf_bytes = HTML(string=html_str, base_url=base_url).write_pdf(
            stylesheets=[CSS(filename=str(css_file))]
        )

        pdf_file.write_bytes(pdf_bytes)
        return str(pdf_file)

    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass
