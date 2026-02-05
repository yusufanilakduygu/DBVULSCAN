# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import render_template
from weasyprint import CSS, HTML

from db import get_db


# ✅ CSS artık dosyada değil, burada:
ASSESSMENT_RUN_DETAIL_CSS = r"""
:root{
  --font-mono: "DejaVu Sans Mono", "Liberation Mono", "Courier New", monospace;
}

html, body{
  margin:0;
  padding:0;
  background:#fff;
  color:#000;
  font-family: var(--font-mono);
  font-size: 8pt;
  line-height: 1.22;
}

@page{
  size: A4;
  margin: 10mm 9mm 12mm 9mm;

  @top-right{
    content: "OmniRiskDB by Arbo Security";
    color: #666;
    font-size: 6.5pt;
  }
  @bottom-right{
    content: "Page " counter(page) " / " counter(pages);
    font-size: 7pt;
  }
}

.shadow-box{
  border: 1px solid #9aa3ad;
  border-radius: 2px;
  background: #fff;
  box-shadow: 1.2px 1.2px 0 rgba(0,0,0,0.22);
}

.report-title{
  text-align:center;
  font-size: 19pt;
  font-weight: 800;
  margin: 3mm 0 4mm 0;
}

.box-title{
  font-weight: 900;
  font-size: 10.5pt;
  margin: 2mm 0 1mm 0;
}

.summary-box{
  padding: 3mm 4mm;
  margin-bottom: 4mm;
}

.kv-row{
  display:flex;
  gap: 6px;
  margin: 1.2mm 0;
}
.kv-key{ font-weight:900; width: 42mm; }
.kv-sep{ width: 4mm; text-align:center; }
.kv-val{ flex:1; }

.section{ margin: 0 0 4mm 0; }
.section-title{
  font-weight: 900;
  font-size: 10.5pt;
  margin: 2mm 0 1mm 0;
}

.table-wrap{ margin: 0 0 3mm 0; }

.tbl{
  width:100%;
  border-collapse: collapse;
  font-size: 8pt;
}

.tbl th, .tbl td{
  border:1px solid #8b8b8b;
  padding:3px 5px;
  word-break: break-word;
}

.tbl-metrics thead th{
  background:#f0f2f5;
}

.th-multi{ text-align:center; }

/* Badges */
.badge{
  display:inline-block;
  padding: 1.2px 7px;
  border-radius: 999px;
  border: 1px solid #cfd6de;
  background: #f8fafc;
  font-weight: 900;
}

/* Status badges */
.status-success{ color:#2f6b2f; background:#edf5ed; border-color:#cfe7cf; }
.status-fail{ color:#8f0000; background:#f6dede; border-color:#f0bcbc; }
.status-error{ color:#a45a00; background:#fbe9d8; border-color:#f4c7a6; }
.status-na{ color:#334155; background:#f1f5f9; border-color:#cfd6de; }

/* Asset impact badges (1..5) */
.impact{
  display:inline-block;
  padding: 1.2px 7px;
  border-radius: 999px;
  border: 1px solid #cfd6de;
  background: #f8fafc;
  font-weight: 900;
  white-space: nowrap;
}
.impact-1{ color:#2f6b2f; background:#edf5ed; border-color:#cfe7cf; }
.impact-2{ color:#5a6b00; background:#f0f7d8; border-color:#d9e7a6; }
.impact-3{ color:#8a6d00; background:#fbf4d8; border-color:#f2df95; }
.impact-4{ color:#a45a00; background:#fbe9d8; border-color:#f4c7a6; }
.impact-5{ color:#8f0000; background:#f6dede; border-color:#f0bcbc; }
.impact-na{ color:#334155; background:#f1f5f9; border-color:#cfd6de; }

/* Risk box */
.risk-box{ padding: 0 6mm; }
.risk-padding-top{ height: 4mm; }
.risk-padding-bottom{ height: 4mm; }

.risk-label{ font-weight:900; }
.risk-desc{ margin-top: 1.4mm; color:#222; }

.risk-separator{
  border-top:1px solid #c9cfd6;
  margin:4mm 0;
}

/* Risk level badges */
.lvl{
  display:inline-block;
  padding: 1.2px 7px;
  border-radius: 999px;
  border: 1px solid #cfd6de;
  background: #f8fafc;
  font-weight: 900;
  white-space: nowrap;
}
.lvl-low{ color:#2f6b2f; background:#edf5ed; }
.lvl-medium{ color:#8a6d00; background:#fbf4d8; }
.lvl-high{ color:#a45a00; background:#fbe9d8; }
.lvl-critical{ color:#8f0000; background:#f6dede; }

/* --- PDF table behavior --- */
thead{ display: table-header-group; }

/* ✅ sadece tablolarda row bölünmesini engelle */
.tbl tr{ break-inside: avoid; }

/* -----------------------------
   CATEGORY METRICS – COMPACT
------------------------------ */

.tbl-cat-matrix{
  font-size: 4.5pt;
  line-height: 1.05;
}
.tbl-cat-matrix th,
.tbl-cat-matrix td{
  padding: 1px 2px;
}
.tbl-cat-matrix thead th{
  font-size: 4.5pt;
  font-weight: 800;
}
.tbl-cat-matrix .mx-subhead{
  font-size: 4pt;
  font-weight: 700;
}
.tbl-cat-matrix .mx-num{
  text-align:center;
  font-variant-numeric: tabular-nums;
  font-size: 4.5pt;
}
.tbl-cat-matrix .mx-left{ text-align:left; }
.tbl-cat-matrix .mx-vsep{ border-right: 2px solid #000; }
.tbl-cat-matrix .mx-group-sep{ border-right: 2px solid #000; }
.tbl-cat-matrix .badge,
.tbl-cat-matrix .lvl{
  font-size: 4.5pt;
  padding: 0.5px 4px;
}

/* -----------------------------
   CHECKPOINT TEST RESULTS
------------------------------ */

.cat-gap{ height: 5mm; }

/* Category header: Category + Risk blocks (left, close) */
.cat-head{
  display:grid;
  grid-template-columns: auto 1fr;
  align-items: start;
  column-gap: 10mm;
  row-gap: 1mm;
}

.cat-title{
  font-weight: 900;
  font-size: 9.5pt;
  margin: 0;
}

/* Risk block (left, stacked) */
.cat-risk{
  justify-self: start;
  display:flex;
  flex-direction: column;
  gap: 1mm;
  font-size: 7.5pt;
  color:#111;
}

.cat-risk-item{
  display:flex;
  align-items:center;
  gap: 1.5mm;
}

.cat-risk-key{
  font-weight:900;
  color:#000;
  min-width: 22mm;
}

.cat-risk-val{
  font-variant-numeric: tabular-nums;
  min-width: 12mm;
  text-align:right;
}

.cat-risk-sep{ display:none; }

.cat-sep{
  border-top: 1px solid #000;
  margin: 1.5mm 0;
}

/* Category -> first checkpoint gap */
.cat-cp-gap{ height: 3.5mm; }

/* ✅ No checkpoints message */
.no-cp{
  border: 1px dashed #9aa3ad;
  border-radius: 2px;
  padding: 2.2mm 2.2mm;
  font-style: italic;
  color: #111;
  background: #fff;
  margin: 0 0 3.5mm 0;
}

/* ✅ Checkpoint blocks */
.cp-block{
  padding: 2.5mm 2.5mm 3mm 2.5mm;
  margin: 3.5mm 0;
  break-inside: auto;
  page-break-inside: auto;
}

/* Checkpoint header table: fixed layout */
.tbl-cp-head{
  margin: 0;
  table-layout: fixed;
  break-inside: avoid;
  page-break-inside: avoid;
}
.tbl-cp-head th, .tbl-cp-head td{
  padding: 2.5px 4px;
  vertical-align: top;
}

.col-id{ width: 9mm; text-align:center; }
.col-sev{ width: 22mm; text-align:center; }
.col-res{ width: 20mm; text-align:center; }
.col-name{ width: auto; }

.checkpoint-name{ font-weight: 700; }

.cp-name-line{ margin-bottom: 1.2mm; }

/* Checkpoint description table */
.tbl-cp-desc{
  width: 100%;
  border-collapse: collapse;
  margin-top: 1mm;
  break-inside: avoid;
  page-break-inside: avoid;
}
.tbl-cp-desc td{
  border: 1px solid #cfd6de;
  padding: 1.8mm 2mm;
  font-weight: 400;
  font-size: 7.6pt;
  line-height: 1.25;
  background: #fff;
}

/* Severity badges */
.sev{
  display:inline-block;
  padding: 1.2px 7px;
  border-radius: 999px;
  border: 1px solid #cfd6de;
  background: #f8fafc;
  font-weight: 900;
  white-space: nowrap;
}
.sev-critical{ color:#8f0000; background:#f6dede; border-color:#f0bcbc; }
.sev-major{ color:#a45a00; background:#fbe9d8; border-color:#f4c7a6; }
.sev-minor{ color:#8a6d00; background:#fbf4d8; border-color:#f2df95; }
.sev-caution{ color:#2f6b2f; background:#edf5ed; border-color:#cfe7cf; }

/* Result badges */
.res{
  display:inline-block;
  padding: 1.2px 7px;
  border-radius: 999px;
  border: 1px solid #cfd6de;
  background: #f8fafc;
  font-weight: 900;
  white-space: nowrap;
}
.res-pass{ color:#2f6b2f; background:#edf5ed; border-color:#cfe7cf; }
.res-fail{ color:#8f0000; background:#f6dede; border-color:#f0bcbc; }
.res-error{ color:#a45a00; background:#fbe9d8; border-color:#f4c7a6; }

/* Test Result / Evidence headers */
.cp-subtitle{
  margin: 3mm 0;
  font-weight: 900;
  font-size: 9pt;
  break-inside: avoid;
  page-break-after: avoid;
}

/* Text blocks */
.cp-text{
  white-space: pre-wrap;
  word-break: break-word;
  border: 1px solid #9aa3ad;
  border-radius: 2px;
  padding: 2.2mm 2.2mm;
  break-inside: auto;
  page-break-inside: auto;
}
"""


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


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

            # Checkpoints (grouped by Category)
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
            css_href="",  # PDF uses embedded CSS
        )

        # ✅ CSS doğrudan string'ten geçiliyor
        base_url = _project_root().resolve().as_uri() + "/"
        pdf_bytes = HTML(string=html_str, base_url=base_url).write_pdf(
            stylesheets=[CSS(string=ASSESSMENT_RUN_DETAIL_CSS)]
        )

        pdf_file.write_bytes(pdf_bytes)
        return str(pdf_file)

    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass
