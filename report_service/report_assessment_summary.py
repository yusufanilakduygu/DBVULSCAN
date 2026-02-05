# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import render_template
from weasyprint import CSS, HTML

from db import get_db


# ✅ CSS artık dosyada değil, burada:
ASSESSMENT_RUN_SUMMARY_CSS = r"""
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
  margin: 10mm 14mm 12mm 14mm;

  @top-right{
    content: "OmniRiskDB by Arbo Security";
    font-size: 6.5pt;
    color: #666;
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
  margin: 0 0 5mm 0;
}

.box-title,
.section-title{
  font-weight: 900;
  font-size: 10.5pt;
  margin-bottom: 3mm;
}

.section{ margin-top: 7mm; }

.summary-box{ padding: 5mm 6mm; }

.kv-row{
  display:grid;
  grid-template-columns: 60mm 6mm 1fr;
  gap: 1.5mm;
  margin: 0.6mm 0;
}

.kv-key{ font-weight: 800; }

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

/* Category blocks in Checkpoint Test Results */
.cat-title{
  font-weight: 900;
  font-size: 9.5pt;
  margin: 0 0 2mm 0;
}

.cat-sep{
  border-top: 1px solid #000;
  margin: 1.5mm 0;
}

/* NEW: Space between categories */
.cat-gap{
  height: 4mm;   /* “bir satır boşluk” hissi */
}

.tbl-cat-metrics thead th{
  background:#f0f2f5;
}

.res-fail{ color:#b00000; font-weight:900; }

/* Base badge styles */
.lvl,
.sev,
.impact{
  display:inline-block;
  padding:1px 5px;
  border-radius:2px;
  font-weight:900;
}

/* Risk box spacing */
.risk-box{ padding: 0 6mm; }
.risk-padding-top{ height: 4mm; }
.risk-padding-bottom{ height: 4mm; }

.risk-label{ font-weight:900; }
.risk-separator{
  border-top:1px solid #c9cfd6;
  margin:4mm 0;
}

/* Risk badges */
.lvl-low{ color:#2f6b2f; background:#edf5ed; }
.lvl-medium{ color:#8a6d00; background:#fbf4d8; }
.lvl-high{ color:#a45a00; background:#fbe9d8; }
.lvl-critical{ color:#8f0000; background:#f6dede; }

/* Asset impact badges */
.impact-very-low{ color:#2f6b2f; background:#edf5ed; }
.impact-low{ color:#2f6b2f; background:#edf5ed; }
.impact-medium{ color:#8a6d00; background:#fbf4d8; }
.impact-high{ color:#a45a00; background:#fbe9d8; }
.impact-critical{ color:#8f0000; background:#f6dede; }

/* Severity badges (mat) */
.sev-critical{ color:#8f0000; background:#f6dede; }
.sev-major{ color:#a45a00; background:#fbe9d8; }
.sev-minor{ color:#8a6d00; background:#fbf4d8; }
.sev-caution{ color:#2f6b2f; background:#edf5ed; }

thead{ display: table-header-group; }
tr{ break-inside: avoid; }
"""


def _project_root() -> Path:
    """
    /home/anil/dbvulscan/report_service/report_assessment_summary.py
    -> parents[1] = /home/anil/dbvulscan
    """
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
            raise RuntimeError(
                "settings tablosunda setting_key='report_dir' bulunamadı. "
                "Report üretimi için report_dir zorunlu."
            )
        p = Path(v)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _pdf_path(report_dir: Path, run_id: int) -> Path:
    # FINAL cache adı
    return report_dir / f"assessment_run_{run_id}_summary.pdf"


def generate(run_id: int, output_dir: Optional[str] = None, force: bool = False) -> str:
    """
    Assessment Run Summary PDF üretir.

    - report_dir: settings(setting_key='report_dir') veya output_dir override
    - cache: assessment_run_{run_id}_summary.pdf
    - force=False ve dosya varsa: reuse
    - Dönüş: PDF path (string)
    """
    rep_dir = _report_dir(output_dir=output_dir)
    pdf_file = _pdf_path(rep_dir, run_id)

    if pdf_file.exists() and not force:
        return str(pdf_file)

    con = None
    try:
        con = get_db()
        with con.cursor() as cur:
            # 1) Summary + Metrics (assessment_runs)
            cur.execute(
                """
                SELECT
                  run_id,
                  assessment_id,
                  executed_at,
                  status,
                  assessment_name,
                  db_type,
                  datasource_name,
                  benchmark_name,

                  total_count,
                  success_count,
                  fail_count,
                  error_count,
                  success_pct,
                  risk,
                  asset_adjusted_risk,

                  risk_level,
                  asset_adjusted_risk_level
                FROM assessment_runs
                WHERE run_id=%s
                LIMIT 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if not r:
                raise RuntimeError(f"assessment_runs içinde run_id={run_id} bulunamadı.")

            # 2) Asset impact (assessments authoritative)
            cur.execute(
                """
                SELECT asset_impact
                FROM assessments
                WHERE assessment_id=%s
                LIMIT 1
                """,
                (r["assessment_id"],),
            )
            arow = cur.fetchone()
            if arow and arow.get("asset_impact"):
                r["asset_impact"] = arow["asset_impact"]

            # 3) Risk level descriptions
            cur.execute(
                "SELECT description FROM risk_levels WHERE risk_level=%s LIMIT 1",
                (r["risk_level"],),
            )
            row1 = cur.fetchone()
            r["risk_level_desc"] = (row1.get("description") if row1 else "") or ""

            cur.execute(
                "SELECT description FROM risk_levels WHERE risk_level=%s LIMIT 1",
                (r["asset_adjusted_risk_level"],),
            )
            row2 = cur.fetchone()
            r["asset_risk_level_desc"] = (row2.get("description") if row2 else "") or ""

            # 4) Category Risk Map
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
            cat_metrics: Dict[str, Dict[str, Any]] = {}
            for cm in cat_metric_rows:
                dv = cm.get("dimension_value")
                if dv:
                    cat_metrics[str(dv)] = cm

            # 5) Checkpoints
            cur.execute(
                """
                SELECT
                  run_checkpoint_id,
                  checkpoint_name,
                  checkpoint_severity,
                  checkpoint_category,
                  test_result
                FROM assessment_run_checkpoints
                WHERE run_id=%s
                """,
                (run_id,),
            )
            all_cp_rows = cur.fetchall() or []

            # Group by category
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
            report_categories = [c for c in categories_order if c in cp_by_category]

        # 6) HTML render (template reuse)
        html_str = render_template(
            "assessment_runs/report_assessment_run_summary.html",
            r=r,
            report_categories=report_categories,
            cp_by_category=cp_by_category,
            cat_metrics=cat_metrics,
            css_href="",  # PDF uses embedded CSS
        )

        # ✅ CSS dosyası yok: string'ten bas
        base_url = _project_root().resolve().as_uri() + "/"
        pdf_bytes = HTML(string=html_str, base_url=base_url).write_pdf(
            stylesheets=[CSS(string=ASSESSMENT_RUN_SUMMARY_CSS)]
        )

        pdf_file.write_bytes(pdf_bytes)
        return str(pdf_file)

    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass
