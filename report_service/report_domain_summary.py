# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from weasyprint import CSS, HTML

from db import get_db


def _project_root() -> Path:
    """
    /home/anil/dbvulscan/report_service/report_domain_summary.py
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


def _pdf_path(report_dir: Path, domain_run_id: int) -> Path:
    # FINAL combined cache adı
    return report_dir / f"domain_run_{domain_run_id}_summary.pdf"


def _tmp_header_pdf_path(report_dir: Path, domain_run_id: int) -> Path:
    return report_dir / f".tmp_domain_run_{domain_run_id}_header.pdf"


def _fmt_dt(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v)


def _escape_html(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _css_inline() -> CSS:
    """
    Assessment Summary Report formatına yakın bir görünüm:
    - Top-right header: OmniRiskDB by Arbo Security
    - Büyük ortalı başlık
    - Info box + grid tables
    """
    return CSS(
        string=r"""
@page {
  size: A4;
  margin: 14mm 14mm 16mm 14mm;

  /* Sağ üstte "header/footer" benzeri watermark */
@top-right {
  content: "OmniRiskDB by Arbo Security";
  font-family: Arial, Helvetica, sans-serif;
  font-size: 9px;
  font-weight: 400;
  letter-spacing: 0;
  color: #64748b;
}

  /* İstersen sayfa no (çok hafif) */
  @bottom-right {
    content: counter(page);
    font-size: 9px;
    color: #94a3b8;
  }
}

* { box-sizing: border-box; }

body {
  font-family: "Courier New", Courier, monospace;
  color: #0f172a;
  font-size: 11px;
  line-height: 1.25;
}

.title {
  text-align: center;
  font-size: 22px;
  font-weight: 700;
  letter-spacing: 0.5px;
  margin: 12px 0 16px 0;
}

.section {
  margin-top: 14px;
}

.section h2 {
  font-size: 13px;
  font-weight: 700;
  margin: 0 0 6px 0;
}

.info-box {
  border: 1px solid #94a3b8;
  border-radius: 2px;
  padding: 10px 12px;
}

.kv {
  width: 100%;
  border-collapse: collapse;
}

.kv td {
  padding: 2px 4px;
  vertical-align: top;
}

.kv td.k { width: 210px; font-weight: 700; }
.kv td.c { width: 12px; text-align: center; font-weight: 700; }
.kv td.v { width: auto; }

.badge {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 999px;
  border: 1px solid #cbd5e1;
  font-size: 10px;
  font-weight: 700;
}

.badge.success { background: #ecfdf5; border-color: #a7f3d0; color: #065f46; }
.badge.partial { background: #fffbeb; border-color: #fcd34d; color: #92400e; }
.badge.incomplete { background: #eff6ff; border-color: #bfdbfe; color: #1d4ed8; }
.badge.error { background: #fef2f2; border-color: #fecaca; color: #991b1b; }

.table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 6px;
}

.table th, .table td {
  border: 1px solid #475569;
  padding: 6px 6px;
  vertical-align: top;
}

.table th {
  background: #f1f5f9;
  font-weight: 700;
}

.small-note {
  margin-top: 10px;
  font-size: 9px;
  color: #64748b;
}
"""
    )


def _render_html(hdr: Dict[str, Any], assessment_rows: List[Dict[str, Any]]) -> str:
    # --- Domain Info values (DATA SAME) ---
    domain_name = _escape_html(hdr.get("domain_name"))
    domain_id = _escape_html(hdr.get("domain_id"))
    domain_run_id = _escape_html(hdr.get("domain_run_id"))
    started_at = _escape_html(_fmt_dt(hdr.get("started_at")))
    status = (hdr.get("status") or "").strip()
    status_html = _escape_html(status)

    badge_class = (status.lower() if status else "").strip()
    if badge_class not in ("success", "partial", "incomplete", "error"):
        badge_class = "incomplete" if badge_class else ""

    # --- Assessment Results rows (DATA SAME) ---
    rows_html: List[str] = []
    for r in assessment_rows:
        st = (r.get("status") or "")
        st_cls = _escape_html(st.lower())
        rows_html.append(
            f"""
<tr>
  <td>{_escape_html(r.get("assessment_name"))}</td>
  <td><span class="badge {st_cls}">{_escape_html(st)}</span></td>
  <td>{_escape_html(r.get("success_pct"))}</td>
  <td>{_escape_html(r.get("risk"))}</td>
  <td>{_escape_html(r.get("risk_level"))}</td>
  <td>{_escape_html(r.get("asset_adjusted_risk"))}</td>
  <td>{_escape_html(r.get("asset_adjusted_risk_level"))}</td>
</tr>
""".strip()
        )

    if not rows_html:
        rows_html.append(
            """
<tr>
  <td colspan="7" style="color:#64748b;">No assessment_runs found for this domain_run_id.</td>
</tr>
""".strip()
        )

    # Title style: Assessment raporuna benzer
    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Domain Run Summary Report</title>
</head>
<body>

  <div class="title">Domain Run Summary Report</div>

  <div class="section">
    <h2>Domain Info</h2>
    <div class="info-box">
      <table class="kv">
        <tr><td class="k">Domain Name</td><td class="c">:</td><td class="v">{domain_name}</td></tr>
        <tr><td class="k">Domain Id</td><td class="c">:</td><td class="v">{domain_id}</td></tr>
        <tr><td class="k">Domain Run Id</td><td class="c">:</td><td class="v">{domain_run_id}</td></tr>
        <tr><td class="k">Report Execution Date</td><td class="c">:</td><td class="v">{started_at}</td></tr>
        <tr><td class="k">Domain Run Status</td><td class="c">:</td>
            <td class="v"><span class="badge {badge_class}">{status_html}</span></td></tr>
      </table>
    </div>
  </div>

  <div class="section">
    <h2>Assessment Results</h2>

    <table class="table">
      <thead>
        <tr>
          <th style="width: 26%;">Assessment Name</th>
          <th style="width: 12%;">Status</th>
          <th style="width: 10%;">Success (%)</th>
          <th style="width: 8%;">Risk</th>
          <th style="width: 12%;">Risk Level</th>
          <th style="width: 12%;">Asset-Adjusted Risk</th>
          <th style="width: 20%;">Asset-Adjusted Risk Level</th>
        </tr>
      </thead>
      <tbody>
        {"".join(rows_html)}
      </tbody>
    </table>

    <div class="small-note">
      Generated by report_service.report_domain_summary
    </div>
  </div>

</body>
</html>
""".strip()
    return html


def _merge_pdfs(pdf_paths: List[Path], out_path: Path) -> None:
    """
    pdf_paths sırasıyla birleştirip out_path'e yazar.
    Dış bağımlılık: pypdf (tercih) veya PyPDF2.
    """
    try:
        from pypdf import PdfReader, PdfWriter  # type: ignore
    except Exception:
        PdfReader = None
        PdfWriter = None

    if PdfReader is None or PdfWriter is None:
        try:
            from PyPDF2 import PdfReader as PdfReader2  # type: ignore
            from PyPDF2 import PdfWriter as PdfWriter2  # type: ignore
            PdfReader = PdfReader2
            PdfWriter = PdfWriter2
        except Exception as e:
            raise RuntimeError(
                "PDF merge için 'pypdf' veya 'PyPDF2' gerekli. "
                "Lütfen venv'e paket ekleyin. (pip install pypdf)"
            ) from e

    writer = PdfWriter()
    for p in pdf_paths:
        reader = PdfReader(str(p))
        for page in reader.pages:
            writer.add_page(page)

    with out_path.open("wb") as f:
        writer.write(f)


def generate(domain_run_id: int, output_dir: Optional[str] = None, force: bool = False) -> str:
    """
    Domain Run Summary PDF üretir ve aynı PDF'in devamına
    her bir assessment_run için report_assessment_summary.generate(run_id) çıktısını ekler (merge).

    - report_dir: settings(setting_key='report_dir') veya output_dir override
    - cache: domain_run_{domain_run_id}_summary.pdf (COMBINED)
    - force=False ve dosya varsa: reuse
    - Dönüş: PDF path (string)
    """
    rep_dir = _report_dir(output_dir=output_dir)
    combined_pdf = _pdf_path(rep_dir, domain_run_id)

    if combined_pdf.exists() and not force:
        return str(combined_pdf)

    header_tmp = _tmp_header_pdf_path(rep_dir, domain_run_id)

    con = None
    try:
        con = get_db()
        with con.cursor() as cur:
            # 1) Domain run + domain info (DATA SAME)
            cur.execute(
                """
                SELECT
                  dr.domain_run_id,
                  dr.domain_id,
                  dr.started_at,
                  dr.status,
                  d.name AS domain_name
                FROM domain_runs dr
                JOIN domains d ON d.domain_id = dr.domain_id
                WHERE dr.domain_run_id=%s
                LIMIT 1
                """,
                (domain_run_id,),
            )
            hdr = cur.fetchone()
            if not hdr:
                raise RuntimeError(f"domain_runs içinde domain_run_id={domain_run_id} bulunamadı.")

            # 2) Assessment rows (DATA SAME) + run_id list for calling report_assessment_summary
            cur.execute(
                """
                SELECT
                  run_id,
                  assessment_name,
                  status,
                  success_pct,
                  risk,
                  risk_level,
                  asset_adjusted_risk,
                  asset_adjusted_risk_level
                FROM assessment_runs
                WHERE domain_run_id=%s
                ORDER BY executed_at ASC, run_id ASC
                """,
                (domain_run_id,),
            )
            rows = cur.fetchall() or []

        # Domain summary html -> header tmp pdf
        table_rows: List[Dict[str, Any]] = []
        run_ids: List[int] = []

        for r in rows:
            if isinstance(r, dict):
                rid = r.get("run_id")
                if rid is not None:
                    try:
                        run_ids.append(int(rid))
                    except Exception:
                        pass
                tr = dict(r)
                tr.pop("run_id", None)
                table_rows.append(tr)
            else:
                rid = r[0]
                try:
                    run_ids.append(int(rid))
                except Exception:
                    pass
                table_rows.append(
                    {
                        "assessment_name": r[1],
                        "status": r[2],
                        "success_pct": r[3],
                        "risk": r[4],
                        "risk_level": r[5],
                        "asset_adjusted_risk": r[6],
                        "asset_adjusted_risk_level": r[7],
                    }
                )

        html_str = _render_html(hdr=hdr, assessment_rows=table_rows)

        base_url = _project_root().resolve().as_uri() + "/"
        header_bytes = HTML(string=html_str, base_url=base_url).write_pdf(
            stylesheets=[_css_inline()]
        )
        header_tmp.write_bytes(header_bytes)

        # 3) Her assessment için assessment summary PDF üret (single source of truth)
        try:
            from report_service import report_assessment_summary
        except Exception as e:
            raise RuntimeError("report_service.report_assessment_summary import edilemedi.") from e

        assessment_pdfs: List[Path] = []
        for run_id in run_ids:
            p = report_assessment_summary.generate(run_id, output_dir=str(rep_dir), force=force)
            assessment_pdfs.append(Path(p))

        # 4) Merge: header + all assessment pdfs
        merge_list = [header_tmp] + assessment_pdfs
        _merge_pdfs(merge_list, combined_pdf)

        return str(combined_pdf)

    finally:
        try:
            if header_tmp.exists():
                header_tmp.unlink()
        except Exception:
            pass

        if con:
            try:
                con.close()
            except Exception:
                pass
