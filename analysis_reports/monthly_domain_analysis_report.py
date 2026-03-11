# -*- coding: utf-8 -*-
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib import colors


@dataclass(frozen=True)
class MonthlyReportResult:
    filename: str
    abs_path: str


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _ensure_report_dir() -> Path:
    report_dir = _project_root() / "report_dir"
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir


def _make_sequence() -> str:
    return f"{random.randint(10000, 99999)}"


def _draw_header_footer(c: canvas.Canvas, *, page_no: int, page_total: int, width: float, height: float) -> None:
    c.setFillColor(colors.black)

    # Smaller + italic brand line
    c.setFont("Helvetica-Oblique", 7)
    c.drawRightString(width - 40, height - 30, "OmniRiskDB by Arbo Security")

    c.setFont("Helvetica", 8)
    c.drawRightString(width - 40, 28, f"Page {page_no} / {page_total}")


def _draw_assessment_statistics_table(
    c: canvas.Canvas,
    *,
    x: float,
    y_top: float,
    width: float,
    values: dict,
    as_of_date: str | None = None,
) -> float:
    # IMPORTANT:
    # Header increased so "As of End Date (...)" is fully readable under the title.
    header_h = 44
    row_h = 24

    rows = [
        ("The Number of Security Assessment Tests with Errors", int(values.get("tests_with_errors", 0))),
        ("The Number of Security Assessments with Errors", int(values.get("assessments_with_errors", 0))),
        ("The number of Security Assessment Tests", int(values.get("total_tests", 0))),
        ("The number of Security Assessments", int(values.get("total_assessments", 0))),
    ]

    table_h = header_h + (len(rows) * row_h)

    # outer border
    c.setLineWidth(1)
    c.setStrokeColor(colors.lightgrey)
    c.rect(x, y_top - table_h, width, table_h, stroke=1, fill=0)

    # header background
    c.setFillColor(colors.whitesmoke)
    c.rect(x, y_top - header_h, width, header_h, stroke=0, fill=1)

    # header title
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 12)
    c.drawCentredString(x + width / 2, y_top - 18, "Assessment Statistics")

    # as-of line (now fully visible, under the title)
    if as_of_date:
        c.setFont("Helvetica", 9)
        c.drawCentredString(x + width / 2, y_top - 34, f"As of End Date ({as_of_date})")

    value_col_w = 70
    label_col_w = width - value_col_w

    # value column divider
    c.setStrokeColor(colors.lightgrey)
    c.setLineWidth(1)
    c.line(x + label_col_w, y_top - header_h, x + label_col_w, y_top - table_h)

    # rows
    y = y_top - header_h
    for i, (label, val) in enumerate(rows):
        if i % 2 == 0:
            c.setFillColor(colors.white)
        else:
            c.setFillColor(colors.HexColor("#f3f4f6"))
        c.rect(x, y - row_h, width, row_h, stroke=0, fill=1)

        c.setStrokeColor(colors.lightgrey)
        c.line(x, y, x + width, y)

        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(x + 10, y - 16, label)

        c.setFont("Helvetica-Bold", 9)
        c.drawRightString(x + width - 10, y - 16, str(val))

        y -= row_h

    return y_top - table_h


def _draw_distribution_of_tests_by_importance_grouped(
    c: canvas.Canvas,
    *,
    x: float,
    y_top: float,
    width: float,
    start_values: dict,
    end_values: dict,
    start_date: str,
    end_date: str
) -> float:
    """
    Legacy gibi:
    - Her severity için 2 bar (StartDate / EndDate)
    - Legend: Start Date (blue) / End Date (green)
    - Legend: caption'ın üstünde olacak
    """

    def _v(d: dict, k: str) -> int:
        return int((d or {}).get(k, 0) or 0)

    labels = ["CRITICAL", "MAJOR", "MINOR"]
    keys = ["critical", "major", "minor"]

    start_data = [_v(start_values, k) for k in keys]
    end_data = [_v(end_values, k) for k in keys]

    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(x, y_top, "Distribution of Tests by Importance")

    chart_top = y_top - 20
    chart_h = 150
    chart_bottom = chart_top - chart_h

    c.setStrokeColor(colors.lightgrey)
    c.setLineWidth(1)
    c.line(x, chart_bottom, x + width, chart_bottom)
    c.line(x, chart_bottom, x, chart_top)

    max_val = max(start_data + end_data) if max(start_data + end_data) > 0 else 1

    c.saveState()
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 9)
    c.translate(x + 18, (chart_bottom + chart_top) / 2)
    c.rotate(90)
    c.drawCentredString(0, 0, "The number of Failed Tests")
    c.restoreState()

    c.setFont("Helvetica-Bold", 9)
    c.drawCentredString(x + width / 2, chart_bottom - 28, "Importance of Tests")

    left_pad = 70
    right_pad = 20
    usable_w = width - left_pad - right_pad

    groups = 3
    group_w = usable_w / groups
    bar_w = 28
    inner_gap = 10

    for i in range(groups):
        gx = x + left_pad + i * group_w + (group_w - (2 * bar_w + inner_gap)) / 2

        sv = start_data[i]
        sh = (sv / max_val) * (chart_h - 10)
        c.setFillColor(colors.HexColor("#3b82b6"))
        c.rect(gx, chart_bottom, bar_w, sh, stroke=0, fill=1)

        ev = end_data[i]
        eh = (ev / max_val) * (chart_h - 10)
        c.setFillColor(colors.HexColor("#10b981"))
        c.rect(gx + bar_w + inner_gap, chart_bottom, bar_w, eh, stroke=0, fill=1)

        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 9)
        c.drawCentredString(gx + bar_w / 2, chart_bottom + sh + 6, str(sv))
        c.drawCentredString(gx + bar_w + inner_gap + bar_w / 2, chart_bottom + eh + 6, str(ev))

        c.setFont("Helvetica-Bold", 9)
        c.drawCentredString(x + left_pad + i * group_w + group_w / 2, chart_bottom - 12, labels[i])

    # legend above caption (bottom-centered)
    legend_y = chart_bottom - 60
    c.setFont("Helvetica", 8)

    legend_x = x + (width / 2) - 110

    c.setFillColor(colors.HexColor("#3b82b6"))
    c.rect(legend_x, legend_y, 10, 10, stroke=0, fill=1)
    c.setFillColor(colors.black)
    c.drawString(legend_x + 14, legend_y + 1, f"Start Date ({start_date})")

    c.setFillColor(colors.HexColor("#10b981"))
    c.rect(legend_x + 140, legend_y, 10, 10, stroke=0, fill=1)
    c.setFillColor(colors.black)
    c.drawString(legend_x + 154, legend_y + 1, f"End Date ({end_date})")

    caption_y = legend_y - 16
    c.setFillColor(colors.black)
    c.setFont("Helvetica", 8)
    c.drawString(
        x,
        caption_y,
        "This distribution compares failed tests by severity for the selected domain on the start and end dates."
    )

    return caption_y - 8


def generate_monthly_domain_analysis_pdf(
    *,
    domain_id: str,
    domain_name: str,
    start_date: str,
    end_date: str,
    stats: dict | None = None,
    severity_dist_start: dict | None = None,
    severity_dist_end: dict | None = None
) -> MonthlyReportResult:
    if stats is None:
        stats = {}
    if severity_dist_start is None:
        severity_dist_start = {}
    if severity_dist_end is None:
        severity_dist_end = {}

    report_dir = _ensure_report_dir()
    seq = _make_sequence()
    filename = f"monthly_analysis_{domain_id}_{seq}.pdf"
    out_path = report_dir / filename

    c = canvas.Canvas(str(out_path), pagesize=A4)
    width, height = A4

    _draw_header_footer(c, page_no=1, page_total=1, width=width, height=height)

    # Move main title slightly upward
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 20)
    c.drawCentredString(width / 2, height - 85, "Monthly Domain Analysis Report")

    c.setLineWidth(1)
    c.setStrokeColor(colors.lightgrey)
    c.line(40, height - 108, width - 40, height - 108)

    c.setStrokeColor(colors.black)
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(40, height - 145, "Domain Info")

    box_x = 40
    box_y_top = height - 160
    box_w = width - 80
    box_h = 115

    c.setLineWidth(1)
    c.setStrokeColor(colors.lightgrey)
    c.rect(box_x, box_y_top - box_h, box_w, box_h, stroke=1, fill=0)

    label_x = box_x + 18
    colon_x = box_x + 175
    value_x = box_x + 190

    y = box_y_top - 22
    line_gap = 16

    c.setFont("Helvetica-Bold", 9)
    c.drawString(label_x, y, "Domain")
    c.setFont("Helvetica", 9)
    c.drawString(colon_x, y, ":")
    c.drawString(value_x, y, f"{domain_name} (ID={domain_id})")

    y -= line_gap
    c.setFont("Helvetica-Bold", 9)
    c.drawString(label_x, y, "Report Execution Date")
    c.setFont("Helvetica", 9)
    c.drawString(colon_x, y, ":")
    c.drawString(value_x, y, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    y -= line_gap
    c.setFont("Helvetica-Bold", 9)
    c.drawString(label_x, y, "Start Date")
    c.setFont("Helvetica", 9)
    c.drawString(colon_x, y, ":")
    c.drawString(value_x, y, str(start_date))

    y -= line_gap
    c.setFont("Helvetica-Bold", 9)
    c.drawString(label_x, y, "End Date")
    c.setFont("Helvetica", 9)
    c.drawString(colon_x, y, ":")
    c.drawString(value_x, y, str(end_date))

    # Assessment Statistics block (end_date için)
    stats_table_y_top = (box_y_top - box_h) - 25
    stats_bottom_y = _draw_assessment_statistics_table(
        c,
        x=40,
        y_top=stats_table_y_top,
        width=width - 80,
        values=stats,
        as_of_date=str(end_date),
    )

    # Grouped Distribution chart
    chart_y_top = stats_bottom_y - 35
    _draw_distribution_of_tests_by_importance_grouped(
        c,
        x=40,
        y_top=chart_y_top,
        width=width - 80,
        start_values=severity_dist_start,
        end_values=severity_dist_end,
        start_date=str(start_date),
        end_date=str(end_date),
    )

    c.setFillColor(colors.black)
    c.setFont("Helvetica-Oblique", 8)
    c.drawString(40, 55, "Generated by DBVulScan - Monthly Domain Analysis")

    c.showPage()
    c.save()

    return MonthlyReportResult(filename=filename, abs_path=str(out_path))