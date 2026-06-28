import json
import sys
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: export_quality_pdf.py <input.json> <output.pdf>", file=sys.stderr)
        return 2

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    payload = json.loads(input_path.read_text(encoding="utf-8-sig"))

    font_name = register_font()
    styles = build_styles(font_name)
    story = []

    story.append(Paragraph("数据质量检查报告", styles["title"]))
    story.append(Spacer(1, 4 * mm))

    summary_rows = [
        ["数据集", payload["dataset_name"]],
        ["源文件", payload["source_path"]],
        ["导入时间", payload["imported_at"]],
        ["工作表规模", f'{payload["row_count"]} 行 × {payload["column_count"]} 列'],
        ["主键候选", join_text(payload.get("key_candidates", []), chunk_size=6)],
        ["时间候选", join_text(payload.get("time_candidates", []), chunk_size=6)],
        ["生效主键", blank_to_dash(payload.get("resolved_primary_key"))],
        ["组合字段", join_text(payload.get("resolved_composite_keys", []), chunk_size=6)],
        ["时间序列列", blank_to_dash(payload.get("resolved_time_column"))],
    ]
    story.append(section_title("概况", styles))
    story.append(make_kv_table(summary_rows, styles))
    story.append(Spacer(1, 4 * mm))

    rules = payload.get("rules", {})
    rule_rows = [
        ["高缺失阈值", f'{float(rules.get("high_missing_threshold", 0.3)) * 100:.0f}%'],
        ["指定主键", blank_to_dash(rules.get("primary_key"))],
        ["指定组合键", join_text(rules.get("composite_keys", []), chunk_size=6)],
        ["指定时间列", blank_to_dash(rules.get("time_column"))],
    ]
    story.append(section_title("规则配置", styles))
    story.append(make_kv_table(rule_rows, styles))
    story.append(Spacer(1, 4 * mm))

    overview = payload.get("overview", {})
    overview_rows = [
        ["高缺失字段", str(overview.get("high_missing_field_count", 0))],
        ["整行缺失记录", str(overview.get("fully_empty_row_count", 0))],
        ["完全重复行", str(overview.get("duplicate_row_count", 0))],
        ["主键重复", str(overview.get("primary_key_duplicate_count", 0))],
        ["组合重复", str(overview.get("composite_duplicate_count", 0))],
        ["主键空值", str(overview.get("primary_key_empty_count", 0))],
        ["数值非法列", str(overview.get("numeric_invalid_column_count", 0))],
        ["混合类型列", str(overview.get("mixed_type_column_count", 0))],
        ["时间顺序异常", str(overview.get("time_order_issue_count", 0))],
        ["范围规则异常", str(overview.get("range_rule_issue_count", 0))],
    ]
    story.append(section_title("质量总览", styles))
    story.append(make_grid_table([["指标", "值"]] + overview_rows, styles, [42 * mm, 18 * mm]))
    story.append(Spacer(1, 4 * mm))

    issues = payload.get("issues", [])
    story.append(section_title("问题台账", styles))
    if issues:
        issue_rows = [["类别", "等级", "字段", "说明"]]
        for issue in issues:
            issue_rows.append(
                [
                    issue.get("category", ""),
                    issue.get("severity", ""),
                    issue.get("field", ""),
                    issue.get("detail", ""),
                ]
            )
        story.append(
            make_grid_table(
                issue_rows,
                styles,
                [28 * mm, 16 * mm, 42 * mm, 168 * mm],
                repeat_rows=1,
            )
        )
    else:
        story.append(Paragraph("未发现质量问题。", styles["body"]))
    story.append(Spacer(1, 4 * mm))

    columns = payload.get("columns", [])
    story.append(section_title("字段台账", styles))
    if columns:
        column_rows = [["字段", "类型", "非空", "缺失", "缺失率", "唯一值", "示例"]]
        for column in columns:
            column_rows.append(
                [
                    column.get("name", ""),
                    column.get("logical_type", ""),
                    str(column.get("non_null_count", "")),
                    str(column.get("missing_count", "")),
                    f'{float(column.get("missing_rate", 0.0)) * 100:.1f}%',
                    str(column.get("unique_count", "")),
                    column.get("sample_value", ""),
                ]
            )
        story.append(
            make_grid_table(
                column_rows,
                styles,
                [42 * mm, 22 * mm, 16 * mm, 16 * mm, 18 * mm, 18 * mm, 128 * mm],
                repeat_rows=1,
            )
        )
    else:
        story.append(Paragraph("当前数据集没有可展示字段。", styles["body"]))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    document = SimpleDocTemplate(
        str(output_path),
        pagesize=landscape(A4),
        leftMargin=10 * mm,
        rightMargin=10 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm,
        title="数据质量检查报告",
        author="data_processing",
    )
    document.build(story)
    return 0


def register_font() -> str:
    font_candidates = [
        Path(r"C:\Windows\Fonts\simhei.ttf"),
        Path(r"C:\Windows\Fonts\msyh.ttc"),
        Path(r"C:\Windows\Fonts\simsun.ttc"),
    ]
    for font_path in font_candidates:
        if font_path.exists():
            try:
                pdfmetrics.registerFont(TTFont("ReportFont", str(font_path)))
                return "ReportFont"
            except Exception:
                continue
    return "Helvetica"


def build_styles(font_name: str):
    base = getSampleStyleSheet()
    title = ParagraphStyle(
        "ReportTitle",
        parent=base["Title"],
        fontName=font_name,
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#16324f"),
        spaceAfter=4,
    )
    section = ParagraphStyle(
        "ReportSection",
        parent=base["Heading2"],
        fontName=font_name,
        fontSize=11,
        leading=14,
        textColor=colors.HexColor("#244e77"),
        spaceAfter=4,
    )
    body = ParagraphStyle(
        "ReportBody",
        parent=base["BodyText"],
        fontName=font_name,
        fontSize=8,
        leading=10,
        textColor=colors.HexColor("#202a34"),
    )
    strong = ParagraphStyle(
        "ReportStrong",
        parent=body,
        fontName=font_name,
        fontSize=8,
        leading=10,
        textColor=colors.HexColor("#0f2740"),
    )
    return {"title": title, "section": section, "body": body, "strong": strong}


def section_title(text: str, styles):
    return Paragraph(text, styles["section"])


def make_kv_table(rows, styles):
    data = []
    for key, value in rows:
        data.append([Paragraph(str(key), styles["strong"]), Paragraph(str(value), styles["body"])])
    table = Table(data, colWidths=[34 * mm, 220 * mm], repeatRows=0)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#eef3f8")),
                ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#1f2d3d")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#c9d6e2")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return table


def make_grid_table(rows, styles, widths, repeat_rows=1):
    table_rows = []
    for row_index, row in enumerate(rows):
        style = styles["strong"] if row_index == 0 else styles["body"]
        table_rows.append([Paragraph(escape_text(str(cell)), style) for cell in row])

    table = Table(table_rows, colWidths=widths, repeatRows=repeat_rows)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dde6ef")),
                ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#1f2d3d")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#c9d6e2")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
            ]
        )
    )
    return table


def escape_text(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br/>")
    )


def join_text(values, chunk_size: int | None = None) -> str:
    filtered = [escape_text(str(value).strip()) for value in values if str(value).strip()]
    if not filtered:
        return "-"

    if chunk_size and chunk_size > 0:
        groups = [
            "，".join(filtered[index : index + chunk_size])
            for index in range(0, len(filtered), chunk_size)
        ]
        return "<br/>".join(groups)

    return "，".join(filtered)


def blank_to_dash(value) -> str:
    text = str(value or "").strip()
    return text if text else "-"


if __name__ == "__main__":
    raise SystemExit(main())
