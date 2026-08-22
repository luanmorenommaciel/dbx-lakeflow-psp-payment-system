#!/usr/bin/env python3
"""Render docs/learner/brd-psp.md to a searchable PDF. Markdown remains canonical."""

from __future__ import annotations

import re
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    KeepTogether,
    ListFlowable,
    ListItem,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs/learner/brd-psp.md"
OUTPUT = ROOT / "docs/learner/brd-psp.pdf"

NAVY = colors.HexColor("#1b2430")
ACCENT = colors.HexColor("#c2410c")
RULE = colors.HexColor("#d6d3d1")
HEADER_BG = colors.HexColor("#1b2430")
ROW_ALT = colors.HexColor("#f5f5f4")


def markdown_inline(text: str) -> str:
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    text = re.sub(r"`([^`]+)`", r"<font face='Courier'><font color='#9a3412'>\1</font></font>", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"<u>\1</u>", text)
    return text


def parse_table(lines: list[str], start: int) -> tuple[list[list[str]], int]:
    rows: list[list[str]] = []
    index = start
    while index < len(lines) and lines[index].startswith("|"):
        raw = [cell.strip() for cell in lines[index].strip("|").split("|")]
        if not all(re.fullmatch(r":?-{3,}:?", cell.replace(" ", "")) for cell in raw):
            rows.append(raw)
        index += 1
    return rows, index


def build_styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "kicker": ParagraphStyle(
            "kicker",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=9,
            textColor=ACCENT,
            tracking=1.2,
            spaceAfter=6,
        ),
        "title": ParagraphStyle(
            "title",
            parent=base["Title"],
            fontName="Times-Bold",
            fontSize=22,
            leading=26,
            textColor=NAVY,
            alignment=TA_LEFT,
            spaceAfter=10,
        ),
        "h1": ParagraphStyle(
            "h1",
            parent=base["Heading1"],
            fontName="Times-Bold",
            fontSize=13,
            leading=16,
            textColor=NAVY,
            spaceBefore=14,
            spaceAfter=6,
            keepWithNext=True,
        ),
        "body": ParagraphStyle(
            "body",
            parent=base["BodyText"],
            fontName="Times-Roman",
            fontSize=10,
            leading=14,
            textColor=NAVY,
            alignment=TA_JUSTIFY,
            spaceAfter=8,
        ),
        "bullet": ParagraphStyle(
            "bullet",
            parent=base["BodyText"],
            fontName="Times-Roman",
            fontSize=10,
            leading=13,
            textColor=NAVY,
            leftIndent=8,
        ),
        "cell": ParagraphStyle(
            "cell",
            parent=base["BodyText"],
            fontName="Times-Roman",
            fontSize=8.5,
            leading=11,
            textColor=NAVY,
        ),
        "cell_header": ParagraphStyle(
            "cell_header",
            parent=base["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=8,
            leading=11,
            textColor=colors.white,
        ),
        "footer": ParagraphStyle(
            "footer",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=8,
            textColor=colors.HexColor("#78716c"),
        ),
    }


def table_flowable(rows: list[list[str]], styles: dict[str, ParagraphStyle]) -> Table:
    width = 7.0 * inch
    columns = len(rows[0])
    if columns == 2:
        col_widths = [2.2 * inch, 4.8 * inch]
    elif columns == 3:
        col_widths = [1.5 * inch, 2.4 * inch, 3.1 * inch]
    else:
        col_widths = [width / columns] * columns

    data = []
    for index, row in enumerate(rows):
        style = styles["cell_header"] if index == 0 else styles["cell"]
        data.append([Paragraph(markdown_inline(cell), style) for cell in row])

    grid = Table(data, colWidths=col_widths, repeatRows=1)
    commands = [
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.3, RULE),
    ]
    for index in range(1, len(rows)):
        if index % 2 == 0:
            commands.append(("BACKGROUND", (0, index), (-1, index), ROW_ALT))
    grid.setStyle(TableStyle(commands))
    return grid


def story_from_markdown(markdown: str, styles: dict[str, ParagraphStyle]) -> list:
    lines = markdown.splitlines()
    story: list = []
    index = 0
    first_heading = True
    while index < len(lines):
        line = lines[index].rstrip()
        if not line:
            index += 1
            continue
        if line.startswith("# "):
            if first_heading:
                story.append(Paragraph("PSP PAYMENT QUALITY  ·  WORKSHOP BRD", styles["kicker"]))
                first_heading = False
            story.append(Paragraph(markdown_inline(line[2:]), styles["title"]))
            index += 1
            continue
        if line.startswith("## "):
            story.append(Paragraph(markdown_inline(line[3:]), styles["h1"]))
            index += 1
            continue
        if line.startswith("|"):
            rows, index = parse_table(lines, index)
            story.append(KeepTogether([table_flowable(rows, styles), Spacer(1, 8)]))
            continue
        if re.match(r"[-*]\s+", line) or re.match(r"\d+\.\s+", line):
            items = []
            numbered = bool(re.match(r"\d+\.\s+", line))
            while index < len(lines) and (
                re.match(r"[-*]\s+", lines[index]) or re.match(r"\d+\.\s+", lines[index])
            ):
                item = re.sub(r"^([-*]|\d+\.)\s+", "", lines[index])
                items.append(ListItem(Paragraph(markdown_inline(item), styles["bullet"]), leftIndent=12))
                index += 1
            list_kwargs = {"leftIndent": 18, "spaceAfter": 8}
            if numbered:
                list_kwargs.update(bulletType="1", start="1")
            else:
                list_kwargs.update(bulletType="bullet")
            story.append(ListFlowable(items, **list_kwargs))
            continue
        story.append(Paragraph(markdown_inline(line), styles["body"]))
        index += 1
    return story


def add_page_chrome(canvas, doc) -> None:
    canvas.saveState()
    canvas.setFillColor(ACCENT)
    canvas.rect(0, letter[1] - 8, letter[0], 8, fill=1, stroke=0)
    canvas.setFillColor(NAVY)
    canvas.rect(0, 0, letter[0], 28, fill=1, stroke=0)
    canvas.setFillColor(colors.white)
    canvas.setFont("Helvetica", 8)
    canvas.drawString(0.75 * inch, 10, "brd-psp  ·  four-entity workshop  ·  m-007  25 → 45")
    canvas.drawRightString(letter[0] - 0.75 * inch, 10, f"{doc.page}")
    canvas.restoreState()


def main() -> None:
    styles = build_styles()
    markdown = SOURCE.read_text()
    document = SimpleDocTemplate(
        str(OUTPUT),
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.7 * inch,
        bottomMargin=0.55 * inch,
        title="Business requirements — merchant risk quality incident",
        author="DBX Agentic Development workshop",
    )
    document.build(story_from_markdown(markdown, styles), onFirstPage=add_page_chrome, onLaterPages=add_page_chrome)
    print(f"wrote {OUTPUT}")


if __name__ == "__main__":
    main()
