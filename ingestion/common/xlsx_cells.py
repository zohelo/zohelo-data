"""Cells of an XLSX workbook, read with the standard library (no spreadsheet dependency).

For small official workbooks (Ministry of Finance, KRUS) whose layout is a report, not a table: Bronze keeps
every non-empty cell as source text with its sheet, row, column and number format, and Silver picks the
cells it models. ``cells(body)`` yields one dict per non-empty cell, in sheet order and row-major order.

Values: shared and inline strings as text; numbers as the stored text (``"1234.5"``, no rounding);
booleans ``"0"``/``"1"``; errors as stored (``"#N/A"``); formula cells as their cached value. Dates are
numbers in Excel's serial form: ``number_format`` (the cell style's format code, e.g. ``"yyyy-mm-dd"``)
tells Silver which numbers are dates. Merged areas keep the value in their top-left cell only, as stored.
"""

from __future__ import annotations

from collections.abc import Iterator
import io
import posixpath
import re
from typing import Any
import xml.etree.ElementTree as ET
import zipfile

MAIN = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
PKG_REL = "{http://schemas.openxmlformats.org/package/2006/relationships}"
# Built-in number formats (ECMA-376 18.8.30) that a cell may use without a <numFmt> element.
BUILTIN_FORMATS = {
    0: "General", 1: "0", 2: "0.00", 3: "#,##0", 4: "#,##0.00", 9: "0%", 10: "0.00%", 11: "0.00E+00",
    12: "# ?/?", 13: "# ??/??", 14: "mm-dd-yy", 15: "d-mmm-yy", 16: "d-mmm", 17: "mmm-yy", 18: "h:mm AM/PM",
    19: "h:mm:ss AM/PM", 20: "h:mm", 21: "h:mm:ss", 22: "m/d/yy h:mm", 37: "#,##0 ;(#,##0)",
    38: "#,##0 ;[Red](#,##0)", 39: "#,##0.00;(#,##0.00)", 40: "#,##0.00;[Red](#,##0.00)", 45: "mm:ss",
    46: "[h]:mm:ss", 47: "mmss.0", 48: "##0.0E+0", 49: "@",
}
REF = re.compile(r"^([A-Z]{1,3})([0-9]+)$")
MAX_CELLS = 2_000_000  # a guard against a mislinked large file; the declared workbooks hold a few thousand


def column_number(letters: str) -> int:
    n = 0
    for ch in letters:
        n = n * 26 + ord(ch) - 64
    return n


def _text(node: ET.Element) -> str:
    """Text of a <si> or <is> element: plain <t>, or the runs' <t> joined (phonetic runs skipped)."""
    parts = [t.text or "" for t in node.findall(MAIN + "t")]
    parts += [t.text or "" for r in node.findall(MAIN + "r") for t in r.findall(MAIN + "t")]
    return "".join(parts)


def _shared_strings(z: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in z.namelist():
        return []
    return [_text(si) for si in ET.fromstring(z.read("xl/sharedStrings.xml")).iter(MAIN + "si")]


def _formats(z: zipfile.ZipFile) -> list[str]:
    """Number format code per cell style index (cellXfs order)."""
    if "xl/styles.xml" not in z.namelist():
        return []
    root = ET.fromstring(z.read("xl/styles.xml"))
    custom = {int(f.attrib["numFmtId"]): f.attrib.get("formatCode", "") for f in root.iter(MAIN + "numFmt")}
    xfs = root.find(MAIN + "cellXfs")
    if xfs is None:
        return []
    out = []
    for xf in xfs.findall(MAIN + "xf"):
        fid = int(xf.attrib.get("numFmtId", "0"))
        out.append(custom.get(fid, BUILTIN_FORMATS.get(fid, f"builtin:{fid}")))
    return out


def sheets(z: zipfile.ZipFile) -> list[tuple[str, str]]:
    """(sheet name, part path) in workbook order."""
    workbook = ET.fromstring(z.read("xl/workbook.xml"))
    rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
    target = {r.attrib["Id"]: r.attrib["Target"] for r in rels.iter(PKG_REL + "Relationship")}
    out = []
    for sheet in workbook.iter(MAIN + "sheet"):
        t = target[sheet.attrib[REL + "id"]]
        path = t.lstrip("/") if t.startswith("/") else posixpath.normpath(posixpath.join("xl", t))
        out.append((sheet.attrib["name"], path))
    return out


def cells(body: bytes) -> Iterator[dict[str, Any]]:
    """Every non-empty cell of every worksheet: sheet_index (1-based), sheet_name, row_number, column_number,
    cell_ref, cell_type (s, n, str, b, e, inlineStr, d), value_text, number_format."""
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        strings, formats, count = _shared_strings(z), _formats(z), 0
        for index, (name, part) in enumerate(sheets(z), start=1):
            if part not in z.namelist():
                continue  # a chart sheet or a dangling relationship
            for _event, c in ET.iterparse(z.open(part)):
                if c.tag != MAIN + "c":
                    continue
                kind = c.attrib.get("t", "n")
                v = c.find(MAIN + "v")
                if kind == "inlineStr":
                    node = c.find(MAIN + "is")
                    value = _text(node) if node is not None else None
                elif v is None or v.text is None:
                    value = None
                elif kind == "s":
                    value = strings[int(v.text)]
                else:
                    value = v.text
                if value is None or value == "":
                    c.clear()
                    continue
                m = REF.match(c.attrib.get("r", ""))
                if not m:
                    raise ValueError(f"cell without a reference on sheet {name!r}")
                style = int(c.attrib.get("s", "0"))
                count += 1
                if count > MAX_CELLS:
                    raise ValueError(f"more than {MAX_CELLS} cells: not a declared workbook")
                yield {
                    "sheet_index": index,
                    "sheet_name": name,
                    "row_number": int(m.group(2)),
                    "column_number": column_number(m.group(1)),
                    "cell_ref": m.group(0),
                    "cell_type": kind,
                    "value_text": value,
                    "number_format": formats[style] if style < len(formats) else None,
                }
                c.clear()
