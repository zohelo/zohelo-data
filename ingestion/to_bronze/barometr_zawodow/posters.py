"""Barometr Zawodów posters: occupation lists by forecast category, read from a PDF page's text and drawing.

A poster page ("Barometr zawodów <year>", an area: "Polska" or "województwo <name>") lists every occupation the
experts assessed for that area, in one coloured block per forecast category. The category is not printed beside
the occupations: each block has a tab of the same colour at its left edge with the category's name as rotated text
(e.g. "DEFICYT", "RÓWNOWAGA", "NADWYŻKA"). A page is read as:

- blocks: filled shapes at least half the page wide and taller than one text line, by colour;
- tabs: filled shapes narrower than 70 pt and taller than 25 pt whose colour is the colour of exactly one block;
- labels: the rotated text show operations, each placed by its own start point (text matrix tracked through Tm,
  Td, TD and T*) in the tab that contains it, then decoded per tab with the page's fonts (pypdf's own extraction
  joins the labels of all tabs into one string); a block must have exactly one tab, every tab a label, and every
  rotated piece of text must lie in a tab;
- occupations: the horizontal text lines inside a block, in columns (x positions shared by at least two lines of
  the page); a line that starts with a lower-case letter continues the line above it in the same column.

Anything else rejects the page (``ValueError`` with the reason): a block without a tab or a label, a label outside
a tab, an occupation line outside every block, the same occupation twice on one page, a continuation without a
line above it. Nothing is guessed from position alone.
"""

from __future__ import annotations

import re
from typing import Any
import unicodedata

TITLE = re.compile(r"^Barometr zawodów (20\d\d)$")
AREA = re.compile(r"^(Polska|województwo\s+\S.*)$")
FILL_OPS = {"f", "f*", "F", "B", "B*", "b", "b*"}
COLOUR_OPS = {"k": 4, "rg": 3, "g": 1, "sc": None, "scn": None}
TEXT_OPS = {"Tj", "TJ", "'", '"', "Td", "TD", "T*", "Tc", "Tw", "Tz", "TL", "Ts", "Tr", "Tf"}
LINE_TOLERANCE = 1.5  # pt: chunks of one printed line differ in y by less than this


def clean(text: str) -> str:
    """NFKC (the ligatures "ﬁ", "ﬂ" of the posters' fonts become "fi", "fl"), one space between words."""
    return " ".join(unicodedata.normalize("NFKC", text).split())


def _mul(m: list[float], n: list[float]) -> list[float]:
    """Matrix product m × n of two PDF matrices [a b c d e f]."""
    a, b, c, d, e, f = m
    a2, b2, c2, d2, e2, f2 = n
    return [
        a * a2 + b * c2,
        a * b2 + b * d2,
        c * a2 + d * c2,
        c * b2 + d * d2,
        e * a2 + f * c2 + e2,
        e * b2 + f * d2 + f2,
    ]


def _pt(m: list[float], x: float, y: float) -> tuple[float, float]:
    return (m[0] * x + m[2] * y + m[4], m[1] * x + m[3] * y + m[5])


def _colour(op: str, args: list) -> str:
    nums = [round(float(a), 3) for a in args if isinstance(a, (int, float)) or hasattr(a, "__float__")]
    return f"{op}:{','.join(str(n) for n in nums)}"


def _raw(value):
    """An operand with its strings as the original bytes (a re-encoded TextStringObject breaks custom glyph codes)."""
    from pypdf.generic import ArrayObject, ByteStringObject, TextStringObject

    if isinstance(value, TextStringObject):
        return ByteStringObject(value.original_bytes)
    if isinstance(value, ArrayObject):
        return ArrayObject(_raw(v) for v in value)
    return value


def drawing(page, reader) -> tuple[list[dict], list[dict]]:
    """(filled shapes with bbox and colour, rotated text show operations with their start point and font)."""
    from pypdf.generic import ContentStream

    ops = ContentStream(page.get_contents(), reader).operations
    cm, stack, fill = [1.0, 0, 0, 1.0, 0, 0], [], "g:0"
    path: list[tuple[float, float]] = []
    shapes: list[dict] = []
    glyphs: list[dict] = []
    font: list | None = None
    tlm = [1.0, 0, 0, 1.0, 0, 0]
    leading = 0.0
    for args, raw in ops:
        op = raw.decode("latin-1") if isinstance(raw, bytes) else str(raw)
        if op == "q":
            stack.append(cm)
        elif op == "Q":
            cm = stack.pop() if stack else cm
        elif op == "cm":
            cm = _mul([float(a) for a in args], cm)
        elif op in COLOUR_OPS:
            fill = _colour(op, args)
        elif op == "re":
            x, y, w, h = (float(a) for a in args)
            path += [_pt(cm, x, y), _pt(cm, x + w, y + h)]
        elif op in ("m", "l"):
            path.append(_pt(cm, float(args[0]), float(args[1])))
        elif op in ("c", "v", "y"):
            path.append(_pt(cm, float(args[-2]), float(args[-1])))
        elif op in FILL_OPS:
            if path:
                xs, ys = [p[0] for p in path], [p[1] for p in path]
                shapes.append({"x0": min(xs), "x1": max(xs), "y0": min(ys), "y1": max(ys), "colour": fill})
            path = []
        elif op in ("n", "S", "s"):
            path = []
        elif op == "BT":
            tlm = [1.0, 0, 0, 1.0, 0, 0]
        elif op == "Tf":
            font = list(args)
        elif op == "TL":
            leading = float(args[0])
        elif op == "Tm":
            tlm = [float(a) for a in args]
        elif op in ("Td", "TD"):
            tlm = _mul([1.0, 0, 0, 1.0, float(args[0]), float(args[1])], tlm)
            if op == "TD":
                leading = -float(args[1])
        elif op == "T*":
            tlm = _mul([1.0, 0, 0, 1.0, 0, -leading], tlm)
        elif op in ("Tj", "TJ", "'", '"'):
            if op in ("'", '"'):
                tlm = _mul([1.0, 0, 0, 1.0, 0, -leading], tlm)
            m = _mul(tlm, cm)
            if abs(m[0]) < 1e-6 and abs(m[1]) > 1e-6 and font:  # rotated by 90 or 270 degrees: a tab label
                show = [_raw(args[-1])]
                glyphs.append(
                    {
                        "x": m[4],
                        "y": m[5],
                        "tm": list(tlm),
                        "font": font,
                        "op": b"TJ" if op == "TJ" else b"Tj",
                        "args": show,
                    }
                )
    return shapes, glyphs


def decode_glyphs(page, reader, glyphs: list[dict]) -> str:
    """Text of rotated show operations, decoded together (in content order) with the page's fonts."""
    from pypdf import PageObject
    from pypdf.generic import ContentStream, FloatObject, NameObject

    stream = ContentStream(None, reader)
    operations: list = [([], b"BT")]
    for g in glyphs:
        operations.append((g["font"], b"Tf"))
        operations.append(([FloatObject(v) for v in g["tm"]], b"Tm"))
        operations.append((g["args"], g["op"]))
    operations.append(([], b"ET"))
    stream.operations = operations
    tmp = PageObject.create_blank_page(width=float(page.mediabox.width), height=float(page.mediabox.height))
    tmp[NameObject("/Resources")] = page["/Resources"]
    tmp.replace_contents(stream)
    return clean(tmp.extract_text(orientations=(90, 270)))


def text_chunks(page) -> list[dict]:
    """Horizontal text chunks with their start point (pypdf's visitor)."""
    out: list[dict] = []

    def visit(text, cm, tm, font, size):
        if not text.strip() or abs(tm[1]) > 1e-6 or abs(tm[2]) > 1e-6:
            return
        m = _mul([float(v) for v in tm], [float(v) for v in cm])
        for n, part in enumerate(text.split("\n")):
            if part.strip():
                out.append({"x": m[4], "y": m[5] - n * abs(m[3]) * 1.2 if n else m[5], "text": part})

    page.extract_text(visitor_text=visit, orientations=(0,))
    return out


def lines_of(chunks: list[dict]) -> list[dict]:
    """Chunks joined into printed lines: same y (within LINE_TOLERANCE), left to right, then split into columns
    where a chunk starts at a column x (an x where at least two lines start)."""
    rows: list[list[dict]] = []
    for c in sorted(chunks, key=lambda c: (-c["y"], c["x"])):
        if rows and abs(rows[-1][0]["y"] - c["y"]) < LINE_TOLERANCE:
            rows[-1].append(c)
        else:
            rows.append([c])
    starts: dict[float, int] = {}
    for row in rows:
        for c in row:
            starts[round(c["x"], 0)] = starts.get(round(c["x"], 0), 0) + 1
    columns = {x for x, n in starts.items() if n >= 2}
    lines: list[dict] = []
    for row in rows:
        row.sort(key=lambda c: c["x"])
        for c in row:
            if round(c["x"], 0) in columns or not lines or lines[-1]["y"] != row[0]["y"]:
                lines.append({"x": c["x"], "y": row[0]["y"], "text": c["text"]})
            else:
                lines[-1]["text"] += c["text"]
    for line in lines:
        line["text"] = clean(line["text"])
    return [line for line in lines if line["text"]]


def _inside(shape: dict, x: float, y: float, pad: float = 0.5) -> bool:
    return shape["x0"] - pad <= x <= shape["x1"] + pad and shape["y0"] - pad <= y <= shape["y1"] + pad


def parse(page, reader) -> dict[str, Any] | None:
    """The poster on one page: {year, area, title, categories: [label], rows: [(label, occupation)]}, or None when
    the page is no poster (no "Barometr zawodów <year>" title together with an area line). ValueError on a poster
    page that does not read cleanly."""
    lines = lines_of(text_chunks(page))
    titles = [m.group(1) for m in (TITLE.match(line["text"]) for line in lines) if m]
    areas = [line for line in lines if AREA.match(line["text"])]
    if not titles or not areas:
        return None
    if len(set(titles)) != 1 or len({a["text"] for a in areas}) != 1:
        raise ValueError(f"poster page with titles {sorted(set(titles))} and areas {[a['text'] for a in areas]}")
    width = float(page.mediabox.width)
    shapes, glyphs = drawing(page, reader)
    blocks = [
        s
        for s in shapes
        if s["x1"] - s["x0"] >= width / 2 and 12 <= s["y1"] - s["y0"] < 0.9 * float(page.mediabox.height)
    ]
    colours = {}
    for b in blocks:
        colours.setdefault(b["colour"], []).append(b)
    tabs = [s for s in shapes if s["x1"] - s["x0"] < 70 and s["y1"] - s["y0"] > 25 and s["colour"] in colours]
    if not blocks:
        raise ValueError("poster page without a coloured category block")
    if not glyphs:
        raise ValueError("poster page without a rotated category label")
    # each block: the one tab of its colour beside it (vertical overlap) and the rotated text inside that tab
    named: list[tuple[dict, str]] = []
    placed = 0
    for b in blocks:
        beside = [t for t in tabs if t["colour"] == b["colour"] and t["y0"] < b["y1"] and t["y1"] > b["y0"]]
        if len(beside) != 1:
            raise ValueError(f"block {b['colour']} at y {b['y0']:.0f}-{b['y1']:.0f} has {len(beside)} tabs")
        held = [g for g in glyphs if _inside(beside[0], g["x"], g["y"], pad=2)]
        label = decode_glyphs(page, reader, held) if held else ""
        if not label:
            raise ValueError(f"tab of block {b['colour']} at y {b['y0']:.0f} holds no label")
        placed += len(held)
        named.append((b, label))
    if placed != len(glyphs):
        raise ValueError(f"{len(glyphs) - placed} rotated text pieces lie outside the category tabs")
    rows: list[tuple[str, str]] = []
    seen: set[str] = set()
    last_in_column: dict[tuple[int, float], int] = {}
    for line in lines:
        if line in areas or TITLE.match(line["text"]):
            continue
        holders = [(b, name) for b, name in named if _inside(b, line["x"], line["y"])]
        if not holders:
            continue  # header, footer and logo text outside the blocks
        if len(holders) != 1:
            raise ValueError(f"line {line['text'][:60]!r} lies in {len(holders)} category blocks")
        block, name = holders[0]
        key = (id(block), round(line["x"], 0))
        first = line["text"][0]
        if first.islower():
            if key not in last_in_column:
                raise ValueError(f"continuation line without a line above it: {line['text'][:60]!r}")
            i = last_in_column[key]
            rows[i] = (rows[i][0], f"{rows[i][1]} {line['text']}")
            continue
        last_in_column[key] = len(rows)
        rows.append((name, line["text"]))
    for _, occupation in rows:
        if occupation in seen:
            raise ValueError(f"occupation printed twice on one poster: {occupation[:60]!r}")
        seen.add(occupation)
    if not rows:
        raise ValueError("poster page without an occupation inside a category block")
    empty = sorted({name for _, name in named} - {name for name, _ in rows})
    return {
        "year": titles[0],
        "area": areas[0]["text"],
        "categories": [name for _, name in sorted(named, key=lambda bn: -bn[0]["y1"])],
        "empty_categories": empty,
        "rows": rows,
    }
