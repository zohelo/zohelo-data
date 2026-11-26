"""Barometr Zawodów posters: occupation lists by forecast category, read from a PDF page's text and drawing.

A poster ("Barometr zawodów <year>", an area: "Polska" or "województwo <name>") lists every occupation the experts
assessed for that area, in one coloured block per forecast category. The category is not printed beside the
occupations: each block has a tab of the same colour at its left edge with the category's name as rotated text
("DEFICYT", "RÓWNOWAGA", "NADWYŻKA"). The page's content and the Form XObjects it draws are walked once
(``drawing``): filled shapes with their colour and box, and every text show operation with its own start point
(text matrix tracked through Tm, Td, TD, T* and the graphics state), font and resources. Then:

- blocks: filled shapes at least half the page wide and 12 pt high that hold no other such shape (a page background
  holds the blocks); a block with neither a label nor a line of text in it is decoration and is left out;
- labels: the tab is a shape of the block's colour that starts left of the block, is narrower than half of it and
  overlaps it vertically; the label is the rotated text inside the tab. A block without such a tab takes the rotated
  text printed left of it within its height. Rotated text that no block takes is allowed only inside a tab whose
  colour is no block's colour (the tab of a category the poster does not use, drawn under a white box in 2022);
- lines: horizontal show operations of one printed line (same y) are split into columns at the column positions
  (an x where many lines start, at least COLUMN_GAP from another column) and each piece is decoded on its own by
  pypdf with the real gaps between its operations, so spaces come out as printed and two columns never join;
- occupations: the lines inside a block; a line that starts with a lower-case letter continues the line above it in
  the same column of the same block.

Anything else rejects the page (``ValueError`` with the reason): a block with two tabs or text but no label, rotated
text taken by two blocks, a line in the band of the blocks but in none of them, the same occupation twice, a
continuation without a line above it. Nothing is guessed from position alone.
"""

from __future__ import annotations

from collections import Counter
import re
from typing import Any
import unicodedata

TITLE = re.compile(r"^Barometr zawodów (20\d\d)$")
AREA = re.compile(r"^(Polska|województwo\s+\S.*)$")
FILL_OPS = {"f", "f*", "F", "B", "B*", "b", "b*"}
COLOUR_OPS = {"k", "rg", "g", "sc", "scn"}
SHOW_OPS = {"Tj", "TJ", "'", '"'}
LINE_TOLERANCE = 1.5  # pt: operations of one printed line differ in y by less than this
COLUMN_GAP = 50  # pt: occupation columns are 140-360 pt apart
BASE, GAP = 10000.0, 1000.0  # where each line piece is placed for decoding (one piece per y)


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
    """(filled shapes with box and colour, text show operations with start point, matrix, font and resources), from
    the page's content and the Form XObjects it draws (nested up to 5 deep)."""
    shapes: list[dict] = []
    texts: list[dict] = []
    _walk(page.get_contents(), page.get("/Resources"), reader, [1.0, 0, 0, 1.0, 0, 0], "g:0", shapes, texts, 0)
    return shapes, texts


def _walk(contents, resources, reader, cm, fill, shapes, texts, depth) -> None:
    from pypdf.generic import ContentStream

    if contents is None:
        return
    stack: list = []
    path: list[tuple[float, float]] = []
    font: list | None = None
    tlm = [1.0, 0, 0, 1.0, 0, 0]
    leading = 0.0
    for args, raw in ContentStream(contents, reader).operations:
        op = raw.decode("latin-1") if isinstance(raw, bytes) else str(raw)
        if op == "q":
            stack.append((cm, fill))
        elif op == "Q":
            cm, fill = stack.pop() if stack else (cm, fill)
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
        elif op == "Do" and depth < 5 and resources is not None:
            xobjects = resources.get("/XObject")
            xobject = xobjects.get_object().get(args[0]) if xobjects is not None else None
            xobject = xobject.get_object() if xobject is not None else None
            if xobject is not None and xobject.get("/Subtype") == "/Form":
                matrix = [float(v) for v in xobject.get("/Matrix", [1, 0, 0, 1, 0, 0])]
                inner = xobject.get("/Resources")
                inner = inner.get_object() if inner is not None else resources
                _walk(xobject, inner, reader, _mul(matrix, cm), fill, shapes, texts, depth + 1)
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
        elif op in SHOW_OPS and font:
            if op in ("'", '"'):
                tlm = _mul([1.0, 0, 0, 1.0, 0, -leading], tlm)
            m = _mul(tlm, cm)
            rotated = abs(m[0]) < 1e-6 and abs(m[1]) > 1e-6
            texts.append(
                {
                    "x": m[4],
                    "y": m[5],
                    "m": m,
                    "rotated": rotated,
                    "font": font,
                    "op": b"TJ" if op == "TJ" else b"Tj",
                    "args": [_raw(args[-1])],
                    "resources": resources,
                }
            )


def decode(page, reader, pieces: list[list[dict]], rotated: bool = False) -> list[str]:
    """The text of each piece (a list of show operations), decoded by pypdf with the fonts of the content stream the
    operations came from. Horizontal pieces keep their operations' gaps (spacing as printed) and are moved apart so
    that each piece is a line of its own; a rotated piece is decoded as it stands."""
    from pypdf import PageObject
    from pypdf.generic import ContentStream, FloatObject, NameObject

    out = [""] * len(pieces)
    by_resources: dict[int, list[int]] = {}
    for k, piece in enumerate(pieces):
        if piece:
            by_resources.setdefault(id(piece[0]["resources"]), []).append(k)
    for ks in by_resources.values():
        operations: list = [([], b"BT")]
        for k in ks:
            x0, y0 = pieces[k][0]["x"], pieces[k][0]["y"]
            for t in pieces[k]:
                m = list(t["m"])
                if not rotated:
                    m[4], m[5] = 100.0 + t["x"] - x0, BASE + k * GAP + t["y"] - y0
                operations += [(t["font"], b"Tf"), ([FloatObject(v) for v in m], b"Tm"), (t["args"], t["op"])]
        operations.append(([], b"ET"))
        stream = ContentStream(None, reader)
        stream.operations = operations
        tmp = PageObject.create_blank_page(width=float(page.mediabox.width), height=float(page.mediabox.height))
        resources = pieces[ks[0]][0]["resources"]
        if resources is not None:
            tmp[NameObject("/Resources")] = resources
        tmp.replace_contents(stream)
        if rotated:  # one rotated piece per call: its text is everything decoded
            for k in ks:
                single = ContentStream(None, reader)
                single.operations = [([], b"BT")]
                for t in pieces[k]:
                    single.operations += [
                        (t["font"], b"Tf"),
                        ([FloatObject(v) for v in t["m"]], b"Tm"),
                        (t["args"], t["op"]),
                    ]
                single.operations.append(([], b"ET"))
                tmp.replace_contents(single)
                out[k] = clean(tmp.extract_text(orientations=(90, 270)))
            continue
        found: dict[int, list[str]] = {}

        def visit(text, cm, tm, font, size, found=found):
            if text:
                k = round((float(tm[5]) * float(cm[3]) + float(cm[5]) - BASE) / GAP)
                found.setdefault(k, []).append(text)

        tmp.extract_text(visitor_text=visit, orientations=(0,))
        for k in ks:
            out[k] = clean("".join(found.get(k, [])))
    return out


def lines_of(page, reader, texts: list[dict], split: bool = True) -> list[dict]:
    """Printed lines, {x, y, text}; with ``split`` cut into columns at every operation that starts on a column."""
    flat = sorted((t for t in texts if not t["rotated"]), key=lambda t: (-t["y"], t["x"]))
    rows: list[list[dict]] = []
    for t in flat:
        if rows and abs(rows[-1][0]["y"] - t["y"]) < LINE_TOLERANCE:
            rows[-1].append(t)
        else:
            rows.append([t])
    starts = Counter(round(row_sorted[0]["x"]) for row in rows for row_sorted in _runs(row))
    floor = max(3, max(starts.values(), default=0) // 5)
    columns: list[float] = []
    for x, n in sorted(starts.items(), key=lambda xn: (-xn[1], xn[0])):
        if n >= floor and all(abs(x - c) >= COLUMN_GAP for c in columns):
            columns.append(x)
    pieces: list[list[dict]] = []
    where: list[tuple[float, float]] = []
    for row in rows:
        row.sort(key=lambda t: t["x"])
        current: list[dict] = []
        for t in row:
            if split and current and any(abs(t["x"] - c) <= 2 and current[-1]["x"] < c - 2 for c in columns):
                pieces.append(current)
                where.append((current[0]["x"], row[0]["y"]))
                current = []
            current.append(t)
        pieces.append(current)
        where.append((current[0]["x"], row[0]["y"]))
    texts_out = decode(page, reader, pieces)
    return [{"x": x, "y": y, "text": text} for (x, y), text in zip(where, texts_out, strict=True) if text]


def _runs(row: list[dict]) -> list[list[dict]]:
    """A row's operations in x order, cut where the gap to the next start is at least COLUMN_GAP (column starts)."""
    ordered = sorted(row, key=lambda t: t["x"])
    runs: list[list[dict]] = [[ordered[0]]]
    for t in ordered[1:]:
        if t["x"] - runs[-1][-1]["x"] >= COLUMN_GAP:
            runs.append([t])
        else:
            runs[-1].append(t)
    return runs


def _inside(shape: dict, x: float, y: float, pad: float = 0.5) -> bool:
    return shape["x0"] - pad <= x <= shape["x1"] + pad and shape["y0"] - pad <= y <= shape["y1"] + pad


def _contains(outer: dict, inner: dict) -> bool:
    return (
        outer["x0"] <= inner["x0"] + 0.5
        and inner["x1"] <= outer["x1"] + 0.5
        and outer["y0"] <= inner["y0"] + 0.5
        and inner["y1"] <= outer["y1"] + 0.5
    )


def _tabs(block: dict, shapes: list[dict]) -> list[dict]:
    return [
        t
        for t in shapes
        if t is not block
        and t["colour"] == block["colour"]
        and t["x0"] < block["x0"] - 1
        and t["x1"] - t["x0"] < (block["x1"] - block["x0"]) / 2
        and t["y0"] < block["y1"]
        and t["y1"] > block["y0"]
    ]


def _name_blocks(page, reader, blocks, shapes, glyphs, lines) -> list[tuple[dict, str]]:
    """Each block's label (see the module text); blocks without label and text are left out."""
    named: list[tuple[dict, str]] = []
    taken: list[int] = []
    for b in blocks:
        tabs = _tabs(b, shapes)
        if len(tabs) > 1:
            raise ValueError(f"block {b['colour']} at y {b['y0']:.0f}-{b['y1']:.0f} has {len(tabs)} tabs")
        if tabs:
            held = [i for i, g in enumerate(glyphs) if _inside(tabs[0], g["x"], g["y"], pad=2)]
        else:
            held = [
                i for i, g in enumerate(glyphs) if g["x"] < b["x0"] + 2 and b["y0"] - 0.5 <= g["y"] <= b["y1"] + 0.5
            ]
        label = decode(page, reader, [[glyphs[i] for i in held]], rotated=True)[0] if held else ""
        if not label:
            if not any(_inside(b, line["x"], line["y"]) for line in lines):
                continue  # decoration: no label, no text
            raise ValueError(f"block {b['colour']} at y {b['y0']:.0f}-{b['y1']:.0f} has text but no label")
        taken += held
        named.append((b, label))
    colours = {b["colour"] for b, _ in named}
    unused_tabs = [s for s in shapes if s["colour"] not in colours and s["x1"] - s["x0"] < 200]
    for i, g in enumerate(glyphs):
        if taken.count(i) > 1:
            raise ValueError("rotated text taken by two category blocks")
        if i not in taken and not any(_inside(t, g["x"], g["y"], pad=2) for t in unused_tabs):
            raise ValueError(f"rotated text at x {g['x']:.0f}, y {g['y']:.0f} lies in no category tab")
    return named


def parse(page, reader) -> dict[str, Any] | None:
    """The poster on one page: {year, area, categories: [label], rows: [(label, occupation)]}, or None when the page
    is no poster (no "Barometr zawodów <year>" title together with an area line). ValueError on a poster page that
    does not read cleanly."""
    shapes, texts = drawing(page, reader)
    whole = lines_of(page, reader, texts, split=False)  # the header lines, never cut at a column
    titles = [m.group(1) for m in (TITLE.match(line["text"]) for line in whole) if m]
    areas = [line for line in whole if AREA.match(line["text"])]
    if not titles or not areas:
        return None
    if len(set(titles)) != 1 or len({a["text"] for a in areas}) != 1:
        raise ValueError(f"poster page with titles {sorted(set(titles))} and areas {[a['text'] for a in areas]}")
    lines = lines_of(page, reader, texts)
    width, height = float(page.mediabox.width), float(page.mediabox.height)
    wide = [s for s in shapes if s["x1"] - s["x0"] >= width / 2 and 12 <= s["y1"] - s["y0"] < 0.95 * height]
    blocks = [b for b in wide if not any(o is not b and _contains(b, o) for o in wide)]
    glyphs = [t for t in texts if t["rotated"]]
    if not blocks:
        raise ValueError("poster page without a coloured category block")
    if not glyphs:
        raise ValueError("poster page without a rotated category label")
    named = _name_blocks(page, reader, blocks, shapes, glyphs, lines)
    if not named:
        raise ValueError("poster page without a labelled category block")
    rows: list[tuple[str, str]] = []
    last_in_column: dict[tuple[int, float], int] = {}
    top = max(b["y1"] for b, _ in named)
    bottom = min(b["y0"] for b, _ in named)
    for line in lines:
        if any(abs(line["y"] - h["y"]) < LINE_TOLERANCE for h in areas) or TITLE.match(line["text"]):
            continue
        holders = [(b, name) for b, name in named if _inside(b, line["x"], line["y"])]
        if not holders:
            if bottom < line["y"] < top:  # in the band of the blocks but in none: never dropped silently
                raise ValueError(f"line in no category block: {line['text'][:60]!r}")
            continue  # header, footer and logo text above or below the blocks
        if len(holders) != 1:
            raise ValueError(f"line {line['text'][:60]!r} lies in {len(holders)} category blocks")
        block, name = holders[0]
        key = (id(block), round(line["x"]))
        if line["text"][0].islower():
            if key not in last_in_column:
                raise ValueError(f"continuation line without a line above it: {line['text'][:60]!r}")
            i = last_in_column[key]
            rows[i] = (rows[i][0], f"{rows[i][1]} {line['text']}")
            continue
        last_in_column[key] = len(rows)
        rows.append((name, line["text"]))
    seen: set[str] = set()
    for _, occupation in rows:
        if occupation in seen:
            raise ValueError(f"occupation printed twice on one poster: {occupation[:60]!r}")
        seen.add(occupation)
    if not rows:
        raise ValueError("poster page without an occupation inside a category block")
    return {
        "year": titles[0],
        "area": areas[0]["text"],
        "categories": [name for _, name in sorted(named, key=lambda bn: -bn[0]["y1"])],
        "rows": rows,
    }
