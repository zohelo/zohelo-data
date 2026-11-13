"""Positioned text of a PDF page for the internet.gov.pl report tables (register PL-ENV-034).

pypdf's layout mode joins text drawn within one font height into one line, which breaks a table row whose numbers
are set halfway between the two lines of a wrapped label (the 2024 editions, made with cairo, print
"warmińsko-" / "mazurskie" that way and the leading digit of the first number lands on the label's line). This
module keeps pypdf's own text decoding and coordinates per text show operation (the same functions layout mode
uses, pypdf pinned in requirements.txt) and builds lines and cells itself:

- a piece is one shown string with its start x, end x, baseline (top to bottom) and font height;
- pieces whose baselines differ by at most a quarter of the font height form one line;
- within a line, pieces closer than half the font height form one cell (glyph runs of one word, digit groups
  of one number set with or without a thin space), a wider gap starts a new cell (table columns).

Pure functions apart from reading the page; no value is interpreted here.
"""

from __future__ import annotations

from typing import Any

SAME_LINE = 0.25  # of the font height: baselines closer than this are one line
SAME_CELL = 0.5  # of the font height: horizontal gaps narrower than this stay in one cell
SPACE = 0.15  # of the font height: a gap wider than this inside a cell is written as one space


def pieces(page: Any) -> list[tuple[float, float, float, float, str]]:
    """(x start, x end, baseline from the top, font height, text) of every shown string on the page."""
    from pypdf._text_extraction._layout_mode._fixed_width_page import recurse_to_target_op, set_font_operation
    from pypdf._text_extraction._layout_mode._text_state_manager import TextStateManager
    from pypdf.generic import ContentStream

    contents = page.get("/Contents")
    if contents is None:
        return []
    fonts = page._layout_mode_fonts()
    ops = iter(ContentStream(contents.get_object(), page.pdf, "bytes").operations)
    state, shown = TextStateManager(), []
    for operands, op in ops:
        if op in (b"BT", b"q"):
            shown += recurse_to_target_op(ops, state, b"ET" if op == b"BT" else b"Q", fonts, True)[1]
        elif op == b"Tf":
            set_font_operation(state, fonts, operands)
        else:
            state.set_state_param(op, operands)
    out = []
    for tj in shown:
        if tj.rotated or not tj.font.interpretable or not tj.text.strip():
            continue
        top_down = tj.ty if tj.flip_vertical else -tj.ty
        out.append((round(tj.tx, 2), round(tj.displaced_tx, 2), round(top_down, 2), round(tj.font_height, 2), tj.text))
    return out


def lines(found: list[tuple[float, float, float, float, str]]) -> list[list[str]]:
    """The page's lines, top to bottom, each a list of cells left to right (whitespace normalised)."""
    rows: list[list[tuple[float, float, float, float, str]]] = []
    for piece in sorted(found, key=lambda p: (p[2], p[0])):
        if rows and abs(piece[2] - rows[-1][0][2]) <= SAME_LINE * max(piece[3], rows[-1][0][3]):
            rows[-1].append(piece)
        else:
            rows.append([piece])
    out = []
    for row in rows:
        cells: list[str] = []
        end = None
        for x0, x1, _, height, text in sorted(row, key=lambda p: p[0]):
            gap = None if end is None else x0 - end
            if gap is None or gap >= SAME_CELL * height:
                cells.append(text)
            else:
                cells[-1] += (" " if gap > SPACE * height else "") + text
            end = x1 if end is None or gap is None or gap >= SAME_CELL * height else max(end, x1)
        cells = [" ".join(c.replace(" ", " ").split()) for c in cells]
        out.append([c for c in cells if c])
    return [r for r in out if r]
