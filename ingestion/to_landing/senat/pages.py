"""Strict parsers of the www.senat.gov.pl pages read for the Senate source (register PLG-025, decision D36).

Used by Landing (which sitting days and votes exist) and by Bronze (rows). Every parser checks the page layout and
raises ValueError with the reason when it differs from the one seen on 12 Nov 2026 (Dev probe 43326678967): a
changed page is rejected, never guessed. Text is returned as published (tags removed, entities decoded, blanks
collapsed); nothing is cast here.

Pages:
- sittings list (/prace/posiedzenia/, page,<n>.html): one block per sitting of term XI, newest first: internal
  sitting id, "<n>. posiedzenie Senatu RP XI kadencji", the dates as printed, the status label and the day numbers of
  its votes pages (przebieg,<id>,<day>,glosowania.html); a planned sitting has no days yet.
- sitting-day votes page (przebieg,<id>,<day>,glosowania.html): heading "<n>. posiedzenie …, <day> dzień", the
  sitting's dates, and one row per vote: number (per sitting), subject, subtitle, vote id (glosowanie-drukuj,<id>).
  The list is printed twice (desktop and mobile); both must name the same votes.
- vote print page (glosowanie-drukuj,<id>.html): subject, subtitle, the totals głosowało / za / przeciw / wstrzymało
  się, and how many senators carry each mark (za, przec., …). The senators' names are not returned.
- senators list (/sklad/senatorowie/): id, term, URL slug, name and the annotation (e.g. "mandat wygasł <date>").
"""

from __future__ import annotations

from collections import Counter
from html import unescape
import re

TERM = "11"
SITTING_LABEL = re.compile(r"^(\d+)\. posiedzenie Senatu RP XI kadencji$")
_TAG = re.compile(r"<[^>]+>")
_COMMENT = re.compile(r"<!--.*?-->", re.S)


def text(fragment: str) -> str:
    """Visible text of an HTML fragment: tags removed, entities decoded, blanks collapsed."""
    return " ".join(unescape(_TAG.sub(" ", fragment)).split())


def decoded(body: bytes) -> str:
    """The page as text (UTF-8, as the server declares); HTML comments removed (the day page keeps an old menu in
    one)."""
    return _COMMENT.sub("", body.decode("utf-8"))


def _one(pattern: str, page: str, what: str, flags: int = re.S) -> re.Match:
    found = list(re.finditer(pattern, page, flags))
    if len(found) != 1:
        raise ValueError(f"layout: {what} found {len(found)} times, expected once")
    return found[0]


# --- sittings list ------------------------------------------------------------------------------------------------

_SITTING_BLOCK = '<div class="meeting  container-posiedzenia">'


def sittings(body: bytes) -> dict:
    """{'last_page': int, 'sittings': [{sitting_id, sitting_label, sitting_number, date_text, status_text,
    day_numbers}]} of one list page."""
    page = decoded(body)
    if "Lista posiedzeń" not in page:
        raise ValueError("layout: no 'Lista posiedzeń' heading")
    pages = {int(n) for n in re.findall(r'href="/prace/posiedzenia/page,(\d+)\.html"', page)}
    blocks = page.split(_SITTING_BLOCK)[1:]
    if not blocks:
        raise ValueError("layout: no sitting block")
    out = []
    for block in blocks:
        block = block.split("<footer", 1)[0]
        head = _one(
            r'<h3 class="meeting-headline">\s*<a href="/prace/posiedzenia/tematy,(\d+),1\.html" '
            r'class="headline-anchor">(.*?)</a>',
            block,
            "sitting headline",
        )
        label = text(head.group(2))
        number = SITTING_LABEL.match(label)
        if not number:
            raise ValueError(f"layout: sitting label {label[:80]!r} is not a term XI sitting")
        dates = _one(r'<div class="date-container">\s*<span[^>]*>(.*?)</span>', block, "sitting dates")
        status = _one(r'<div class="labels">(.*?)</div>', block, "status labels")
        days = sorted({int(d) for d in re.findall(rf"przebieg,{head.group(1)},(\d+),glosowania\.html", block)})
        foreign = set(re.findall(r"przebieg,(\d+),\d+", block)) - {head.group(1)}
        if foreign:
            raise ValueError(f"layout: sitting {head.group(1)} block links other sittings {sorted(foreign)}")
        out.append(
            {
                "sitting_id": head.group(1),
                "sitting_label": label,
                "sitting_number": number.group(1),
                "date_text": text(dates.group(1)),
                "status_text": text(status.group(1)),
                "day_numbers": "|".join(str(d) for d in days),
            }
        )
    ids = Counter(s["sitting_id"] for s in out)
    if max(ids.values()) > 1:
        raise ValueError(f"layout: a sitting is listed twice on one page: {[k for k, v in ids.items() if v > 1]}")
    return {"last_page": max(pages) if pages else 1, "sittings": out}


# --- sitting-day votes page ---------------------------------------------------------------------------------------

_DESKTOP = '<div class="wyszukiwarka-drukow-kontener modul-przebiegow desktop">'
_MOBILE = '<div class="mobile">'
_VOTE_ROW = re.compile(
    r'<div class="row">\s*<div class="col-lg-2 col-md-2 col-sm-2 col-xs-2">\s*(\d+)\s*</div>\s*'
    r'<div class="col-lg-5 col-md-5 col-sm-4 col-xs-4">\s*<div>(.*?)</div>\s*'
    r'(?:<p class="podpis"><i>(.*?)</i></p>\s*)?</div>\s*'
    r'<div class="voting-results">.*?'
    r'href="https://www\.senat\.gov\.pl/prace/posiedzenia/glosowanie-drukuj,(\d+)\.html"',
    re.S,
)


def day_votes(body: bytes, sitting_id: str, day_number: str) -> dict:
    """{'sitting_number', 'day_number', 'date_text', 'status_text', 'votes': [{vote_number, subject, subtitle,
    vote_id}]} of one sitting day; a day without votes has an empty list."""
    page = decoded(body)
    title = _one(r'<div class="meeting-title">\s*<h1[^>]*>(.*?)</h1>', page, "meeting title")
    heading = re.fullmatch(r"Posiedzenie: (\d+)\. posiedzenie Senatu RP XI kadencji, (\d+) dzień", text(title.group(1)))
    if not heading:
        raise ValueError(f"layout: heading {text(title.group(1))[:100]!r} is not a term XI sitting day")
    if heading.group(2) != str(day_number):
        raise ValueError(f"layout: heading names day {heading.group(2)}, the page is day {day_number}")
    if f"przebieg,{sitting_id},{day_number}," not in page:
        raise ValueError(f"layout: the page does not link its own address (sitting {sitting_id})")
    date = _one(r'<div class="view-element__date">\s*<span>(.*?)</span>', page, "sitting dates")
    status = _one(r'<div class="view-element__status">(.*?)</div>\s*</div>', page, "sitting status")
    links = re.findall(r"glosowanie-drukuj,(\d+)\.html", page)
    votes: list[dict] = []
    if _DESKTOP in page:
        desktop = page.split(_DESKTOP, 1)[1].split("<!-- mobile -->", 1)[0] if "<!-- mobile -->" in page else None
        if desktop is None:
            desktop = page.split(_DESKTOP, 1)[1].split(_MOBILE, 1)[0]
        header = text(desktop.split('<div class="table-content">', 1)[0])
        if not all(h in header for h in ("Nr głosowania", "Temat głosowania", "Wyniki głosowania")):
            raise ValueError(f"layout: votes table header {header[-120:]!r}")
        for m in _VOTE_ROW.finditer(desktop):
            votes.append(
                {
                    "vote_number": m.group(1),
                    "subject": text(m.group(2)),
                    "subtitle": text(m.group(3) or "") or None,
                    "vote_id": m.group(4),
                }
            )
        in_desktop = re.findall(r"glosowanie-drukuj,(\d+)\.html", desktop)
        if len(in_desktop) != len(votes) or [v["vote_id"] for v in votes] != in_desktop:
            raise ValueError(f"layout: {len(votes)} vote rows parsed, {len(in_desktop)} vote links in the table")
        if Counter(links) != Counter(in_desktop * 2):
            raise ValueError("layout: the desktop and mobile vote lists differ")
    elif links:
        raise ValueError(f"layout: {len(links)} vote links but no votes table")
    ids = Counter(v["vote_id"] for v in votes)
    if ids and max(ids.values()) > 1:
        raise ValueError("layout: a vote is listed twice")
    return {
        "sitting_number": heading.group(1),
        "day_number": heading.group(2),
        "date_text": text(date.group(1)),
        "status_text": text(status.group(1)),
        "votes": votes,
    }


# --- vote print page ----------------------------------------------------------------------------------------------

RESULT_LABELS = ["głosowało:", "za:", "przeciw:", "wstrzymało się:"]


def vote_result(body: bytes) -> dict:
    """{'title', 'subtitle', 'voted', 'votes_for', 'votes_against', 'abstained', 'senators_listed',
    'mark_counts'} of one vote print page (numbers as printed)."""
    page = decoded(body)
    box = _one(r'<div class="ajax-szczegoly-glosowania">(.*?)<footer', page, "vote details")
    content = box.group(1)
    title = _one(r'<h2 class="tytul">(.*?)</h2>', content, "vote title")
    subtitle = _one(r'<h3 class="podtytul">(.*?)</h3>', content, "vote subtitle")
    totals = _one(r'<div class="ogolne-wyniki">(.*?)</div>', content, "vote totals")
    pairs = re.findall(r'<span class="label">(.*?)</span>\s*<span class="value">(.*?)</span>', totals.group(1), re.S)
    labels = [text(label) for label, _ in pairs]
    if labels != RESULT_LABELS:
        raise ValueError(f"layout: vote totals labelled {labels}, expected {RESULT_LABELS}")
    values = [text(v) for _, v in pairs]
    senators = content.count('<div class="senator">')
    marks = re.findall(
        r'<div class="senator">\s*<div class="col-lg-3 col-sm-4 col-xs-6">(.*?)</div>\s*'
        r'<div class="col-lg-1 col-sm-2 col-xs-6">(.*?)</div>',
        content,
        re.S,
    )
    if len(marks) != senators:
        raise ValueError(f"layout: {senators} senator rows, {len(marks)} with a name and a mark")
    counted = Counter(text(mark) for _, mark in marks)
    return {
        "title": text(title.group(1)),
        "subtitle": text(subtitle.group(1)) or None,
        "voted": values[0],
        "votes_for": values[1],
        "votes_against": values[2],
        "abstained": values[3],
        "senators_listed": str(senators),
        "mark_counts": "|".join(f"{k}={v}" for k, v in sorted(counted.items())),
    }


# --- senators list ------------------------------------------------------------------------------------------------

_SENATOR = re.compile(
    r'<div class="senator-kontener">.*?'
    r'<a href="/sklad/senatorowie/senator,(\d+),(\d+),([a-z0-9-]+)\.html">(.*?)</a>\s*'
    r'(?:<p class="annotation">(.*?)</p>\s*)?<div class="wyroznienie">',
    re.S,
)


def senators(body: bytes) -> list[dict]:
    """[{senator_id, term, slug, name, annotation}] of the senators list (photos and biographies are not read)."""
    page = decoded(body)
    blocks = page.count('<div class="senator-kontener">')
    found = [
        {
            "senator_id": m.group(1),
            "term": m.group(2),
            "slug": m.group(3),
            "name": text(m.group(4)),
            "annotation": text(m.group(5) or "") or None,
        }
        for m in _SENATOR.finditer(page)
    ]
    if not blocks or len(found) != blocks:
        raise ValueError(f"layout: {blocks} senator blocks, {len(found)} parsed")
    terms = {s["term"] for s in found}
    if terms != {TERM}:
        raise ValueError(f"layout: senators of terms {sorted(terms)}, expected only {TERM}")
    return found
