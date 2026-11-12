"""Read-only probe 2 for CD-006 (KNF OAM list) and CD-007 (GPW report list): by-date pattern, paging, depth.
GET (and the list form's own POST to ajaxindex.php), one request at a time, 5 seconds apart, identified
User-Agent. List pages only: never a report document or file. Prints counts to the job log; nothing is written."""

from collections import Counter
import html as htmllib
import re
import sys
import time

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
PAUSE = 5
_n = 0
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept-Language": "pl,en"})


def call(method, url, **kw):
    global _n
    if _n:
        time.sleep(PAUSE)
    _n += 1
    t0 = time.time()
    try:
        r = S.request(method, url, timeout=60, **kw)
    except requests.RequestException as exc:
        print(f"### {method} {url} -> {type(exc).__name__}: {str(exc)[:200]}", flush=True)
        return None
    print(f"### {method} {url} {kw.get('data') or ''} -> {r.status_code} {len(r.content)} bytes {time.time() - t0:.1f}s "
          f"ct={r.headers.get('content-type')}", flush=True)
    return r


KNF_ROW = re.compile(
    r'(?is)<tr class="(?:even|odd)">\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(\d{4}-\d{2}-\d{2})</td>\s*<td[^>]*>'
    r'<a href="?0/([0-9A-F]{32})"?>(.*?)</a></td>\s*<td[^>]*>.*?</td>\s*</tr>')


def knf_page(day, start=1, count=20):
    r = call("GET", f"https://moam.knf.gov.pl/moam.nsf/mOAM?readForm&RestrictToCategory={day}&start={start}&count={count}")
    if r is None or r.status_code != 200:
        return None
    text = r.content.decode("utf-8", "replace")
    msg = re.search(r'(?s)<div id="message">(.*?)</div>', text)
    rows = KNF_ROW.findall(text)
    trs = len(re.findall(r'<tr class="(?:even|odd)"', text))
    pager = sorted(set(re.findall(r"RestrictToCategory=[^\"']*", text)))
    print(f"  day={day} start={start} message={' '.join(msg.group(1).split()) if msg else None!r} rows={len(rows)} trs={trs} "
          f"days_in_rows={Counter(x[1] for x in rows).most_common(3)} pager={pager}")
    print("  prefixes:", Counter(htmllib.unescape(x[3]).split(",")[0].strip()[:25] for x in rows).most_common(12), flush=True)
    return text, rows


def knf():
    r = call("GET", "https://moam.knf.gov.pl/moam.nsf/js/index.js")
    if r is not None and r.status_code == 200:
        js = r.text
        for m in list(re.finditer(r"(?i)datepicker|RestrictToCategory|readForm", js))[:6]:
            print("  JS ", re.sub(r"\s+", " ", js[max(0, m.start() - 300): m.start() + 500]))
    got = knf_page("2026-11-12", 1)
    if got:
        text = got[0]
        i = text.find("</table>")
        print("  AFTER TABLE ", re.sub(r"\s+", " ", text[i: i + 1500]))
    knf_page("2026-11-12", 81, 10)
    knf_page("2026-11-10", 1)
    for day in ("2026-11-08", "2026-10-12", "2025-11-12", "2021-11-12", "2016-11-10", "2011-11-10", "2009-03-02", "2026-11-13"):
        knf_page(day, 1)


GPW_ITEM = re.compile(
    r'(?is)<li[^>]*>\s*<span class="date">([^<]*)</span>\s*<strong class="name">\s*<a href="espi-ebi-report\?geru_id=(\d+)[^"]*">'
    r'(.*?)</a>\s*</strong>.*?<p>(.*?)</p>')
FORM = {"action": "GPWEspiReportUnion", "start": "ajaxSearch", "page": "espi-ebi-reports", "format": "html", "lang": "EN",
        "letter": "", "searchText": ""}


def gpw_items(text, label):
    items = GPW_ITEM.findall(text)
    lis = len(re.findall(r"geru_id=", text)) // 2
    heads = [[p.strip() for p in x[0].split("|")] for x in items]
    ids = [int(x[1]) for x in items]
    print(f"  {label}: items={len(items)} links/2={lis} ids={min(ids, default=None)}..{max(ids, default=None)} "
          f"systems={Counter(h[2] if len(h) > 2 else '?' for h in heads)} categories={Counter(h[1] if len(h) > 1 else '?' for h in heads)} "
          f"days={Counter(h[0][:10] for h in heads).most_common(6)} first={heads[0] if heads else None} last={heads[-1] if heads else None} "
          f"isin_missing={sum(not re.search(r'\(([A-Z]{2}[A-Z0-9]{9}[0-9])\)\s*$', htmllib.unescape(x[2]).strip()) for x in items)}")
    more = re.findall(r'<a href="" class="more"[^>]*>', text)
    print("  more:", more[:2], "len", len(text), flush=True)
    return items


def gpw_post(label, **fields):
    data = [(k, v) for k, v in {**FORM, **{k: v for k, v in fields.items() if not isinstance(v, list)}}.items()]
    for k, v in fields.items():
        if isinstance(v, list):
            data += [(k, x) for x in v]
    r = call("POST", "https://www.gpw.pl/ajaxindex.php", data=data, headers={"X-Requested-With": "XMLHttpRequest"})
    if r is None or r.status_code != 200:
        print("  body:", (r.text[:300] if r is not None else None))
        return []
    if "geru_id" not in r.text:
        print("  body without rows:", re.sub(r"\s+", " ", r.text[:400]))
    return gpw_items(r.text, label)


def gpw():
    r = call("GET", "https://www.gpw.pl/robots.txt")
    print("  robots:", r.text.replace("\n", " | ") if r is not None else None)
    r = call("GET", "https://www.gpw.pl/espi-ebi-reports")
    if r is None or r.status_code != 200:
        return
    text = r.text
    for word in (r"datepicker\(", r"data-type=\"pager\"|data-type='pager'|\.more", "typeRaports"):
        for m in list(re.finditer(word, text))[:3]:
            print(f"  AROUND {word!r} @{m.start()}: ", re.sub(r"\s+", " ", text[max(0, m.start() - 400): m.start() + 900]))
    i = text.find("function loadSearchData")
    print("  JS loadSearchData: ", re.sub(r"\s+", " ", text[i: i + 3500]))
    for m in re.finditer(r'(?is)<label[^>]*>\s*<input[^>]*name="(?:typeRaports|categoryRaports)\[\]"[^>]*>(.*?)</label>', text):
        print("  LABEL ", re.sub(r"\s+", " ", m.group(0))[:300])
    gpw_items(text, "page")
    all_types = ["RB", "P", "Q", "O", "R"]
    gpw_post("ebi offset 0 limit 15", offset="0", limit="15", date="", **{"categoryRaports[]": ["EBI"], "typeRaports[]": all_types})
    gpw_post("ebi offset 15 limit 30", offset="15", limit="30", date="", **{"categoryRaports[]": ["EBI"], "typeRaports[]": all_types})
    for d in ("10-11-2026", "2026-11-10"):
        gpw_post(f"ebi date {d}", offset="0", limit="15", date=d, **{"categoryRaports[]": ["EBI"], "typeRaports[]": all_types})
    gpw_post("ebi date 10-11-2026 offset 15", offset="15", limit="30", date="10-11-2026", **{"categoryRaports[]": ["EBI"], "typeRaports[]": all_types})
    for d in ("08-11-2026", "12-10-2026", "12-11-2025", "12-11-2021", "10-11-2016", "10-11-2011", "13-11-2026"):
        gpw_post(f"ebi date {d}", offset="0", limit="15", date=d, **{"categoryRaports[]": ["EBI"], "typeRaports[]": all_types})
    r = call("GET", "https://www.gpw.pl/espi-ebi-reports?categoryRaports=EBI&typeRaports=RB,P,Q,O,R&searchText=&date=10-11-2026")
    if r is not None and r.status_code == 200:
        gpw_items(r.text, "GET page with query")


if __name__ == "__main__":
    which = sys.argv[1:] or ["knf", "gpw"]
    if "knf" in which:
        knf()
    if "gpw" in which:
        gpw()
