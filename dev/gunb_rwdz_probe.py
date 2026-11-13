"""Read-only probe 2 for PL-ENV-033 (GUNB RWDZ): the producer's bulk files (pobranie.html).

GET/HEAD only, one request at a time with pauses, user agent set. Values of columns that may name persons or
addresses are never printed: only column names, fill counts, distinct counts and value shapes (letters -> a,
digits -> 9); sample values only for columns whose names look like codes, dates, kinds or authorities.
"""

import csv
import io
import re
import tempfile
import time
import urllib.request
import zipfile
from collections import Counter
from pathlib import Path

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
B = "https://wyszukiwarka.gunb.gov.pl"
FILES = ["dolnoslaskie", "kujawsko-pomorskie", "lodzkie", "lubelskie", "lubuskie", "malopolskie", "mazowieckie",
         "opolskie", "podkarpackie", "podlaskie", "pomorskie", "slaskie", "swietokrzyskie", "warminsko-mazurskie",
         "wielkopolskie", "zachodniopomorskie", "zgloszenia_2016_2021", "zgloszenia_2022_up"]
SAFE = re.compile(r"(?i)data|date|kod|teryt|terc|rodzaj|kategor|typ|organ|woj|powiat|gmina|jedn|status|rok|stan|decyz|wniosk|zgl|obiekt_w|^id|numer_ew|kubatura")
UNSAFE = re.compile(r"(?i)nazwisk|imie|imię|inwest|projekt|ulica|nr_dom|dzialk|działk|obreb|obręb|adres|miasto|miejsc|nazwa_zam|opis|kod_poczt|arkusz|cecha")


def fetch(url, n=None, method="GET", timeout=600):
    req = urllib.request.Request(url, headers={"User-Agent": UA}, method=method)
    t = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read(n) if n else (r.read() if method == "GET" else b"")
        return r.status, dict(r.headers), body, round(time.time() - t, 1)


def shape(v):
    return re.sub(r"[0-9]", "9", re.sub(r"[^\W\d_]", "a", v))[:40]


def profile(name, data: bytes):
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "f.zip"
        p.write_bytes(data)
        with zipfile.ZipFile(p) as z:
            infos = z.infolist()
            print(f"  members: {[(i.filename, i.file_size, i.date_time) for i in infos]}")
            for info in infos[:2]:
                raw = z.read(info)
                print(f"  {info.filename}: bytes {len(raw)} bom {raw[:3] == b'\xef\xbb\xbf'} crlf {raw.count(b'\r\n')} lf {raw.count(b'\n')}")
                text = raw.decode("utf-8-sig", errors="replace")
                lines = text.splitlines()
                print("  first line (header):", lines[0][:1500])
                header = lines[0].split("#")
                widths = Counter(len(line.split("#")) for line in lines[1:])
                print("  header fields", len(header), "line field counts", widths.most_common(6))
                rows = list(csv.reader(io.StringIO(text), delimiter="#", quotechar='"'))
                print("  csv-module rows", len(rows) - 1, "widths", Counter(len(r) for r in rows[1:]).most_common(5))
                quoted = sum(1 for line in lines[1:2000] if '"' in line)
                print("  lines with a double quote (first 2000)", quoted)
                body = [r for r in rows[1:] if len(r) == len(header)]
                for i, col in enumerate(header):
                    vals = [r[i].strip() for r in body]
                    filled = sum(1 for v in vals if v)
                    dist = Counter(vals)
                    sh = Counter(shape(v) for v in vals).most_common(4)
                    line = f"   [{i}] {col!r}: filled {filled}/{len(vals)} distinct {len(dist)} shapes {sh}"
                    if SAFE.search(col) and not UNSAFE.search(col):
                        line += f" top {[(k[:50], c) for k, c in dist.most_common(30)]}"
                        if re.search(r"(?i)data", col):
                            years = Counter(v[:4] for v in vals if v)
                            line += f" years {sorted(years.items())} min {min((v for v in vals if v), default=None)} max {max((v for v in vals if v), default=None)}"
                    else:
                        line += " (values not printed)"
                    print(line)
                for i, col in enumerate(header):
                    if re.search(r"(?i)numer_ew|^id", col):
                        dist = Counter(r[i] for r in body)
                        print(f"  key {col}: rows {len(body)} distinct {len(dist)} repeated {sum(1 for c in dist.values() if c > 1)} empty {dist.get('', 0)}")


def main():
    r = fetch(B + "/pobranie.html")
    t = r[2].decode("utf-8", "replace")
    print("terms words on pobranie.html:", re.findall(r"(?i)[^.]{0,120}(?:licenc|ponown|wykorzyst|regulamin|warunk|osobow|RODO)[^.]{0,120}", re.sub(r"<[^>]+>", " ", t))[:6])
    time.sleep(2)
    r = fetch(B + "/")
    t = re.sub(r"\s+", " ", re.sub(r"(?s)<script.*?</script>|<[^>]+>", " ", r[2].decode("utf-8", "replace")))
    print("terms words on home:", re.findall(r"(?i)[^.]{0,160}(?:licenc|ponown|wykorzyst|regulamin|warunk|osobow|RODO|jawn)[^.]{0,160}", t)[:8])
    print("# HEAD of every file")
    for f in FILES:
        time.sleep(1.5)
        try:
            st, h, _, s = fetch(f"{B}/pliki_pobranie/wynik_{f}.zip", method="HEAD", timeout=60)
            print(f"  {f}: {st} {h.get('Content-Length')} {h.get('Last-Modified')} {h.get('ETag')} {h.get('Content-Type')} {s}s")
        except Exception as e:  # noqa: BLE001
            print(f"  {f}: {type(e).__name__} {str(e)[:150]}")
    for f in ("opolskie", "zgloszenia_2022_up"):
        time.sleep(3)
        st, h, body, s = fetch(f"{B}/pliki_pobranie/wynik_{f}.zip")
        print(f"# {f}: {st} bytes {len(body)} in {s}s")
        profile(f, body)


if __name__ == "__main__":
    main()
