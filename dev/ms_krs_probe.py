"""Read-only probe for PLG-012 (Open KRS API): route, response shape (keys only), limits, terms, selection size."""

from __future__ import annotations

import json
import re
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

API = "https://api-krs.ms.gov.pl/api/krs"
H = {"User-Agent": USER_AGENT, "Accept": "application/json"}
KNOWN = {"0000028860": "ORLEN", "0000026438": "PKO BP", "0000023302": "KGHM", "0000006865": "CD PROJEKT",
         "0000009831": "PZU", "0000635012": "ALLEGRO"}


def get(url, **kw):
    try:
        r = requests.get(url, headers=H, timeout=(20, 60), **kw)
        return r, r.status_code
    except requests.RequestException as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:200]}"


def tree(node, out, path=""):
    if isinstance(node, dict):
        for k, v in node.items():
            tree(v, out, f"{path}.{k}" if path else k)
    elif isinstance(node, list):
        out.add(path + "[]") if not node else [tree(v, out, path + "[]") for v in node[:50]]
    else:
        out.add(f"{path}:{type(node).__name__}")


def pick(node, path):
    for part in path.split("."):
        node = node.get(part) if isinstance(node, dict) else None
    return node


def main():
    # 1. robots and pages
    pages = {}
    for url in ["https://api-krs.ms.gov.pl/robots.txt", "https://prs.ms.gov.pl/robots.txt",
                "https://prs.ms.gov.pl/krs/openApi"]:
        r, code = get(url)
        row = {"status": code}
        if r is not None:
            text = r.text
            row.update({"bytes": len(r.content), "type": r.headers.get("Content-Type")})
            if url.endswith("robots.txt"):
                row["head"] = text[:400]
            else:
                plain = re.sub(r"<script.*?</script>|<style.*?</style>", " ", text, flags=re.S | re.I)
                plain = re.sub(r"<[^>]+>", " ", plain)
                plain = re.sub(r"\s+", " ", plain)
                row["text_len"] = len(plain)
                hits = []
                for m in re.finditer(r"(?i)limit|regulamin|licenc|warunk|ponown|biuletyn|odpispelny|zapyta[nń]", plain):
                    hits.append(plain[max(0, m.start() - 120): m.end() + 160])
                row["hits"] = hits[:8]
                row["links"] = sorted(set(re.findall(r'href="([^"]+(?:pdf|swagger|openapi|json|docx?)[^"]*)"', text, flags=re.I)))[:15]
                row["scripts"] = re.findall(r'<script[^>]+src="([^"]+)"', text)[:6]
                row["head"] = plain[:600]
        pages[url] = row
        time.sleep(1)
    annotate("notice", "probe pages", pages)

    # 2. dane.gov.pl dataset licence
    r, code = get("https://api.dane.gov.pl/1.4/datasets/27606")
    row = {"status": code}
    if r is not None and code == 200:
        a = r.json().get("data", {}).get("attributes", {})
        row["title"] = a.get("title")
        row["licence"] = {k: v for k, v in a.items() if "licen" in k.lower() and v not in (None, "", False)}
        row["flags_false"] = sorted(k for k, v in a.items() if "licen" in k.lower() and v is False)
        row["update_frequency"] = a.get("update_frequency")
        row["notes"] = re.sub(r"<[^>]+>", " ", a.get("notes") or "")[:900]
    annotate("notice", "probe dane.gov.pl dataset 27606", row)
    r, code = get("https://api.dane.gov.pl/1.4/datasets/27606/resources")
    row = {"status": code}
    if r is not None and code == 200:
        row["resources"] = [
            {k: (x.get("attributes", {}).get(k) or "")[:300] if isinstance(x.get("attributes", {}).get(k), str) else x.get("attributes", {}).get(k)
             for k in ("title", "link", "format", "description")} for x in r.json().get("data", [])][:8]
    annotate("notice", "probe dane.gov.pl resources", row)

    # 3. selection from GLEIF Silver
    sample, selection = [], {}
    try:
        import duckdb

        from ingestion.common.r2 import catalog

        t = catalog().load_table(("silver", "gleif_legal_entities"))
        g = t.scan(row_filter="legal_jurisdiction == 'PL'", selected_fields=(
            "lei", "legal_name", "legal_form_code", "entity_status", "registration_status",
            "registration_authority_id", "registration_authority_entity_id", "legal_address_region")).to_arrow()
        con = duckdb.connect()
        con.register("g", g)
        q = lambda s: con.sql(s).fetchall()
        selection["pl_rows"] = g.num_rows
        selection["by_ra"] = q("select registration_authority_id, count(*) from g group by 1 order by 2 desc limit 8")
        k = "from g where registration_authority_id = 'RA000484'"
        selection["krs"] = q(f"""select count(*), count(distinct registration_authority_entity_id),
            count(*) filter (where regexp_full_match(registration_authority_entity_id, '[0-9]{{10}}')),
            count(distinct lpad(regexp_replace(registration_authority_entity_id, '[^0-9]', '', 'g'), 10, '0')),
            min(registration_authority_entity_id), max(registration_authority_entity_id) {k}""")
        selection["bad_ids"] = q(f"select registration_authority_entity_id from g where registration_authority_id = 'RA000484' and not regexp_full_match(registration_authority_entity_id, '[0-9]{{10}}') limit 12")
        selection["by_reg_status"] = q(f"select registration_status, count(*) {k} group by 1 order by 2 desc")
        selection["by_entity_status"] = q(f"select entity_status, count(*) {k} group by 1 order by 2 desc")
        selection["dup_ids"] = q(f"select count(*) from (select registration_authority_entity_id {k} group by 1 having count(*) > 1)")
        selection["upto"] = q(f"select count(*) filter (where registration_authority_entity_id <= '0000010000'), count(*) filter (where registration_authority_entity_id <= '0000030000'), count(*) filter (where registration_authority_entity_id <= '0000100000') {k}")
        v = "registration_authority_id = 'RA000484' and regexp_full_match(registration_authority_entity_id, '[0-9]{10}')"
        sample = [x[0] for x in q(f"""
            (select registration_authority_entity_id from g where {v} and legal_name ilike '%likwidacji%' order by lei limit 8)
            union (select registration_authority_entity_id from g where {v} and legal_name ilike '%upad%' order by lei limit 8)
            union (select registration_authority_entity_id from g where {v} and (legal_name ilike '%fundacja%' or legal_name ilike '%stowarzyszenie%') order by lei limit 6)
            union (select registration_authority_entity_id from g where {v} and entity_status = 'INACTIVE' order by lei limit 6)
            union (select registration_authority_entity_id from g where {v} and hash(lei) % 1000 = 7 order by lei limit 20)""")]
    except Exception as exc:  # noqa: BLE001
        selection["error"] = f"{type(exc).__name__}: {str(exc)[:600]}"
    annotate("notice", "probe gleif selection", selection)

    # 4. API: extracts (keys only) for known + sampled numbers, one at a time
    keys, statuses, timings, headers, values, sizes = set(), {}, [], {}, {}, []
    numbers = list(KNOWN) + [n for n in sample if n not in KNOWN]
    registers = {}
    for n in numbers[:60]:
        started = time.monotonic()
        r, code = get(f"{API}/OdpisAktualny/{n}", params={"rejestr": "P", "format": "json"})
        timings.append(round(time.monotonic() - started, 2))
        used = "P"
        if code == 404:
            time.sleep(1)
            r, code = get(f"{API}/OdpisAktualny/{n}", params={"rejestr": "S", "format": "json"})
            used = "S"
        statuses[f"{used}:{code}"] = statuses.get(f"{used}:{code}", 0) + 1
        if r is not None and not headers:
            headers = {k: v[:120] for k, v in r.headers.items() if k.lower() not in ("set-cookie",)}
        if r is not None and code == 200:
            sizes.append(len(r.content))
            try:
                doc = r.json()
            except ValueError:
                statuses["not_json"] = statuses.get("not_json", 0) + 1
                continue
            tree(doc, keys)
            registers[pick(doc, "odpis.naglowekA.rejestr")] = registers.get(pick(doc, "odpis.naglowekA.rejestr"), 0) + 1
            if n in KNOWN:
                d1 = pick(doc, "odpis.dane.dzial1") or {}
                values[n] = {"naglowekA": pick(doc, "odpis.naglowekA"), "rodzaj": pick(doc, "odpis.rodzaj"),
                             "danePodmiotu": d1.get("danePodmiotu"), "siedziba": (d1.get("siedzibaIAdres") or {}).get("siedziba"),
                             "kapital": d1.get("kapital"),
                             "pkd": pick(doc, "odpis.dane.dzial3.przedmiotDzialalnosci.przedmiotPrzewazajacejDzialalnosci")}
        elif r is not None and len(statuses) < 6:
            statuses[f"body_{used}_{code}"] = r.text[:200]
        time.sleep(1)
    annotate("notice", "probe api statuses", {"statuses": statuses, "registers": registers, "requests": len(numbers[:60]),
             "seconds_min_med_max": [min(timings), sorted(timings)[len(timings) // 2], max(timings)] if timings else None,
             "bytes_min_max": [min(sizes), max(sizes)] if sizes else None, "headers": headers})
    ks = sorted(keys)
    top = [x for x in ks if not x.startswith("odpis.dane.dzial")]
    annotate("notice", "probe keys top", top)
    for d in ("dzial1", "dzial2", "dzial3", "dzial4", "dzial5", "dzial6"):
        part = [x.replace(f"odpis.dane.{d}.", "") for x in ks if x.startswith(f"odpis.dane.{d}")]
        text = json.dumps(part, ensure_ascii=False)
        print(f"KEYS {d} {text}", flush=True)
        annotate("notice", f"probe keys {d}", part)
    print("VALUES " + json.dumps(values, ensure_ascii=False), flush=True)
    annotate("notice", "probe values orlen", values.get("0000028860"))

    # 5. bulletin of changed numbers
    bulletin = {}
    for day in ("2026-11-10", "2026-01-15", "2022-06-01"):
        for name in ("Biuletyn",):
            r, code = get(f"https://api-krs.ms.gov.pl/api/Krs/{name}/{day}")
            row = {"status": code}
            if r is not None:
                row["type"] = r.headers.get("Content-Type")
                row["bytes"] = len(r.content)
                try:
                    doc = r.json()
                    row["json"] = type(doc).__name__
                    row["n"] = len(doc)
                    row["head"] = doc[:5] if isinstance(doc, list) else {k: (v if not isinstance(v, (list, dict)) else str(v)[:120]) for k, v in list(doc.items())[:8]}
                except ValueError:
                    row["text"] = r.text[:200]
            bulletin[f"{name}/{day}"] = row
            time.sleep(1)
    annotate("notice", "probe bulletin", bulletin)


if __name__ == "__main__":
    main()
