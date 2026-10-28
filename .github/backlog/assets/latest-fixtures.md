# latest.json fixtures and their check (product owner, 28 Oct 2026)

Source for Task `data/published/published-v1/contract/latest-fixtures` (decision D26). Values are illustrative,
like every fixture. Every value carries its own `unit_key` (docs/published.md, #2148); gdp_current_prices has two
units on purpose, because the values of one KPI may differ in unit.

## Fixture files

Layout of `fixtures/kpi/unemployment_rate/latest.json`: 2-space indent, keys in the order kpi_key, unit_key,
generated_at, values; value keys in the order source_key, geography_key, geography_level, period, value, unit_key,
previous_period, previous_value, flags, checked_at; `generated_at` and every `checked_at` `2026-01-01T00:00:00Z`;
a trailing newline. Rows below give the values of each file (`-` = key left out).

| file (kpi_key, unit_key) | source_key | geography_key | geography_level | period | value | unit_key | previous_period | previous_value | flags |
|---|---|---|---|---|---|---|---|---|---|
| population (persons) | eurostat | PL | country | 2025 | 36620000 | persons | 2024 | 36750000 | [] |
| population (persons) | gus_bdl | TERC:12 | voivodeship | 2024 | 3430000 | persons | 2023 | 3440000 | [] |
| population (persons) | gus_bdl | TERC:14 | voivodeship | 2024 | 5510000 | persons | 2023 | 5500000 | [] |
| gdp_current_prices (mln_ncu) | eurostat | PL | country | 2024 | 845000 | mln_eur | 2023 | 750000 | ["p"] |
| gdp_current_prices (mln_ncu) | world_bank_wdi | PL | country | 2024 | 914700000000 | usd | - | - | [] |
| inflation (pct) | eurostat | PL | country | 2026-09 | 2.9 | pct | 2026-08 | 3.0 | [] |
| inflation (pct) | world_bank_wdi | PL | country | 2025 | 3.6 | pct | - | - | [] |

Periods are strings ("2025"), values numbers. zohelo copies these files into fixtures/published/ (zohelo#235), so the
values here are the ones both repos use.

## Check in ops/repo_check.py published_contract()

After the fixture loop (add `import ast` to the standard-library import block, sorted, if it is missing):

```python
    # D26: every published KPI outside publish.SKIP has a latest.json fixture. SKIP is read with ast
    # because warehouse.publish imports duckdb.
    tree = ast.parse((ROOT / "warehouse" / "publish.py").read_text(encoding="utf-8"))
    skip = next(
        set(ast.literal_eval(node.value))
        for node in tree.body
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "SKIP" for t in node.targets)
    )
    catalog = json.loads((fixtures / "catalog" / "catalog.json").read_text(encoding="utf-8"))
    for kpi in catalog.get("kpis") or []:
        key = kpi.get("kpi_key")
        if kpi.get("status") != "published":
            continue
        latest = fixtures / "kpi" / str(key) / "latest.json"
        if key in skip:
            if latest.exists():
                problems.append(f"kpi/{key}/latest.json: {key!r} is in publish.SKIP and has no latest.json (D26)")
        elif not latest.exists():
            problems.append(f"kpi/{key}/latest.json: missing for published KPI {key!r} (D26)")
        elif (found := json.loads(latest.read_text(encoding="utf-8")).get("kpi_key")) != key:
            problems.append(f"kpi/{key}/latest.json: kpi_key {found!r} is not {key!r}")
```

Docstring of published_contract(), new last sentence: "Every published KPI of the catalog fixture outside
publish.SKIP has a kpi/<kpi_key>/latest.json fixture (D26)."

README.md (warehouse/published), end of the `fixtures/` line: "; one `kpi/<kpi_key>/latest.json` per published KPI
except those in `publish.SKIP` (D26)."
