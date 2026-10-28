# Dimensional model

How Gold is modelled (owner decisions, 13 October 2026). Dimension definitions live as
code in [warehouse/dimensions/](../warehouse/dimensions/README.md), one YAML file per
dimension, with the source mappings and a coverage table over the 182-source inventory.
The dbt project in `warehouse/models/` builds Silver and Gold (Pipeline `platform`,
"silver/gold: build models"); sources not yet modelled are in Bronze only.

## Built (October 2026)

Gold v1 (cut over 28 Oct 2026, expand/contract D6): the former proof of concept (design draft 2:
series grain, facts by frequency, measure catalogue, concepts, metrics tables; history in
[docs/gold-poc.md](gold-poc.md)) is built from `warehouse/models/gold/` into the namespace
`gold`. Model files keep the `poc_` prefix; the tables are named by their aliases. The v0
tables and the `poc` namespace stay until the retire Task drops them.

| Gold table | Content |
|---|---|
| `dim_calendar` | calendar 1900–2100 at day grain, ISO weeks, Polish statutory holidays and working days from 1990, period labels and period-start flags; `date_key` is the date (decision D19: no separate `dim_date`) |
| `dim_geography`, `map_geography` | geography keyed by area (`PL`, `TERC:<code>`, `NUTS:<code>`) with each source's codes as attributes; every source geography code → `geography_key` |
| `dim_unit`, `dim_frequency`, `dim_source`, `dim_observation_status` | conformed units, frequencies, sources and observation flags |
| `dim_measure`, `measure_dimensions`, `dim_series` | measure catalogue, the dimensions each measure offers, one row per series (measure × geography × breakdown) |
| `fact_annual`, `fact_monthly`, `fact_daily` | observations per series and year / month / day |
| `dim_concept`, `concept_measures` | cross-source concepts (KPIs) and their measures |
| `metrics_population`, `metrics_gdp_current_prices`, `metrics_inflation`, `metrics_unemployment_rate`, `metrics_fx_rate` | the published metrics |
| `src_eurostat`, `src_dbw`, `src_dbw_dictionary`, `build_eurostat`, `build_bdl`, `build_dbw`, `build_wdi`, `build_nbp` | extraction and build stages per source |
| `build_check`, `grain_check` | per activated block rows, series, unmatched geography; fact grain check |
| `coverage` | one row per block a source publishes (Eurostat dataset, BDL subgroup, DBW file, GLEIF/MF file, OpenData member, Landing entity of NBP/WDI/TERYT/PRG): published and landed version, in Bronze, in Silver, Silver rows, status `loaded` / `loading` / `outdated` / `waiting` / `rejected` / `published without data` / `excluded` / `no longer published` (Eurostat: held, but gone from the inventory). Not dbt: each source's `bronze.verify` replaces its own rows from catalogue and table metadata (`ingestion/common/coverage.py`). `excluded` marks a published block outside the scope (Eurostat Comext datasets, from `_control/eurostat/catalogue.json`, rewritten from the whole inventory by every `landing.inspect` and `landing.extract`). A verify that stops before its checks (no snapshot, no Landing file) leaves the previous rows; `checked_at` shows their age. Tells a block that is not offered apart from a block not loaded yet |

Metrics (MetricFlow, `warehouse/semantic_models/gold.yml`): population, GDP at current prices,
unemployment rate, inflation and NBP exchange rates over the Gold v1 metrics tables.
A machine-readable catalog of every table, column, test, lineage edge, metric and registry dimension is published after each build (`warehouse/catalog.py`) and shown in the
portal.

## Three kinds of dimensions

| Kind | What it is | Keys and history |
|---|---|---|
| **Conformed** | Shared by all sources; built on an international standard where one exists (SDMX cross-domain code lists, ISO, NUTS, NACE, ISCED, ISCO, COICOP, COFOG, ESA 2010, ICD, CSA). Sources *enrich* it: every attribute records the source that contributed it. | Standard code (+ scheme and version where codes are versioned); correspondence tables between versions |
| **Entity** | Registers of real-world objects: organisations, persons in public roles, securities, markets, institutions, political actors, documents, transport stops and routes, monitoring sites, facilities, addresses, spatial features. | SCD2 (valid_from/valid_to); official identifiers (REGON, NIP, KRS, LEI, ISIN, MIC, RSPO, TERYT, ELI, GTFS ids …) as conformed keys, all of them in one identifier cross-reference with validity and evidence |
| **Source-specific** | Breakdowns only one source has (most Eurostat `indic_*`, BDL n1..n5 labels, DBW positional dimensions, NBP rate type). | Kept in a bridge next to the fact (`breakdown`), source codes and labels as published |

**Promotion rule.** When a second source has the same dimension, it is promoted to
conformed: adopt the standard (or the better-documented code list), add a mapping table
per source (`exact|broader|narrower|partial`), keep the source code on the fact, update
the registry. Entity matching uses identifiers or certified mappings only, never names.

## Special members

Every conformed dimension has the SDMX special members **`_T` total** and **`_Z` not
applicable** (plus `_U` unknown where a source can say "unknown"), so facts never carry
NULL keys. A fact that is not broken down by sex points to `sex = _T`; an indicator for
which sex makes no sense points to `_Z`. Source totals (`TOTAL`, `T`, `ogółem`) map to `_T`.

## Fact kinds

| Fact kind | Grain | Examples |
|---|---|---|
| Statistical observation | indicator × geography × period × conformed breakdowns × breakdown set; value, unit, observation status, source version | Eurostat, BDL, DBW, WDI, NBP rates |
| Entity snapshot | entity × as-of date (attribute states, counts) | GLEIF LEI records, white-list file, TERYT and PRG states |
| Event / transaction | one event with its timestamp | votes, procurement awards, filings, vehicle positions |
| Relationship | entity × entity × relationship type × validity | GLEIF parent relationships, ownership, memberships |

Statistical observations stay long. Values are stored once, as published, with the
unit key; never summed across indicators, units or overlapping breakdowns. Facts of
different sources are drilled across through conformed dimensions, not unioned.

## Indicator catalogue

`dim_indicator` is the entry point for people and AI agents. One row per source
indicator with: definition, measure type and aggregation rule, unit, price base and
seasonal adjustment, frequencies, the dimensions available and their members, coverage
(geography, period range, observation count), licence, last source update and
freshness, primary CSA domain (+ secondary, SDG and COFOG tags), and `same_as` links to
equivalent indicators of other sources (with evidence; never merged). The portal search
and the semantic layer read from it; a short reviewed description per indicator
supports AI discovery.

**Domain** uses the UN Classification of Statistical Activities (CSA 2.1): data domains
1–5, areas (e.g. 101 Population, 205 Prices) and subareas (e.g. 20302 Energy). Each
indicator gets one primary area. Provider themes (Eurostat TOC, OECD categories, WDI
topics, BDL K/G/P, DBW areas) map through crosswalks.

## Geography

- **Schemes with vintages**: ISO 3166 / UN M49; NUTS 2016/2021/2024; LAU per year;
  TERYT (TERC units, SIMC localities); GUS KTS / BDL units; World Bank economies, regions
  and income groups (dated); Eurostat GEO codes. A member is (scheme, code, vintage) with
  valid_from/valid_to.
- **Flattened hierarchy** L0–L7 per scheme (for Poland: country, makroregion,
  województwo, region, podregion, powiat, gmina, …) so any level can be rolled up.
- **Cross-scheme bridge** (BDL ↔ TERC ↔ NUTS ↔ LAU, ISO ↔ WB ↔ Eurostat) with
  relationship type and an optional weight. BDL unit id → TERC = characters 3–4 + 8–12
  (to verify against the GUS KTS–TERYT table). Weights are analytical assumptions, never
  source facts; fine geography is never joined to coarser observations as if measured.
- **Aggregates** (EU27_2020, EA20, World Bank regions) are flagged members with a dated
  composition.
- **Boundaries**: PRG geometries are keyed by TERYT per edition (`spatial_feature`).

## Time

- `dim_calendar` (built; holds the former `dim_date`, D19): one row per day; date roles on facts (effective, publication, as-of).
- `dim_period`: SDMX TIME_PERIOD (YYYY, YYYY-Qn, YYYY-MM, YYYY-Www, YYYY-MM-DD …) with
  frequency, start and end day. Annual values are never expanded to days.
- `bridge_period_day` links each period to its days for filtering and alignment.

## Units and measures

Unit = base unit + multiplier + currency (UCUM for physical units, ISO 4217 for
currencies, SDMX CL_UNIT_MULT). Price base (current, constant, chain-linked + reference
year) and seasonal adjustment are separate dimensions even where Eurostat or WDI bake
them into a unit code or indicator name; Silver parses them with reviewed mapping tables.

## Classifications

Versioned code lists with correspondence bridges: NACE Rev. 2/2.1 ↔ PKD 2007/2025 ↔
ISIC 4/5; CPA ↔ PKWiU; COICOP 2018; ISCED 2011 / ISCED-F 2013; ISCO-08 (KZiS);
HS/CN/SITC/BEC; ESA 2010 sectors; COFOG; ICD-10/11.

## Decisions taken (14–16 October 2026, under the owner's mandate)

1. **Latest version only across all layers (policy #822/#845, 16 October 2026):**
   Bronze, Silver and Gold hold only the latest version of every source entity. A Bronze
   load replaces the entity's previous version (superseded Landing files are pruned after each
   load); Silver models select the latest version per entity (or clear superseded batches);
   Gold fact and dimension tables are rebuilt from the deduplicated Silver tables. Superseded
   versions live only in the Landing Archive within configured retention.
2. **Local domain extensions beyond CSA:** only where CSA stops and a real source needs
   it (109.01 Media, 109.02 Sport); everything else maps to CSA codes.

Other open points are listed per dimension (`open_questions`) and marked `unverified`.

## Gold v1 (decided 27 Oct 2026)

1. Namespace gold; PoC tables keep their names without the poc_ prefix; v0 models dim_date, dim_period, dim_currency, dim_geography, dim_indicator, fact_observation, fact_fx_rate, fact_gold_price are deleted (dim_calendar replaces dim_date, PoC dim_geography replaces v0).
2. Keys stay natural strings (geography_key, measure_key, series_key as in the PoC); no hash keys in v1.
3. Geography: GUS KTS–TERYT correspondence for BDL ids, NUTS 3 ↔ podregion for DBW; name matching only as a reported fallback.
4. Measure rules per source stay as in the PoC for v1; per-domain refinements are new Tasks.
5. Facts: annual, monthly, daily; quarterly added with the first quarterly KPI.
6. Rows without a value are not loaded; status flags kept.
7. warehouse/entity/*.sql stay: they are per-entity Silver models applied by warehouse/entity.py (design #875), not Gold (deviation from plan A-23, logged in docs/decisions.md).
