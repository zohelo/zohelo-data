# Source registration: a source's lines live in the source's own files

Decision D34 (12 November 2026). Until now every new source appended lines to the same shared files, so two
source pull requests always conflicted and were merged one at a time or as hand-built merge trains. The rule
now: **adding a source touches only files that belong to that source.** The shared files keep what they
held on 12 November (nothing was moved, so open pull requests still merge) and take no new source lines. The
code lists every source adds rows to followed on 13 November (D36, below).

## Where a new source's lines go

| Was appended to | A new source writes instead | How it is found |
|---|---|---|
| `pipeline.yml`, `source` options | nothing | the input is free text; the plan job refuses an id that is not `ingestion/sources/<source>.yaml` or `platform` (`run.py plan` checks again) |
| `ingestion/common/lineage.py`, `SOURCES` | nothing | `lineage.sources()`: every declared source with `bronze.load` in `activities` |
| `warehouse/warehouse.yaml`, `gold` | `gold:` in `ingestion/sources/<source>.yaml` | `warehouse/declared.py` `gold()` = `warehouse.yaml` + every source's list |
| `warehouse/warehouse.yaml`, `full_rebuild` | `full_rebuild:` in `ingestion/sources/<source>.yaml` (`model: {rows: …}`, measured count in a comment) | `warehouse/declared.py` `full_rebuild()` |
| `warehouse/models/bronze/sources.yml` | `warehouse/models/bronze/<source>.yml` (one `sources:` entry named after the source) | dbt reads every YAML file under `models/` |
| `warehouse/models/gold/_gold.yml` | `warehouse/models/gold/_<source>.yml` for the source's own models | dbt reads every YAML file under `models/` |
| `poc_blocks.sql`: block macros, `poc_block_models()`, `poc_activated_blocks()`, the union in `poc_observations()` | `warehouse/macros/sources/<source>.sql`: the block macros and `poc_source__<source>()` | `macros/source_registration.sql` finds the macros by name |
| `poc_map_geography.sql`: a CTE and a union branch | `poc_geography__<source>()` in the same `warehouse/macros/sources/<source>.sql` | the same |
| `docs/sources.md` | `docs/sources/<source>.md` (the same section, as its own file) | – |
| `warehouse/tests/metrics_value_range.sql`: a row of `bounds`, a branch of the `metrics` union | `warehouse/macros/value_range/<source>.sql`: `poc_value_range__<source>()` with the bounds and the metrics models | the test finds the macros by name (below, "Value ranges") |
| `warehouse/seeds/source.csv`, `licence.csv`, `kpi.csv`, `kpi_measure.csv`, `kpi_v2_map.csv`, `unit.csv`, `unit_map.csv`, `unit_decimals.csv` | `warehouse/seed_rows/<list>/<source>.csv` (the list's header, then the source's rows; D36, below) | the model `warehouse/models/gold/seed_rows/<list>.sql` reads every CSV file of the folder; Python reads `warehouse/seed_rows.py` `rows()` |

`warehouse.yaml` keeps what belongs to no single source: conformed dimensions, facts, seeds, KPI metrics
that read several sources, and their ceilings.
`models/bronze/sources.yml` keeps the tables of no single source's Bronze (the Silver tables the loaders write
per entity, `gold.coverage`) and `models/gold/_gold.yml` the shared Gold models; the entries of the sources
registered before D34 moved to their own files on 16 November.

### The Gold registration file

`warehouse/macros/sources/<source_system>.sql` (the file name is the `source.csv` key):

```sql
{% macro poc_example_blocks() %}
    select * from (values ('rates', 'Example rates')) as t (entity, block_name)
{% endmacro %}

{% macro poc_source__example() %}
    {{ return({
        'block_model': 'poc_build_example',
        'activated': "select 'example', entity, block_name from (" ~ poc_example_blocks() ~ ")",
    }) }}
{% endmacro %}

{% macro poc_geography__example() %}
example as (
    select
        'example' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
)
{% endmacro %}
```

- `poc_source__<source>()` returns `block_model` and either `activated` (one select of source_system,
  block_id, block_name, in that order) or `blocks` (`[[block_id, block_name], …]` for a short fixed list).
  `poc_block_models()`, `poc_observations()` and `poc_activated_blocks()` include it; the waiting-block
  rule of `warehouse/changed.py` (tag `gold_block`) is unchanged.
- `poc_geography__<source>()` is optional (a source without areas maps to `_Z` in its build stage). It
  holds CTE text: one CTE named `<source_system>` with `source_system, source_code, geography_key`, helper
  CTEs named `<source_system>_<x>`. It is placed right after `geo`, so it may read `geo` and any `ref()`,
  not the CTEs of other sources. Prefer static lists and `geo` (iso2, iso3, wb_code) over the
  source's own Silver table, so the shared model never waits for a new source.
- Registered sources are placed in name order (in the geography map closed by an empty branch; the activated
  blocks under the column names `source_system, block_id, block_name`, so a select need not name them): the
  compiled SQL depends on which sources exist, never on the order pull requests were merged.
- A block that emits other source_systems too (`poc_build_public_finance`: mf_public_debt, mf_treasury_debt,
  mf_state_budget, krus) is registered by one source, whose `activated` select lists every source's blocks, and
  names the others in its config meta `source_systems` (`warehouse/changed.py`).

How the macros are found: dbt gives a macro every project macro by name in `context`;
`poc_registered(prefix)` lists those starting with `poc_source__` or `poc_geography__`. `ref()` calls inside
them are recorded at parse time like any other. This is long-standing dbt behaviour but not a documented
interface, so `poc_registered` fails the parse if it cannot find itself there (a dbt upgrade that changes
this fails `check`, it does not drop sources silently). Partial parsing is off (`dbt_project.yml`), because
it cannot follow macros found by name; a full parse takes about five seconds.

## What stays shared, and why

- **The `workflow_dispatch` choice list** cannot be built from files: GitHub reads `options` from the
  workflow file itself. It became a free-text input validated in the plan job. The text also names the run's
  concurrency group before the plan job runs, so a typed `scheduler` or a text with a hyphen gets a group of
  its own, and a source id may only be lower-case letters, digits and `_` (`run.py`). Cost: the owner types the
  source id instead of picking it; a typo fails the plan job in seconds with the message naming the rule.
- **`kpi`, `unit`, `unit_decimals` and `licence` rows** belong to a KPI, a unit or a licence, not to a source, so
  no check can say which file a row belongs in. The rule is the same as for the other lists (next section): the
  pull request that adds the row puts it into the file named after its source. A KPI pull request still adds a
  metrics model of several sources to `warehouse.yaml`.
- **Lines of sources registered before D34** moved to the sources' own files one kind at a time, each move
  proved not to change the resolved lists, the compiled SQL or a build fingerprint. Moved (16 November): the
  sources' Gold tables and row ceilings in `warehouse.yaml` (`gold.build_public_finance` stays, shared by
  the MF family). Moved (25 November): their sections of `docs/sources.md`, one file each in `docs/sources/`
  (the NBP section gave `nbp_interest_rates` and `nbp_bop` their own files; the text is unchanged, headings one
  level up); `docs/sources.md` keeps the register status table and the route matrix of the first five sources.
  Moved (26 November): the geography CTEs of 17 sources from `poc_map_geography.sql` to
  `poc_geography__<source>()` in `macros/sources/<source>.sql` (gus_bdl, gus_dbw, mrpips_unemployment, oecd, gleif,
  gus_teryt, gugik_prg, the five World Bank sources, zus_psz, ecb, un_wpp, unhcr, eea; the World Bank map is
  `poc_world_bank_economies(source_system)` in `world_bank_wdi.sql`, one CTE per source instead of the shared `ids`
  and `wb_family`). The map returns the same rows; the compiled SQL of no other model changed. Moved (26 November,
  second step): the last four, unctad, who, unesco_uis and faostat, which read their own Silver tables through
  `poc_optional_ref`. Their tables stay in the map's config meta `optional_parents` (`warehouse/changed.py` plans by
  it), and `repo_check` now pairs that list with the calls in every `poc_geography__<source>()` as well as the
  model's own text; a call anywhere else in a source's macro file fails the check. The model writes no source's CTE
  any more: the registered CTEs, then one union of them closed by an empty branch (the compiled map has the same 95
  CTEs, text unchanged, and the same 71 branches, now all in name order). Moved (27 November): the Gold blocks of
  the 24 sources in `poc_legacy_block_models()` and `poc_legacy_activated_blocks()` of `poc_blocks.sql`, each
  with the block lists its build model and geography macro read (`poc_eurostat_blocks`, `poc_oecd_areas`, …),
  to `poc_source__<source>()` in `macros/sources/<source>.sql` (new files for nbp, mf_public_debt,
  mf_biala_lista and mf_jst_budgets); the two shared lists are gone and `repo_check` fails if they come back. The
  resolved block list holds the same 113 blocks and the activated list the same 352 rows; the unions of the five
  facts, `poc_dim_series`, `poc_dim_measure`, `poc_dim_unit` and the two checks read the same blocks, now all in
  name order (rows unchanged). `poc_blocks.sql` keeps only what every source shares: `poc_nuts_pl`,
  `poc_woj_makroregion`, `poc_concepts`, `poc_observations` and its helpers. Still shared: the `optional_parents`
  list itself, for the rare source whose map must read its own Silver table (prefer static codes).

## Value ranges

`warehouse/tests/metrics_value_range.sql` fails every metrics row outside its KPI × source bound, and every row
without one. Each source added a row to its `bounds` and a branch to its `metrics` union at the same two places, so
after D36 it was the main cause of conflicts between source pull requests (#3002, #3136). Since 16 November a
source declares both in its own file, `warehouse/macros/value_range/<source_system>.sql`:

```sql
{# Example (#123): highest value and where it was measured (Dev trial 1234567890). #}
{% macro poc_value_range__example() %}
    {{ return({
        'bounds': [
            ['example_rate', '%', '0', '100'],
            ['example_count', 'example:table_1:%', '0', '5e6'],
        ],
        'models': ['poc_metrics_example_rate', 'poc_metrics_example_count'],
    }) }}
{% endmacro %}
```

- A bound is `[concept_key, measure_key_pattern, value_min, value_max]`; the source is the file's. The numbers are
  text, written as SQL reads them (`'5e7'`, `'-0.5'`), so the compiled test shows them as written. The evidence for
  each bound goes into the comment, as before.
- `models` lists the `poc_metrics_*` models of the source's KPIs. A model that several sources feed is listed by
  each of them; the union reads it once. `'models': []` until the KPI's metrics model exists (the bound then waits,
  as the documentation-only rows of the test file do).
- The test reads the rows and models written in it first (the sources from before the split; edited in place, no
  new rows or models), then each source's file in name order. It fails the parse on a malformed bound, a bound
  declared twice by one source, or a model that is not `poc_metrics_*`.
- Moved on 16 November: the bounds and the single-source models of cie_sio, crossref, gugik_prng, gus_culture,
  gus_culture_participation, gus_nsp2021, gus_psr2020, gus_tourism, mrpips_social_assistance, ms_isws, owid,
  policja_statistics, radon, rpwdl, ted_notices and ulc_airports (the compiled test reads the same 116 bounds and 57 models, and returns the
  same rows on the same data). The other sources' rows stay in the test file.
- Counts and levels cannot be 0 (#2807): for the KPIs in the test's `positive_concepts` (population,
  population_projection, gdp_current_prices, gdp_per_capita, fx_rate) the lower bound is exclusive, whatever the
  source's bound says, so a 0 stored in place of a missing value fails. A 0 that the publisher itself prints goes into
  `published_zeros` (KPI, source, measure, area) with the evidence; the only one is the World Bank's Serbian GDP in
  current dinars, 1990-1992.
- No model calls these macros (`repo_check` `configuration`), and `warehouse/changed.py` keeps
  `macros/value_range/` out of every build fingerprint: a new or changed bound rebuilds nothing. The test runs when
  one of the models it reads is built, as before.

## Code lists kept as rows per source (D36)

Every source added a row to `source.csv` and `licence.csv`, and most added rows to `kpi.csv`, `kpi_measure.csv`,
`kpi_v2_map.csv`, `unit.csv`, `unit_map.csv` and `unit_decimals.csv`. After D34 these rows were the only cause
of conflicts between source pull requests: `merge=union` in `.gitattributes` joins them in a local merge, but
GitHub's own merge does not use it (trial of 12 November: pull requests #2927 and #2928, each appending one row
to `source.csv` after another row was appended on the base, were `mergeable: false`).

Since 13 November each of the eight lists is a folder, `warehouse/seed_rows/<list>/`:

- `_shared.csv` holds the rows from before the split. Rows there are edited in place (a KPI's status, a licence
  text); it takes **no new rows**.
- `<source_system>.csv` holds the rows a source's pull requests add: the same header line, then the rows. In
  `source`, `kpi_measure`, `kpi_v2_map` and `unit_map` every row names its source, and the row goes into that
  source's file. In `kpi`, `unit`, `unit_decimals` and `licence` the row goes into the file of the source whose
  pull request adds it (a unit that two sources need is added once; the second pull request finds it there).
- A file is created by `git add`; nobody appends to a file another pull request appends to, so GitHub has
  nothing to merge. Two pull requests for the same source can still meet in that source's file.

How the rows reach Gold. The lists are no longer dbt seeds (dbt builds one table per CSV file and cannot union
files it finds by name). `warehouse/models/gold/seed_rows/<list>.sql` is a dbt model named like the seed was, so
`ref('kpi')` and `gold.kpi` are unchanged. It reads `seed_rows/<list>/*.csv` from the checkout with DuckDB
(`read_csv`, every column as text, columns found by header name) and casts the typed columns itself; the tests
that were in `seeds/_seeds.yml` are in `models/gold/seed_rows/_seed_rows.yml`. There is no generation step and
no generated file: `dbt parse`, `dbt run` and `dbt build` work on a fresh checkout from any directory (the
folder is found through dbt's `invocation_args_dict.project_dir`). A trial that built the seeds with
`dbt seed` builds these eight with the Gold models (`dbt run --select path:models/gold`, or
`path:models/gold/seed_rows` alone).

Python (`gold.publish`, the semantic manifest, `repo_check`) reads a list through `warehouse/seed_rows.py`
`rows(<list>)`: the files in file-name order, `_shared.csv` first. That function also reads the lists that are
still dbt seeds (`warehouse/seeds/<list>.csv`: domain, frequency, topics, …; no source adds rows to them).

Differences from a dbt seed, none of which changes a row of 13 November (the eight tables built both ways have
the same columns, types and rows): an empty field is NULL, and the text `null` is now kept as text (dbt's seed
loader read it as NULL); a column's type is the model's cast, not guessed from the values, so one odd row cannot
change it. The docs still call a list by its old file name (`kpi.csv` is the list `kpi`).

**Open pull requests that added rows to `warehouse/seeds/<list>.csv`.** Merge `origin/main`. Git follows the
rename, so the rows arrive in `warehouse/seed_rows/<list>/_shared.csv` (or the old file comes back, when git
could not follow). `python -m ops.repo_check` (`seed files`) names each such file: cut your rows into
`warehouse/seed_rows/<list>/<source_system>.csv` under a copy of the header line.

## Checks

`python -m ops.repo_check` (`configuration`) fails when a source is declared in one place and missing in
another: a `bronze:` table that is no dbt source table; a Silver model in no source's `silver:` list; a
Gold model in no `gold` list or a listed table that no model builds; a full-rebuild model without a
ceiling, or with two; a `poc_build_*` model that is not tagged or not registered, or registered twice; the shared
block lists `poc_legacy_block_models()` or `poc_legacy_activated_blocks()` put back; a value-range file not named after a declared source,
defining another macro than `poc_value_range__<source>`, or such a macro outside `macros/value_range/` or called by
a model; a registration that names no model or no
blocks, sits in the wrong file or names an unknown source; a geography CTE that is not named after its
source or whose name already exists (in `poc_map_geography.sql` or in another source's macro); a
`poc_optional_ref` table the map does not list in `optional_parents`, or one listed and not read;
a `source.csv` or `licence.csv` row missing for a source (`register`); a choice list put back on the
Pipeline `source` input.

`seed files` checks the code lists. A dbt seed: a repeated row or key, a second header line, a row with the
wrong number of fields, a last line without a line break (what a union merge can leave behind). A list kept as
rows per source, the same for each file, and: a file that is neither `_shared.csv` nor named after a declared
source; more rows in `_shared.csv` than at the split (`SEED_ROWS_SHARED` in `ops/repo_check.py`); a row of
`source`, `kpi_measure`, `kpi_v2_map` or `unit_map` in another source's file; a key on two rows across the
files; a header that differs from the other files; a folder without its model, or a model that does not read the
folder or does not name it in its config meta; a seed CSV put back next to its folder.

## Rebuilds

`warehouse/changed.py` fingerprints every model with one digest of the code all models share: everything
under `warehouse/macros/` except `macros/sources/` and `macros/value_range/`, `dimensions/` (not its README),
`dbt_project.yml` and `requirements.txt`.
A change there makes the next scheduled build rebuild every dbt model and seed once ("code changed"). Since 27
November the digest reads each file without its comments and layout (`code_text()`): in SQL and Jinja the Jinja
comments and the `--` comments go and white space outside quotes counts as one space (text inside quotes is
kept); YAML counts as its parsed document; `requirements.txt` without `#` comments. So a comment or
re-indentation of a shared macro rebuilds nothing; a file that does not read cleanly (an unclosed quote) counts
by its bytes, as before. A Jinja comment goes as Jinja renders it: white space that touches it stays (`a{# c #}b`
reads `ab`), and a `-` marker strips the white space beside it. SQL whose quotes the reading does not follow (a
`/* */` comment, a `$` outside quotes, an `E'…'` string) counts by its bytes too, so an apostrophe there cannot
turn a real change into a "comment" (#3342 review). `repo_check` (`configuration`) asserts the cases listed next to
`sql_code()`.

The files under `macros/sources/` enter the fingerprint only of the models that use them
(`source_macro_files()`): a model that calls a macro defined there gets that file, and a model that reaches
the registration macros of `macros/source_registration.sql` (`poc_observations()`, `poc_block_models()`,
`poc_activated_blocks()`, the two geography macros) gets all of them. Calls are taken from dbt's manifest and
from the text of the model and of the macros it reaches, because dbt records no dependency for a macro called
by a built name. So a new source's file rebuilds its own build stage, the geography map, the facts and
dimensions that union the blocks and what is built on them (142 of the 166 Gold models on 12 November), and
no Silver model and no seed. A macro in `macros/sources/` whose name does not start with `poc_` could
replace a dbt macro for every model, so its file counts for every model.

A list kept as rows per source is fingerprinted with its files (the model's config meta `seed_rows` names the
folder): a new or edited file rebuilds that list and what reads it, as an edited seed CSV did. A new source's
rows in `source` and `licence` rebuild `dim_source` and what is built on it; rows in `kpi_measure` or `unit_map`
rebuild the KPI and unit models and the metrics.

## Undo

Revert the pull request of D34: the choice list, `lineage.SOURCES` and the direct reads of `warehouse.yaml`
come back. Sources registered the new way in the meantime need their lines moved to the shared files first
(`repo_check` names each one). No data changes either way.

D36 alone: for each list, join the files of `warehouse/seed_rows/<list>/` into `warehouse/seeds/<list>.csv`
(one header), delete the folder and the model, and move the list's tests back to `seeds/_seeds.yml` (with
`column_types: {factor: "decimal(38,9)"}` for `unit` and every column of `unit_map` as `varchar`).
`seed_rows.rows()` then reads the seed, so its callers do not change. The tables keep their names and rows.

The move of 27 November (the 24 older Gold blocks to their sources' files, `code_text()` in the shared digest):
revert its pull request. A source registered later is unaffected; no data changes; the next build rebuilds every
model once more (the shared digest changes back).
