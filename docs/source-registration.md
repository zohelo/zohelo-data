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
| `warehouse/seeds/source.csv`, `licence.csv`, `kpi.csv`, `kpi_measure.csv`, `kpi_v2_map.csv`, `unit.csv`, `unit_map.csv`, `unit_decimals.csv` | `warehouse/seed_rows/<list>/<source>.csv` (the list's header, then the source's rows; D36, below) | the model `warehouse/models/gold/seed_rows/<list>.sql` reads every CSV file of the folder; Python reads `warehouse/seed_rows.py` `rows()` |

`warehouse.yaml` keeps what belongs to no single source: conformed dimensions, facts, seeds, KPI metrics
that read several sources, and their ceilings.

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
  CTEs named `<source_system>_<x>`. It is placed after `geo` and `nuts_pl`, so it may read those two and
  any `ref()`, not the CTEs of older sources. Prefer static lists and `geo` (iso2, iso3, wb_code) over the
  source's own Silver table, so the shared model never waits for a new source.
- Registered sources come after the older ones, in name order: the compiled SQL depends on which sources
  exist, never on the order pull requests were merged.

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
- **Lines of sources registered before D34** stay where they are. Moving them is mechanical (cut the block
  macros, the list line, the CTE and the branch into `macros/sources/<source>.sql`; a trial move of WHO
  changed only the position of its branches in the compiled SQL) but reorders the unions, so the compiled
  SQL of the facts changes once. Do it in one pull request when no source pull request is open.

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
ceiling, or with two; a `poc_build_*` model that is not tagged, or neither registered nor in the old list,
or both; a source with a block in the old list and a registration; a registration that names no model or no
blocks, sits in the wrong file or names an unknown source; a geography CTE that is not named after its
source or whose name already exists (in `poc_map_geography.sql` or in another source's macro);
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
under `warehouse/macros/` except `macros/sources/`, `dimensions/`, `dbt_project.yml` and `requirements.txt`.
A change there makes the next scheduled build rebuild every dbt model and seed once ("code changed"); so does
a Gold pull request of a source that still adds lines to `macros/poc_blocks.sql`.

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
