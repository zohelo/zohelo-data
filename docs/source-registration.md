# Source registration: a source's lines live in the source's own files

Decision D34 (12 November 2026). Until now every new source appended lines to the same shared files, so two
source pull requests always conflicted and were merged one at a time or as hand-built merge trains. The rule
now: **adding a source touches only files that belong to that source.** The shared files keep what they
held on 12 November (nothing was moved, so open pull requests still merge) and take no new source lines.

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
- **Seeds** (`source.csv`, `licence.csv`, `kpi.csv`, `kpi_measure.csv`, `kpi_v2_map.csv`, `unit.csv`,
  `unit_decimals.csv`) stay one file each for now. dbt builds one table per CSV file; per-source fragments
  would need either a generation step before every `dbt` command, `repo_check` and `gold.publish` (all read
  the CSVs directly), or one seed per fragment plus a union model whose refs cannot be discovered the way
  macros are. Neither is clean. Two candidates, in this order:
  1. `warehouse/seeds/*.csv merge=union` in `.gitattributes`: concurrent appended rows merge without a
     conflict in local merges (the merge trains). Needs a trial of whether GitHub's merge button honours it,
     and a duplicate-key check per seed in `repo_check`, because a union merge keeps both versions of a row
     two pull requests edited.
  2. Fragments `warehouse/seed_rows/<seed>/<source>.csv` assembled into `warehouse/seeds/<seed>.csv` by one
     function that `build.py`, `changed.manifest`, `repo_check` and `publish` call first; the assembled
     files leave git.
- **`kpi.csv`-family rows and KPI metrics models** belong to a KPI, not to a source; a KPI pull request
  still adds its metrics model to `warehouse.yaml`. The same fragment idea applies and is left for the
  seeds step.
- **Lines of sources registered before D34** stay where they are. Moving them is mechanical (cut the block
  macros, the list line, the CTE and the branch into `macros/sources/<source>.sql`; a trial move of WHO
  changed only the position of its branches in the compiled SQL) but reorders the unions, so the compiled
  SQL of the facts changes once. Do it in one pull request when no source pull request is open.

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

## Undo

Revert the pull request of D34: the choice list, `lineage.SOURCES` and the direct reads of `warehouse.yaml`
come back. Sources registered the new way in the meantime need their lines moved to the shared files first
(`repo_check` names each one). No data changes either way.
