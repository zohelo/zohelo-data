# Warehouse

The dbt project (dbt-duckdb, R2 Data Catalog attached as `r2`): Bronze → Silver → Gold,
the dimensional model and the semantic layer.

```
dbt_project.yml, profiles.yml     project and connection (credentials from env vars)
models/bronze/<source>.yml        Bronze tables as dbt sources (built by ingestion/to_bronze); sources.yml: older sources
macros/sources/<source>.sql       a source's Gold block, activated blocks and geography codes (docs/source-registration.md)
declared.py                       the Gold list and row ceilings: warehouse.yaml plus each source's own YAML
models/silver/<source_system>/    typed, cleaned, conformed source tables
models/gold/                      conformed dimensions and facts
semantic_models/                  MetricFlow semantic models and metrics
seeds/                            seeds/ = owner-readable code lists, built into gold
seed_rows/<list>/, seed_rows.py   the code lists every source adds rows to, one CSV file per source (docs/source-registration.md, D36)
dimensions/                       dimension registry (definitions-as-code, draft; docs/model.md)
macros/, tests/                   shared macros; singular data tests
entity/, entity.py                per-entity Silver SQL applied by the Eurostat and BDL loaders; gold_*.sql: Gold per block
observations.py                   Gold observations per block, by rule (docs/gold-coverage.md)
build.py, changed.py, catalog.py  build engine: changed-only build, catalog publication
warehouse.yaml                    activities the Pipeline may run
```

Proven on 12 October 2026: dbt-duckdb reads Bronze and writes Iceberg tables to the R2
catalog (readable by DuckDB and R2 SQL). Iceberg tables cannot be renamed in the
transaction that created them, so the profile commits each statement separately; a
table rebuild is therefore not atomic. When the catalog's rate limit (HTTP 429) or an
Iceberg commit that does not yet see the build's own `__dbt_tmp` table (#2574) fails a
model, `build.py` resumes the build with `dbt retry` on fewer threads, also when a data test
failed in the same build (#2789; the test still fails the build) (`warehouse.yaml`
`rate_limit_retry_*`, `commit_retry_threads`), which also replaces tables a failed rename
left as `__dbt_backup`. After the declared waits it goes on retrying with the last wait while
each retry re-runs fewer nodes than the one before, up to `rate_limit_retry_max` retries and
`rate_limit_retry_deadline_minutes` from the start of the build step (#2618).

The scheduled build (empty filter) rebuilds only models whose inputs or code changed
(owner policy #822, `changed.py`): a model is selected when its table is missing, its
code fingerprint (SQL, config, macros, dimensions, `dbt_project.yml`, requirements; a file under
`macros/sources/` only for the models that use it, docs/source-registration.md)
differs, an upstream model is rebuilt, or a Bronze input or upstream table changed after
its last build started. The scheduled build records `zohelo.built_from_ms` and
`zohelo.build_fingerprint` as table properties of each model it wrote whose tests passed
(`pass` or `warn`: a warn-severity test such as `bdl_observations.year` not_null counts as passed);
models it did not finish or whose tests failed get none, so they are rebuilt (and tested)
next time; tests of unchanged models run when the model next rebuilds. The
recorded start is moved back 10 minutes to cover Bronze commits that become visible late.
A manual run (any filter; `fqn:*` builds everything) writes no properties, so the next
scheduled run rebuilds what it built, once. A model table changed by anything but dbt
(other than compaction) is not noticed downstream: only dbt writes Silver and Gold.

Metrics models are block-scoped (#3204, D38; kill switch `metrics_scoped` in `warehouse.yaml`). A
`poc_metrics_<kpi>` model is exactly one `poc_concept_metrics('<kpi>')` call (`repo_check`); its scope is the
Gold blocks of the KPI's sources (`seed_rows/kpi_measure`; source → block from `poc_legacy_block_models()`,
each `poc_source__<source>()` and a block's config meta `source_systems`). It is rebuilt when one of those
blocks was rebuilt in this run or after its last build, or when a parent that does not derive from a block
(`kpi`, `kpi_measure`, `unit`, `unit_map`, `poc_dim_geography`, `poc_dim_calendar`) changed; a rebuilt fact or
dimension alone no longer rebuilds all of them. `gold_coverage.coverage` is not an input (`OUTSIDE_METRICS` in
`changed.py`): it adds only non-activated rows to `poc_dim_measure`, which no metrics model reads; the data test
`tests/gold_coverage_outside_metrics.sql` fails the build if that stops being true. Its
fingerprint includes the code of the facts and dimensions above it (not the block list or the per-source macro
files), so an edit to `poc_fact_annual.sql` still rebuilds every metrics model and a new source none of the
others. A KPI with a source that maps to no block stays unscoped. The plan lists the metrics models the old rule
would have rebuilt as `scoped_skipped` (a count in the build annotation).

Incremental models (`materialized='incremental'`; today `silver.bdl_observations`, ~229 M
rows) are rebuilt incrementally when selected. `bdl_observations` keys each row by its
recipe: subgroup, Bronze file (`raw_sha256`) and `variables_key` (digest of the subgroup's
variable mapping in `bdl_variables`). Its pre-hook deletes rows whose recipe is no longer
current, then the build inserts the current recipes it lacks; the singular test
`tests/bdl_observations_current.sql` fails if Silver holds anything else
(`macros/bdl_observations.sql`). When such a model's code fingerprint changed (its own SQL or
config, or shared code: macros, dimensions, `dbt_project.yml`, requirements) or it has no
build properties, the scheduled build rebuilds it in full once (dbt var
`full_refresh_models`, listed as `changed_only.full_refresh` in the build annotation).
Bronze rows rewritten under the same `raw_sha256` (a to_bronze re-decode) are not noticed:
such a fix must also change the model's code (e.g. a line in its header comment), so the
next scheduled build rebuilds it in full; announce it in the Work log.

The five Gold facts and `poc_dim_series` are incremental per Gold block (#3204 PR B, D39;
`macros/poc_incremental.sql`). Every row carries its block model in a trailing column `gold_block`
(`poc_observations()`). The changed-only plan lists `changed_blocks`: the blocks rebuilt in the run, and the blocks
whose table changed after such a model's last build. `build.py` passes them as a dbt var, and the model reads only
those blocks. The strategy `gold_blocks` then deletes the rows of those blocks and of every block that is no
longer registered, and inserts the new rows, in one explicit transaction: one Iceberg commit. Their fingerprint
leaves out the block list and the per-source macro files, so a new source is a block change, not a code change.
The block list they were built with is the property `zohelo.build_blocks`; when it differs, the plan says "blocks
changed" and the model is rebuilt incrementally. A full refresh happens when the model's code changed, it has no
build properties or no table, or more than half of its counted rows are position deletes ("position deletes";
`total-records` in the snapshot summary still counts deleted rows). A build that is not the changed-only plan (an
explicit filter, `fqn:*`) has no `changed_blocks` var and rebuilds them in full, as before. They have no
`full_rebuild` ceiling; the grain tests (`fact_grain`, unique `series_key` in `dim_series`) catch a series that
two blocks emit.

Every other model is a full-rebuild table (the project default) and must be small (owner
policy #822: below ~1M rows). `full_rebuild` (in the source's own YAML, or `warehouse.yaml` for a shared model) gives each one a row ceiling;
`repo_check` fails when a non-incremental model has none, when a listed model is gone or
incremental, or when a ceiling above 1M names no `exception:` issue. After a prod build,
`build.py` reads the rows of each full-rebuild model it wrote (snapshot `total-records`)
and reports them as `row_ceilings` in the build annotation: above the ceiling the
annotation is a warning; above twice the ceiling the build fails.

Target `trial` writes every model to the scratch namespace `zz_dbt_trial`. Sources without
Silver models (GUS DBW, dane.gov.pl, GLEIF, MF Biała Lista) exist only in Bronze until they
are modelled; the retired pass-through copies (#835) are in git history.

Eurostat and GUS BDL observations are not dbt models. Their loaders write Silver per entity
(design #875): `warehouse/entity/<table>.sql` is applied to each dataset / subgroup right after
its Bronze file, and the result replaces that entity in Silver (`warehouse/entity.py`). dbt
sees them as sources (`silver_entities` in `models/bronze/sources.yml`) for Gold.

Gold per block (docs/gold-coverage.md, O36, O43, decision D35): `observations.py` puts every loaded Silver
block of a big source into `gold.<source>_observations`, `_series`, `_measures` and `_geography` by
rule, one file per block, rebuilt only when the block's Silver files, the Gold SQL
(`entity/gold_*.sql`) or the mapping of its geography codes changed; each block is checked for one
row per `series_key` × `date_key` before it is accepted. Built for Eurostat, GUS DBW and GUS BDL
(`entity/gold_gus_dbw_*.sql`, `entity/gold_gus_bdl_*.sql`); the Pipeline activity `gold.observations` runs it for
every source that lists it in `activities` (docs/operating.md; Eurostat; the tables of GUS DBW and GUS BDL are
declared, the activity is not authorized yet): the first load is started by hand with a filter, later
runs continue within their time budget in the order Polish regions, countries and aggregates, the rest
(O43). Dev trials write to a scratch namespace (`--trial zz_dev_<topic>`).

```sh
cd warehouse && DBT_PROFILES_DIR=. dbt build --target trial   # needs the R2 env vars
```
