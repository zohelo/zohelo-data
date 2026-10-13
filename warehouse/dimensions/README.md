# Dimension registry

Definitions-as-code for the Gold dimensional model: one YAML file per dimension. The
design is in [docs/model.md](../../docs/model.md). Status: **draft for owner review** —
nothing here is built yet and no code reads these files; dbt models will be generated
from or checked against them when Gold is modelled.

## File format

| Field | Meaning |
|---|---|
| `name` | Dimension name; the Gold table is `gold.dim_<name>` |
| `kind` | `conformed` (shared, standard-based), `entity` (register of real-world objects, SCD2) or `source_specific` (bridge next to one source's fact) |
| `status` | `draft` until the owner accepts it |
| `description` | One or two sentences |
| `grain` | What one row is |
| `key` | `surrogate` (Gold key column) and `natural` (columns that identify a row) |
| `identifiers` | Official/natural identifiers (schemes) carried by the dimension; entity dimensions also list them in the identifier cross-reference (`identifier_scheme.yaml`) |
| `standards` | `name`, `owner`, `version`, `url`, `verified` (`true` only when checked against the publisher) |
| `hierarchy` | Ordered levels, top first |
| `versioning` | `static`, `versioned_codelist` (members per version + correspondences), `scd2` (valid_from/valid_to per change) or `vintage` (members per scheme vintage) |
| `special_members` | `_T` total, `_Z` not applicable, and others (e.g. `_U` unknown); facts never carry NULL keys |
| `attributes` | `name`, `description`, `source` (who contributes the value: a standard or a `source_system`) |
| `used_by.platform` | The 10 platform sources (`source_system` id, inventory id) and how each expresses the dimension |
| `used_by.inventory` | Candidate sources from the 182-row source inventory (by inventory id), from keyword matching — a lower bound, not verified |
| `mapping_notes` | How source codes map to members |
| `open_questions` | Unresolved points; anything marked `unverified` must be checked before use |

## Promotion rule

A source-specific dimension lives in a bridge next to its fact (`source_specific`,
see `breakdown.yaml`). When a **second source** carries the same concept, the
dimension is **promoted to conformed**: pick the international standard if one exists
(else the better-documented source's code list), add a mapping table
`map_<dimension>__<source_system>` (source code → conformed member, relationship type
`exact|broader|narrower|partial`), keep the source code on the fact for lineage, and
record the promotion in this registry (`kind` change + `used_by`).

## Platform sources ↔ inventory ids

| source_system | Inventory id | Note |
|---|---|---|
| eurostat | GL-001 | Comext (GL-002) out of scope |
| gus_bdl | PLS-001 | |
| gus_dbw | PLS-002 | |
| world_bank_wdi | GL-005 | |
| nbp | PLG-001 | exchange rates and gold only |
| gleif | CD-015 | |
| mf_biala_lista | PLG-011 | flat file (hashes), not the lookup API |
| gugik_prg | PL-ENV-001 | |
| gus_teryt | PLS-007 | |
| opendata_org | — | not in the inventory; legacy config cites `GL-006` (World Bank GEM), which is wrong |

## Platform sources × dimensions

From the `used_by.platform` entries (several are unverified against Bronze; see each file).
Every statistical source also uses `calendar` through the period→day bridge.

| source_system | Fact kind | Dimensions |
|---|---|---|
| eurostat | statistical observation | geography, period, frequency, unit, price_base, seasonal_adjustment, observation_status, sex, age, activity, product, trade_product, consumption_purpose, education, occupation, institutional_sector, government_function, currency, urbanisation, disease, breakdown, indicator, dataset, domain, source |
| gus_bdl | statistical observation | geography (KTS/BDL units), period, frequency, unit, observation_status, breakdown (n1..n5), indicator (variable), domain (K/G/P), dataset, source; parsed from labels: sex, age, activity, education, … |
| gus_dbw | statistical observation | geography (no TERYT), period, frequency, unit, observation_status, breakdown (positional dims), indicator, domain, dataset, source; dictionary-dependent classifications |
| world_bank_wdi | statistical observation | geography (WB codes, aggregates), period (year), frequency, unit, price_base, observation_status, indicator (series), domain (topic), dataset, source; sex/age/education only inside indicator names |
| nbp | statistical observation | calendar/period (day), currency, unit, breakdown (table, rate type), commodity (gold), indicator, dataset, source |
| gleif | entity snapshot + relationship | organisation, identifier_scheme, address, geography (country/region), calendar; security/market only via mapping files (not in Bronze) |
| mf_biala_lista | entity snapshot (hashes) | organisation (verification of known NIP + account only), identifier_scheme, calendar |
| gugik_prg | entity snapshot (geometry) | spatial_feature, geography (TERYT), unit (area), calendar |
| gus_teryt | reference data | geography (TERC, SIMC), address (ULIC), identifier_scheme, calendar |
| opendata_org | entity snapshot + relationship | organisation, person, address, geography, activity, identifier_scheme (all unverified) |

## Coverage (182 inventory sources)

Keyword match of the inventory's `entities`, `keys_and_links`, `granularity`,
`modeling_notes` and `domains` columns, aggregates excluded for entity dimensions.
Counts are lower bounds: the inventory is product-level and rarely lists every
breakdown (e.g. 25 sources publish `person-aggregate` data, which usually implies
sex/age, but only those naming sex/age are counted). Calendar, period, frequency, unit,
observation status, indicator, source and dataset apply to every statistical source.

| Dimension | Kind | Sources | Inventory ids |
|---|---|---:|---|
| geography (reference area) | conformed | 107 | GL-001…040; PLS-001…035 except 008, 012, 013, 020, 023, 024, 028; PLG-001…010, 020, 023, 029, 031…033; PL-ENV-001…007, 015, 017, 018, 022, 023, 025, 029, 034; CD-002, 004, 031, 033, 034; EX-001, 003, 004 |
| geography (partner/counterpart) | conformed | 11 | GL-002, 013, 014, 015, 019, 020, 022, 029; PLG-002, 004, 005 |
| sex | conformed | 11 | GL-001, 023, 024, 026, 027, 028, 031, 033, 038, 039; PLS-012 |
| age | conformed | 6 | GL-001, 024, 026, 033, 039; PLS-012 |
| activity (NACE/PKD/ISIC) | conformed | 6 | GL-001, 040; PLS-027, 028; PLG-014; CD-021 |
| institutional_sector | conformed | 12 | GL-015, 016, 034; PLG-002…006, 009, 016…018 |
| trade_product (HS/CN/SITC) | conformed | 4 | GL-002, 014, 019, 020 |
| commodity (market commodities) | source_specific | 7 | GL-022; PLS-032, 033; PLG-001; CD-002, 009, 010 |
| currency | conformed | 10 | GL-013, 015, 016; PLS-032, 033; PLG-001, 005, 008; CD-002, 009 |
| education (ISCED) | conformed | 4 | GL-026; PLS-021, 022, 024 |
| occupation (ISCO/KZiS) | conformed | 2 | PLS-029, 030 |
| government_function (COFOG / budget classification) | conformed | 2 | PLG-007, 010 |
| disease (ICD) | conformed | 4 | GL-024, 025; PLS-012, 019 |
| organisation | entity | 46 | GL-010, 032; PLS-008, 014, 015, 020, 021; PLG-007, 011…023, 028, 034; PL-ENV-025, 034; CD-001, 003…008, 012…016, 021, 024, 028…030, 032; EX-001, 002, 004 |
| person (non-aggregate) | entity | 10 | PLS-023; PLG-015, 019, 024, 025, 029; CD-014, 016, 028, 030 |
| security | entity | 15 | PLG-008; CD-001…009, 011…013, 015, 026 |
| market (trading venue) | entity | 6 | CD-001, 002, 005, 009, 010, 015 |
| institution | entity | 10 | GL-004, 032; PLS-014, 015, 020…023; PLG-031, 032 |
| political_actor | entity | 3 | PLG-024, 025, 029 |
| document (acts, filings, notices, decisions) | entity | 23 | GL-032; PLG-020…028, 034; PL-ENV-017, 033, 035; CD-003, 006…008, 012…014, 016, 030 |
| transport_stop | entity | 8 | PL-ENV-025…032 |
| transport_route | entity | 7 | PL-ENV-022, 023, 028…032 |
| monitoring_site | entity | 8 | GL-036, 037; PL-ENV-008…012, 014 |
| facility | entity | 3 | GL-037; PL-ENV-015, 021 (+ PL-ENV-019 generation units, aggregated) |
| address | entity | 7 | PLS-007, 015, 021, 026; PL-ENV-007, 033, 034 |
| spatial_feature | entity | 13 | PL-ENV-001…006, 011…013, 016…018; CD-027 |

Identifier keys named in `keys_and_links` / `modeling_notes` (lower bound):
TERYT/TERC/SIMC/ULIC 13 · REGON 10 · ISIN 8 · NIP 7 · LEI 7 · KRS 6 · SDMX full key 5 ·
ticker/symbol 5 · MIC 4 · GTFS ids 4 · NUTS 3 · ISO country 3 · CIK 2 · RSPO 2 ·
DOI/ORCID/ROR 2 · ELI 2 · ICD 2 · Wikidata Q-id 2 · BIC 1 · KTS/BDL unit 1.

Not yet defined (single-source or low priority; promote when a source is admitted):
patents and IP rights (EX-002, EX-004, GL-032), scholarly works (CD-028, CD-029),
social-media accounts and content (CD-017…024), OSM/Wikidata objects (used as
identifier cross-reference sources), vehicles and listings (PL-ENV-024, CD-032…034),
health products and NFZ service catalogues (PLS-012…014).
