# GUS Dziedzinowe Bazy Wiedzy (DBW) (`gus_dbw`)

**Extraction (24 Oct 2026).** The DBW website publishes every indicator as bulk ZIP files
(one per cross-section × presentation × periodicity, some also per year), each with one
semicolon CSV of ids (`id_zmienna;id_przekroj;id_wymiar_1;id_pozycja_1;…;id_okres;
id_sposob_prezentacji_miara;id_daty;id_brak_wartosci;id_tajnosci;id_flaga;wartosc;precyzja`).
The indicator listing (`getAggregatesById`) gives each file with its last-update date, so
entity = file stem (e.g. `1000_156_93_4_pl`), version = that date, and a daily run lands
only files whose date is newer than Landing (`ingestion/to_landing/gus_dbw/extract.py`).
Each ZIP also holds `<id>_Slowniki.csv`, the labels of every id it uses, loaded as `bronze.dbw_dictionary_items` (parsed line by line: GUS leaves quotes inside the descriptions undoubled). No account, no key, no VPN. Bronze keeps the CSV as text; Silver
(`warehouse/entity/dbw_observations.sql`) types the ids and the value (decimal comma).
Labels of the ids come from the DBW dictionaries, loaded later into Gold. The old
pipeline's `01_landing/gus_dbw/native/` and `_control/` stay as they are (owner, 21 Oct).

**Gold per block (docs/gold-coverage.md, D35; #2983).** `warehouse/observations.py --source gus_dbw` builds
`gold.gus_dbw_observations`, `_series`, `_measures` and `_geography` with `warehouse/entity/gold_gus_dbw_*.sql`.
The Pipeline activity `gold.observations` is authorized in `ingestion/sources/gus_dbw.yaml` (#3287): the
first load is started by hand with a filter (`*` or block ids), and the platform's weekly run continues it and
rebuilds only changed blocks. Every DBW block is Polish, so the load order is one group, smallest blocks first.

- **Block:** indicator × cross-section × presentation × periodicity, the file id without language and
  year. Most yearly files end in the year (`581_57_93_4_2020_pl`); 570 are named
  `<indicator>_<cross-section>_<presentation>_<year>_282_pl` (282 is the period "rok"), and their block is
  `<indicator>_<cross-section>_<presentation>_282`. A block is read with the dictionary files of its Silver
  files; a file without its dictionary rejects the block.
- **Periods:** DBW names a period `<cycle> - <kind of data> - <name>`. Annual, half-yearly, quarterly and
  monthly data are placed on the first day of the period in the year `id_daty` (A, S, Q, M). Cumulative
  periods, three-year data and non-calendar years are not loaded (open decision 5 of gold-coverage.md).
- **Geography:** DBW has names, not codes. The source code is `<NAME> #<position id>`; the name is looked up
  in `gold.map_geography` (Poland, voivodeships, NUTS macroregions, regions and subregions); powiaty,
  gminas and countries stay `_X` with the code kept; a block without a territorial dimension is `_Z`.
- **NUTS twins:** a cross-section that lists a voivodeship and the NUTS region that is the same area
  ("DOLNOŚLĄSKIE", "REGION DOLNOŚLĄSKIE") gives two rows for one Gold area. The twin's row is left out
  only when the voivodeship's row has the same value; otherwise both stay and the block is rejected on
  the grain.
- **Signs:** see gold-coverage.md decision 5. A 0 with a no-value sign is never a value.
