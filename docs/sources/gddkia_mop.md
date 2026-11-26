# GDDKiA rest areas, MOP table (`gddkia_mop`)

**Built (register PL-ENV-023, #2995): Landing → Silver scheduled, Gold block and entity table registered.** GDDKiA
publishes on https://www.gov.pl/web/gddkia/wykaz-parkingow-i-mop a monthly workbook "Tabela MOP istniejące": the
existing rest areas (miejsca obsługi podróżnych, MOP) and parkings on national roads, one row each (Lp., Oddział,
Województwo, Zarządca, Miejscowość, rest-area name, current and target category).

- **Route:** the page is the entry (the attachment uuid changes per edition); the attachment link whose text starts
  "Tabela MOP" and names "istniejące" is one edition, entity `mop_<yyyy>_<mm>` from the month in the link text
  ("Tabela MOP - wrzesień 2026 r."). No such link, a link without a readable month, or two links for one month stop
  the run. Version = Last-Modified; a new URL lands only when its bytes differ; restart unit: one file. Checked
  weekly (Thursday 10:35 UTC); the September 2026 edition appeared on 26 October.
- **History:** the page links only the current edition, so history starts with the first edition landed and grows
  by one month per edition; each edition stays its own entity (a corrected file replaces its month).
- **Bronze** `bronze.gddkia_mop_cells`: every non-empty cell of the workbook (shared cell shape); a file without the
  header row (column 1 "Lp." with "Województwo" and "Obecna kategoria") is rejected.
- **Silver** `silver.gddkia_mop_rest_areas`: one row per rest area of each edition, grain edition × Lp. (the
  producer's row number; the table has no other id and Lp. is not kept across editions); columns found by header
  text. The manager (Zarządca) is kept as text only for GDDKiA, "Prywatny" and names with a company form; any other
  value (possibly a sole trader's personal name) is `manager_kind = other` without the name.
- **Gold:** `gold.build_gddkia_mop`, block `rest_areas`: `gddkia_mop:rest_areas:facilities`, the count of listed
  rest areas per edition month (frequency M), Poland and the 16 voivodeships (printed names matched to the TERYT
  names of dim_geography), breakdown current category as published and `_T`. `gold.gddkia_mop_rest_area`: entity
  table of the newest edition keyed by Lp. Draft KPI `road_rest_areas` (no metrics model yet).
- **Left out:** the road-programme status PDF (no PDF text reader on the platform yet), its contractor and legend
  PDFs, and the road-obstruction XML feed on archiwum.gddkia.gov.pl (a live operational feed; the host did not
  answer runners); road numbers and chainage are not in the MOP table.
- **Licence:** unresolved (register PL-ENV-023): the page states no licence for the file; the gov.pl footer licenses
  text content (CC BY-SA 4.0), not attached data files. Internal only (licence gate).
