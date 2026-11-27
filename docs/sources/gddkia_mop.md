# GDDKiA rest areas, MOP table (`gddkia_mop`)

**Built (register PL-ENV-023, #2995): Landing → Silver scheduled, Gold block and entity table registered.** GDDKiA
publishes on https://www.gov.pl/web/gddkia/wykaz-parkingow-i-mop a monthly workbook "Tabela MOP istniejące": the
existing rest areas (miejsca obsługi podróżnych, MOP) and parkings on national roads, one row each, 35 columns
(September 2026: 424 rows): Lp., Oddział, Województwo, Zarządca, Miejscowość, rest-area name, current and target
category, x, y, road class, road number, chainage, direction, parking places for cars, lorries and buses, and 17
amenity columns (toilets, fuel, food, beds, EV charging, security, fence, CCTV, lighting, dangerous goods, car wash,
workshop, waste disposal, shower, hydrogen, CNG, LNG, snow ramps).

- **Route:** the page is the entry (the attachment uuid changes per edition); the attachment link whose text starts
  "Tabela MOP" and names "istniejące" is one edition, entity `mop_<yyyy>_<mm>` from the month in the link text
  ("Tabela MOP - wrzesień 2026 r."). No such link, a link without a readable month, or two links for one month stop
  the run. Version = Last-Modified when the server's header is read, else download time (the Dev trial got download
  time: gov.pl sends lower-case header names and the shared download keeps them in a plain dict); a new URL lands
  only when its bytes differ; restart unit: one file. Checked weekly (Thursday 10:35 UTC); the September 2026
  edition appeared on 26 October.
- **History:** the page links only the current edition, so history starts with the first edition landed and grows
  by one month per edition; each edition stays its own entity (a corrected file replaces its month).
- **Bronze** `bronze.gddkia_mop_cells`: every non-empty cell of the workbook (shared cell shape); a file without the
  header row (column 1 "Lp." with "Województwo" and "Obecna kategoria") is rejected.
- **Silver** `silver.gddkia_mop_rest_areas`: one row per rest area of each edition, grain edition × Lp. (the
  producer's row number; the table has no other id and Lp. is not kept across editions); columns found by header
  text; road class, number, chainage, direction and x/y as published text. The manager (Zarządca) is kept as text
  only for GDDKiA, "Prywatny" and names with a company form; any other value (possibly a sole trader's personal
  name) is `manager_kind = other` without the name.
- **Gold:** `gold.build_gddkia_mop`, block `rest_areas`: `gddkia_mop:rest_areas:facilities`, the count of listed
  rest areas per edition month (frequency M), Poland and the 16 voivodeships (printed names matched to the TERYT
  names of dim_geography), breakdown current category as published and `_T`. `gold.gddkia_mop_rest_area`: entity
  table of the newest edition keyed by Lp. Draft KPI `road_rest_areas` (no metrics model yet). Dev trial 45829412455
  (edition 2026-09): 424 rest areas in Silver and the entity table, 63 Gold rows, Poland 424 (MOP funkcja podstawowa
  189, komercyjna 159, Parking prywatny 70, Parking 6), voivodeships add up to Poland, no unmatched area. Lp. is the
  only key: 13 combinations of branch, locality and name repeat (31 rows, e.g. private parkings named "OOUR").
- **Left out:** the parking-place and the 17 amenity columns of the MOP table (to be profiled and typed first), the
  road-programme status PDF (no PDF text reader on the platform yet), its contractor and legend PDFs, and the
  road-obstruction XML feed on archiwum.gddkia.gov.pl (a live operational feed; the host did not answer runners).
- **Licence:** unresolved (register PL-ENV-023): the page states no licence for the file; the gov.pl footer licenses
  text content (CC BY-SA 4.0), not attached data files. Internal only (licence gate).
