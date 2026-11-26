# KG PSP fire-service interventions (`kgpsp_interventions`)

**Built (register EX-003, #2970): Landing → Silver scheduled, Gold blocks registered.** The Headquarters of the State
Fire Service publishes on https://www.gov.pl/web/kgpsp/interwencje-psp, per year since 2016, a ZIP of standard
summary tables ("Tabele stałych Zestawień-ST SWD PSP", about 8-12 MB) and a ZIP of incident-report data ("zestawienie
danych z informacji ze zdarzeń", one CSV of about 0.5 M records a year).

- **Route:** the page is the entry (attachment uuids change when a file is replaced); every attachment link whose
  text is a year and a `.zip` file name and does not name the incident reports is one entity (`tabele_<year>`); a
  year with two such links or a gap between the first and the newest year stops the run. Version = Last-Modified;
  restart unit: one file. Checked monthly (12th, 06:50 UTC); a year's tables appear in March of the next year.
- **Bronze** `bronze.kgpsp_interventions_cells`: every non-empty cell of two workbooks per ZIP, found by file name:
  table 1 (incidents by type and size) and table 10 (deaths and injured in fires and local hazards, counts only).
  A ZIP without exactly one .xlsx of each is rejected.
- **Layouts:** 2016-2022 one workbook per table and level (voivodeship `1_2016.xlsx`, powiat `1_1_…`, gmina `1_2_…`;
  the voivodeship one is read); from 2023 one workbook per table with a row per gmina (TERYT WWPPGG), no totals.
- **Silver** `silver.kgpsp_interventions_counts` (year × table × area × measure, header text read, never by
  position); **Gold** `gold.build_kgpsp_interventions`, blocks `incidents` and `casualties`, Poland and voivodeships;
  from 2023 the voivodeship and Poland values are sums of the published gmina rows, except where a city with districts
  is printed without all its TERYT codes, the city row and every district (Wrocław 0264: 6 codes, Łódź 1061: 6,
  Kraków 1261: 5, Warszawa 1465: 19, Poznań 3064: 6; macro `poc_kgpsp_interventions_incomplete_cities`, shared by the
  build and the warning test `kgpsp_interventions_city_districts`): the 2025 tables print each of these cities as one
  row, so 2025 has no Poland value and none for those five voivodeships; 2023 and 2024 have every code (Dev trial
  45781372123). Layout break 2022/2023, checked: the 2022 gmina copies (`1_2_2022.xlsx`, `10_2_2022.xlsx`; 2,479
  gmina rows) summed per voivodeship equal the published 2022 voivodeship and Poland rows in every cell (table 1:
  272 of 272, table 10: 306 of 306; Dev trial 45781372123), so summing gmina rows reproduces the published totals.
  It cannot show whether the new gmina tables of 2023 on leave incidents out; the fall in 2023 (Poland, all
  incidents: 608,818 in 2022, 486,055 in 2023) is as the producer's gmina rows give it. The 2024 tables print three
  TERYT codes twice, for two gminas each; Silver keeps the gmina name in the grain. Draft KPIs: fire_service_fires,
  fire_service_local_hazards, fire_service_false_alarms.
- **Left out:** the other 31 numbered tables and tables A-C (causes, objects, times, units, equipment, losses), the
  powiat and gmina copies of 2016-2022, gmina rows in Gold, the incident-report archives (size; record-level location
  fields, TERYT down to locality and road chainage, to be checked first) and the casualty summary page.
- **Licence:** CC BY 4.0, stated on the page ("Dane statystyczne KG PSP, zostały zamieszczone na portalu dane.gov.pl
  Rodzaj licencji: CC BY 4.0").

Access: on 16 Nov 2026 www.gov.pl and dane.gov.pl did not answer TCP connections from GitHub runners (Dev runs
43798868238 to 43815599563); from 26 Nov they answer again (Dev probes 45745935044, 45746735646). The extractor keeps
its bounded retries; a refused run is a failed run, never a workaround.
