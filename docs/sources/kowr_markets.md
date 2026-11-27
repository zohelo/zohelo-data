# KOWR weekly grain market reports (`kowr_markets`)

**Built (Nov 2026, register PLS-033, #457).** KOWR (Biuro Analiz i Strategii) publishes its weekly market reports
only as PDF attachments of gov.pl pages: https://www.gov.pl/web/kowr/raporty-tygodniowe lists the latest four weeks of
the grain, meat, dairy and combined reports, and the archive https://www.gov.pl/web/kowr/Rynek-zboz lists every grain
report since the week 20-26.03.2023 (175 on 26 Nov 2026, the newest about five weeks back; Dev probes 45826667517 and
45895465874). There is no spreadsheet and no dane.gov.pl dataset. Only the grain report is built.

**Landing** (`ingestion/to_landing/kowr_markets/`). Entity = one report week, `grain_<yyyymmdd>` (the Monday the link
text names, "Raport o sytuacji na rynku zbóż w okresie 12.10.2026 r. – 18.10.2026 r."); version = the server's
Last-Modified. Weeks not landed yet are downloaded once; the weeks of raporty-tygodniowe are read again on each run, so
a replaced file lands as a new version (unchanged bytes are skipped). Two files for one week on one page, or a week
not starting on a Monday, fail the run; a page without grain reports is an error. One request at a time, 5 s between
attachment requests, time-budgeted (the backfill of about 180 files takes some 15 minutes); restart unit one file.

**Bronze** `bronze.kowr_markets_cells` (`ingestion/to_bronze/kowr_markets/`): the text layer (pypdf, plain mode) of
three tables, each found by its title line, one row per cell with the period and the stated source as printed:

| Table (title) | Content | Stated source | Silver |
|---|---|---|---|
| Eksportowe ceny zbóż na rynkach zagranicznych (na bazie FOB) | FOB export prices in EUR/t of milling wheat (HRW and SRW US Gulf, Rouen, Black Sea), feed barley (Black Sea, Rouen), maize (US Gulf, Black Sea, Bordeaux), as of one quotation date per report | "KE, EBC, NBP" | `kowr_markets_fob_prices` |
| Ceny zakupu zbóż w krajach UE | purchase prices in EUR/t of wheat, barley and maize in the 27 member states and the EU average, per report week | "Komisji Europejskiej" | `kowr_markets_eu_prices` |
| Ceny wybranych zbóż na krajowych giełdach towarowych | sale offers in zł/t (minimum, maximum, average) on Polish commodity exchanges, per report week | "krajowych giełd towarowych" | `kowr_markets_exchange_offers` |

A file is rejected into `bronze.kowr_markets_rejected_files`, with the reason, when a title is missing or printed
twice, a header, rate or source line is missing, a market, country or commodity is unknown or repeated, a row has the
wrong number of values, a value is not a number or "-" / "####" / "0" as printed, a period is not one or two weeks
(Monday to Sunday) ending between four weeks before and one week after the report week's Sunday (the EU and exchange
tables often show the week before the report's), more than two EU columns of a block disagree with the EUR price times the ECB rate printed under the table
(shifted columns), or an exchange average lies outside its minimum and maximum. The commodity of each EU block is read
from the text positions (its heading row lies above it), because the text layer prints the headings in no fixed order.
A single FOB row or EU cell whose converted price disagrees with its EUR price (FOB: NBP rate, ±1 % + 1 zł; EU: ECB
rate, ±1 % + 2 zł; Polska is not checked, its PLN price is ZSRIR's), or an exchange row whose average lies outside its
minimum and maximum, is kept with a `kontrola` cell, and Silver loads no price from it (which of the two is wrong cannot
be told; 32 feed-barley rows from July 2024). Rejected in Dev trial 45920665418 (27 Nov 2026):
30 of 179 reports. An EU price row with 26 or 27 values for 28 columns (14: a cell left blank, so the columns cannot
be aligned), a table printed as an image or left out (12: no FOB, EU or exchange title, no country list, no FOB
header), an EU table without its period (2), an exchange period with the wrong year ("20-26.01.2024" in the report of
20-26.01.2025) and an EU block whose PLN prices disagree in 11 columns (one each). Their weeks are missing in every
table; the reports themselves stay in Landing and the Archive.

**Silver.** Only the published prices: the changes in % and the converted PLN prices are KOWR's derivations and are
not kept. "-" (brak danych) and, in the EU table, "0" (printed for a member state without a report) keep the value
null, never 0, as do the rows marked `kontrola` (`value_flag`). The FOB table is dated by quotation day (a weekday after the report week); the other two by the period they print
(one week, once two), and a period printed by two reports keeps the newer report.

**Gold.** Block `fob_export_prices` (`poc_build_kowr_markets`, frequency D at the quotation date, one quotation a
week; EUR/t as published, unit `eur_per_tonne`); areas US and FR, the Black Sea ports stay `_X` (`BLACK_SEA`, no area
in `dim_geography`). The two weekly tables stay in Silver: curated Gold has no weekly fact model, as for the weekly
tables of `mrirw_zsrir`. Draft KPI `wheat_export_price_fob` (Rouen; no metrics model and no `kpi_v2_map` row yet).

**Not read.** The ZSRIR weekly price table and summary box of the grain report (MRiRW ZSRIR, loaded as
`mrirw_zsrir`), its charts, its narrative (MATIF futures from Kaack Terminhandel GmbH, GUS monthly prices and
purchases), and the meat, dairy and combined reports.

**Licence.** gov.pl terms (https://www.gov.pl/web/gov/prawa-autorskie): text CC BY-SA 4.0, official materials free to
use; the files carry no licence of their own. The tables are KOWR's compilations from third-party data (European
Commission, ECB, NBP, the commodity exchanges), whose terms stay with their owners; each row keeps the stated source.
`licence_key = unresolved` (internal, licence gate): the three tables loaded are all stated as compiled from others' data, whose reuse terms were not read, so the gov.pl terms are not claimed for them. **Schedule:** Tuesday 09:25 UTC (the grain report of week N appears on Monday of
week N+2); the run ends with Bronze, Silver and Gold are built on Sunday (O46).
