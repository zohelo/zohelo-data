# ZUS statistical portal (`zus_psz`)

**Route (12 Nov 2026, #2461).** Portal Statystyczny ZUS (psz.zus.pl, register PLS-009) has no API: its category
pages link one XLSX per topic table and period under `/documents/493365/<folder>/<name>.xlsx/<uuid>`, and the folder
id stays the same for a table across years. `ingestion/to_landing/zus_psz/` reads six category pages (one request
each, 2 s apart), keeps the links of a curated list of folders (`FAMILIES`), reads the period from the file name and
lands each file through the shared `bulk_file.load_file`. **Entity** = family × period (`pensioners_dec_2025`,
`absence_voiv_2026_q3`); **version** = Last-Modified, written only when the SHA-256 changed. Each run lands missing
periods and re-downloads the two newest of each family (corrections, the current year's monthly files). **Restart
unit:** one file. English copies (`EN…`) and `.xls` years are not landed.

Families: `pensioners_dec` (old-age pensioners in December by age and sex, 2018–) and `pension_amount_dec` (average
old-age pension in December by sex, 2018–), Poland; `insured_pension` (pension insurance, by age and sex),
`insured_sickness_voiv` (sickness insurance, Poland and voivodeships), `payers_voiv` (active contribution payers by
voivodeship and payer type) and `absence_voiv` (sick-leave days and certificates by voivodeship), quarterly;
`insured_health` (health insurance, monthly, 2012–) and `foreign_insured` (foreign citizens in pension insurance, all
and Ukrainian, monthly, 2020–). The portal lists the quarterly tables for the current year only, so their history
starts with the first landing and is kept by `keep_latest: 1` per entity; the Archive is then the only copy. ZUS
publishes no voivodeship pension table after 2011. Licence `unresolved`: ZUS's re-use terms cover information published
in its BIP and on dane.gov.pl (or passed on request), not psz.zus.pl, which states no terms of its own. The source stays
internal (licence gate) until the same tables are landed from dane.gov.pl or ZUS confirms in writing that psz.zus.pl
falls under its terms (owner question).

**Bronze to Gold.** `bronze.zus_psz_cells`: one row per cell (`xlsx_cells`), latest version per entity; a workbook
without its family's header row is rejected with the reason. Silver `silver.zus_psz_observations`: one row per family
× period × area × sex × breakdown × measure; voivodeships matched by their official names to TERYT codes ('00' =
Poland); `tests/zus_psz_voivodeships.sql` fails when a voivodeship table lacks one of the 17 areas. Gold
`poc_build_zus_psz.sql`, one block per family, one frequency per measure (A, Q or M). KPIs (draft): `zus_pension_avg`
(December average, next to GUS BDL), `zus_insured_persons`, `foreign_insured_persons`, `sick_leave_days`. Schedule:
Thursdays 07:25 UTC; refresh `full` (D32 + #2461).
