# MRPiPS social assistance statistics (`mrpips_social_assistance`)

**Built (12 Nov 2026, register PLS-031, #2910).** gov.pl lists a page "Statystyka za rok <year>" per year (from
https://www.gov.pl/web/rodzina/statystyki-pomocy-spolecznej, 1999-2025) with the MRPiPS-03, -05 and -06 reports as
old Excel (.xls) attachments (2021 as XLSX). Only MRPiPS-03 is landed (a "korekta" attachment replaces the original),
years from 2019; entity = one year (`mrpips03_<year>`), version = download time, landed attachment URLs are not
downloaded again, restart unit one file (`ingestion/to_landing/mrpips_social_assistance/`). Bronze
`bronze.mrpips_social_assistance_cells` reads the .xls with xlrd (`requirements.txt`) into the shared cell shape.
Silver `silver.mrpips_social_assistance_tables` (macro `gus_tables.sql`, section DZIAŁ 2, DZIAŁ 3 until 2020, found by
header): persons granted a benefit by decision, families and persons in families by voivodeship, "RAZEM" = Poland.
Gold block `social_assistance` (annual), registered in `warehouse/macros/sources/mrpips_social_assistance.sql`; KPI
`social_assistance_recipients` (draft). Monthly check. Licence: gov.pl terms (official materials free to use).
