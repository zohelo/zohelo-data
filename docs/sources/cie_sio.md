# SIO archival education statistics (`cie_sio`)

**Built (register PLS-022, #3130).** Centrum Informatyczne Edukacji keeps a closed archive of SIO compilations
(reference date 30 September, 2010-2018): https://cie.gov.pl/archiwalne-dane-statystyczne/ links six topic pages
whose plain HTML links 94 workbooks (64 .xls, 30 .xlsx) under `/content/uploads/2020/09/`. Landed
(`ingestion/to_landing/cie_sio/`, `SELECTION`): the nine graduates workbooks (2010-2018), the tidy schools-and-pupils
workbook of 2018/2019 and the newest teachers workbook (2006-2018; the five older teachers workbooks repeat its
series). Entity = one workbook (file name, lower case), version = Last-Modified; a landed workbook is downloaded again
only when its Last-Modified is newer, and lands only when its SHA-256 changed. Restart unit: one file; one request at
a time, 2 s apart. `landing.inspect` warns when the archive's link count (94) or the selection (11) changes.

Bronze `bronze.cie_sio_cells`: every non-empty cell of every sheet (.xls with xlrd, .xlsx with the shared decoder).
Silver, found by header and label text, never by cell position:
- `silver.cie_sio_teachers`: persons (total, full-time, part-time) and full-time posts by professional grade,
  Poland 30 September 2006-2018 and the 16 voivodeships at the last date (date read from the block title).
- `silver.cie_sio_schools`: schools and pupils 2018/2019 by voivodeship, school type, pupil category, publicness and
  governing body (no Poland row is published; none is derived).
- `silver.cie_sio_graduates`: graduates and of which girls 2010-2018 by voivodeship (and the published Poland row),
  pupil category and school type (two layouts: per-category cross-tabs 2010-2017, a tidy table 2018).

Gold block `poc_build_cie_sio` (blocks `teachers`, `schools`, `graduates`, annual, `date_key` = 1 January of the
reference year, `period_label` = the 30 September date), registered in `warehouse/macros/sources/cie_sio.sql`
(areas by GUS voivodeship id → `TERC:<id>`, `PL`). KPI `teachers` (draft). Units as published: `persons`, `fte`
(full-time posts), `schools`. Monthly check on the 12th. Licence: `unresolved` (cie.gov.pl states no reuse terms; the
source stays internal, licence gate).

Not modelled yet: schools and pupils 2010-2016 (per-voivodeship cross-tabs without totals; no 2017 workbook),
pupils by grade and by age, pre-school, special educational needs, foreign and minority languages, accidents.
