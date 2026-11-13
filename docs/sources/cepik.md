# CEPiK vehicle registration statistics (`cepik`)

**Built (13 Nov 2026, register PL-ENV-024, #2966).** The Ministry of Digital Affairs publishes the statistics of the
Central Register of Vehicles and Drivers on gov.pl: https://www.gov.pl/web/cepik/statystyki lists one page per topic
and year (`/web/cepik/pojazdy-zarejestrowane-w-<year>-roku`) with an XLSX and a PDF by voivodeship and by vehicle
type (XLSX from 2016). Route: the year pages, one request at a time (`ingestion/to_landing/cepik/`); entity = topic
and year (`vehicles_voivodeship_<year>`, `vehicles_type_<year>`), version = the attachment's Last-Modified; the
current year's workbook is replaced monthly under a new attachment URL and is downloaded on every run (unchanged
bytes are not written), closed years again only under a new attachment URL; restart unit: one file. A year before
the current one without a workbook fails Landing. api.cepik.gov.pl does not answer GitHub runners and is not used.
Bronze `bronze.cepik_cells` (every cell, shared workbook shape, `ingestion/common/xlsx_cells.py`). Silver
`silver.cepik_vehicle_registrations`: vehicles registered per month by voivodeship (with the national total row) and
by vehicle type (Poland), 2016-01 to the newest month; blocks found by header label and Polish month names, never
by cell position; placeholder zeros of unpublished months are dropped; the voivodeship rows of a month printed out
of order (September 2026 in the workbook of 22 Oct 2026) are marked `order_suspect` and left out of Gold. Gold block
`vehicle_registrations` (monthly), registered in `warehouse/macros/sources/cepik.sql`; KPI `vehicle_registrations`
(draft). Monthly schedule (26th, the month's workbook appears about three weeks after the month). Not landed: driving
licences issued, inspection stations and technical inspections (same pages; next step). Licence: CEPiK terms on the
API page (commercial and non-commercial use with the source named; `cepik_terms`, open-conditional).
