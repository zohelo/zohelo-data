# NBP balance of payments and international investment position (`nbp_bop`)

**Built (11 Nov 2026, register PLG-002, PLG-003).** NBP publishes the balance of payments of Poland and the
international investment position as three complete XLSX workbooks at fixed URLs, landed whole through the
shared `bulk_file` pattern (version = Last-Modified, written only when the SHA-256 changed; restart unit: one
file): `https://static.nbp.pl/dane/bilans-platniczy/bop_m.xlsx` (entity `bop_m`, monthly BoP),
`bop_q.xlsx` (`bop_q`, quarterly BoP) and `iip_q.xlsx` (`iip_q`, quarterly positions), each with sheets
PLN, EUR and USD (IIP: "IIP PLN" …), million units, items in rows and periods in columns from 2004. The
nbp.pl pages that link the files answer automated clients with HTTP 403 (Dev probe 43085044303); the files
answer. Bronze: `bronze.nbp_bop_cells` (one row per non-empty cell, the shared standard-library reader
`ingestion/common/xlsx_cells.py`; a workbook whose sheets are not exactly its currency sheets is rejected). Silver: `silver.nbp_bop_observations` (workbook ×
currency × item × period; `item_code` fixed by rule and checked against the expected list; NBP's typo
"Finsncial derivatives" in bop_m is normalised). Gold: build stage
`build_nbp_bop` (PLN and EUR, one measure per workbook × currency × item; IIP components and their parent
totals are separate measures, never to be summed together). The workbooks hold amounts only: the shares of GDP of the current
account and of the net position are not published there. Licence: NBP website Terms of Use, unresolved
(internal, owner decision of 14 Oct 2026).
