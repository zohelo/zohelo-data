# KG PSP fire-service interventions (`kgpsp_interventions`)

**In progress (register EX-003, #2970): Landing and Bronze written, not yet proven.** The Headquarters of the State
Fire Service publishes on https://www.gov.pl/web/kgpsp/interwencje-psp, per year since 2016, a ZIP of standard
summary tables ("Tabele stałych Zestawień-ST SWD PSP", about 8-11 MB) and a ZIP of incident-report data ("zestawienie
danych z informacji ze zdarzeń", one CSV of about 0.5 M records a year). Route: the page is the entry (attachment
uuids change when a file is replaced); every attachment link whose text is a year and a `.zip` file name and does
not name the incident reports is one entity (`tabele_<year>`); a year with two such links or a gap between the first
and the newest year stops the run. Version = Last-Modified; restart unit: one file. Bronze
`bronze.kgpsp_interventions_cells`: every cell of every `.xlsx` or `.xls` member (inner ZIPs opened, member path
joined with `!`). Left out: the incident-report archives (size; the record-level location fields, TERYT down to
locality and road chainage, are checked before any load). Licence: CC BY 4.0, stated on the page ("Dane
statystyczne KG PSP, zostały zamieszczone na portalu dane.gov.pl Rodzaj licencji: CC BY 4.0").

Access: on 16 Nov 2026 www.gov.pl and dane.gov.pl did not answer TCP connections from GitHub runners (IPv4 timeout,
IPv6 unreachable; Dev runs 43798868238 to 43815599563), after answering on 12 and 13 Nov. The extractor keeps its
bounded retries; a refused run is a failed run, never a workaround.
