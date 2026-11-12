# KPRM work list of the Council of Ministers (`kprm_work_list`)

**Built (register PLG-034, #491).** The approved route for the government legislative process: the Council of
Ministers' work list (Wykaz prac legislacyjnych i programowych Rady Ministrów) that KPRM publishes on gov.pl. RPL
itself (legislacja.rcl.gov.pl, legislacja.gov.pl) times out for GitHub runners and is not read.

- **Route:** bulk file. The page https://www.gov.pl/web/premier/wplip-rm links "Pobierz dane do pliku csv"
  (`/register-file/Rejestr_20874195.csv` on 12 Nov 2026). The address is not documented as stable, so the shared
  `bulk_file` pattern reads the link from the page on every run (`url_from`, pattern
  `/register-file/Rejestr_[0-9]+\.csv$`): a page without it fails Landing. robots.txt of www.gov.pl allows all.
- **Entity and version:** one entity, `council_of_ministers` (the file); the file has no Last-Modified or ETag, so
  version = download time and a copy lands only when its SHA-256 changed; the schedule runs once a day (03:25 UTC).
  Restart unit: the file. Refresh full (D32).
- **File:** UTF-8 (a BOM is accepted; the file of 12 Nov 2026 has none), `;`-separated, CRLF rows, quoted values
  with line breaks and semicolons, 19 columns, one row per draft (1,526 on 12 Nov 2026).
- **Bronze** `bronze.kprm_work_list_drafts`: DuckDB reads the file with quotes and strict mode, so every row must be
  19 fields wide; a different header (names or order) or a row of another width rejects the file into
  `bronze.kprm_work_list_rejected_files` with the reason (the offending line is not copied: it may name officials).
  Not loaded beyond Landing: `Osoba odpowiedzialna` (names of officials), the aims and substance texts (up to
  91,000 characters) and the three regulatory-impact columns (empty in every row on 12 Nov 2026).
- **Silver** `silver.kprm_work_list_drafts`: one row per work-list number (`Numer projektu`, unique: tested, never
  deduplicated); kind, type, responsible, drafting and cooperating body, planned adoption as published (free text:
  a plan, not a date), resignation information, status `realised` / `withdrawn` / `in_progress` (the source's empty
  status), publication on the list (`Data publikacji`, Warsaw local time), link.
- **Gold:** entity table `gold.government_drafts` (keyed by the work-list number) and the build stage
  `gold.build_kprm_work_list`: drafts entering the list per month (`…:monthly`) and per year (`…:annual`) by month
  or year of `Data publikacji`, total and by kind or by responsible body; the drafts that entered the list in a
  month or year by their current status (`…:monthly_by_current_status`, `…:annual_by_current_status`: the status in
  the newest file of that period's entries, not a history of status changes); periods without an entry have no row.
  `Data publikacji` rises with the work-list number and does not move when the status changes (Dev probe
  43320804384), so it dates the entry. Realised and withdrawn counts by the date of the change are not built: the
  file does not give that date. Draft KPI `government_drafts_listed` (state.government), monthly total.
- **Left out:** the Prime Minister's own list (https://www.gov.pl/web/premier/wpl-prm, `Rejestr_20874196.csv`): 8
  columns, no kind, body or publication date, so not the same layout; the lists of earlier governments (HTML pages
  without a file).
- **Licence:** the stricter of two statements, CC BY-SA 4.0 (page footer: "Treści tekstowe publikowane w serwisie
  (z wyłączeniem treści audiowizualnych), są udostępniane na licencji typu Creative Commons: uznanie autorstwa - na
  tych samych warunkach 4.0 (CC BY-SA 4.0)."; the KPRM re-use page https://www.gov.pl/web/premier/ponowne-wykorzystywanie
  states CC BY 3.0 PL); licence row `cc_by_sa_4_0_kprm`, attribution "Źródło: Kancelaria Prezesa Rady Ministrów,
  Wykaz prac legislacyjnych i programowych Rady Ministrów, gov.pl (<data pobrania>), CC BY-SA 4.0".
