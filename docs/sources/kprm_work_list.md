# Work list of the Council of Ministers (`kprm_work_list`)

**Built (register PLG-034, #491).** The Chancellery of the Prime Minister (KPRM) publishes the whole work list of the
Council of Ministers (Wykaz prac legislacyjnych i programowych Rady Ministrów) as one CSV, linked as "Pobierz dane do
pliku csv" from https://www.gov.pl/web/premier/wplip-rm (robots.txt of www.gov.pl allows every agent). Route: the
shared `bulk_file` pattern; the link (`/register-file/Rejestr_<n>.csv`) is read from the page on every run
(`url_from`). Entity = the work list (`work_list`, one file); version = download time (no Last-Modified or ETag),
written only when the SHA-256 changed; restart unit: the file. About 11 MB, UTF-8 with BOM, `;` separated, 19 columns,
one row per draft since 21 December 2023 (the current government).

Bronze `bronze.kprm_work_list_drafts` (`ingestion/to_bronze/kprm_work_list/`, DuckDB reads the CSV; another header
rejects the file into `bronze.kprm_work_list_rejected_files`): one row per draft, text as published, without the two
long text columns (aims, substance) and without the responsible official's name. Silver `silver.kprm_work_list_drafts`
(grain: work-list number): kind, type, bodies, status (`in_progress` when the list gives none), publication time and
the planned adoption as published free text (a plan: never a date or a value). Gold: entity table
`gold.government_drafts`; build stage `build_kprm_work_list`, block `drafts_published`: drafts published on the list
per month and per year, in total, by kind, by current status and by responsible body (Poland; periods without a
draft have no row; the current period is provisional). Draft KPI `government_drafts_published` (drafts published on
the list per month, Poland; `gold.metrics_government_drafts_published`). Weekly, Friday 19:20 UTC (O46).
Licence: CC BY-SA 4.0 (`kprm_cc_by_sa_4_0`), the stricter of KPRM's two statements (the re-use page names
CC BY 3.0 PL, the page footer CC BY-SA 4.0).

Left out: the Government Legislative Process portal (RPL: legislacja.rcl.gov.pl, legislacja.gov.pl), which times out
from GitHub runners and whose robots.txt is unread (stage documents, consultations, ministers' own regulations); the
Prime Minister's own list (`/web/premier/wpl-prm`); work lists of earlier terms (HTML pages without a file); the
aims and substance texts and the officials' names.
