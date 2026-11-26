# ZUS social insurance statistical yearbook (`zus_yearbook`)

**Built (25 Nov 2026, register PLS-010, #434).** ZUS publishes "Rocznik Statystyczny Ubezpieczeń Społecznych" every
three years as one PDF per edition on
https://www.zus.pl/baza-wiedzy/statystyka/rocznik-statystyczny-ubezpieczen-spolecznych; there is no XLSX, CSV, API or
dane.gov.pl copy (Dev probe 43326914693), and robots.txt of www.zus.pl allows every path. Route: the yearbook page and
its list pages (at most 4, 5 s apart) link each edition under `/documents/10182/167642/<name>.pdf/<uuid>`
(`ingestion/to_landing/zus_yearbook/`); the Polish editions from 2009-2011 on are landed whole, the English copies
and the older editions (1996-2008, other layouts) are not. Entity = one edition (`rocznik_<first>_<last>`), version =
Last-Modified (download time when absent), written only when the SHA-256 changed; missing editions first, then the
newest edition again; restart unit: one edition. Bronze `bronze.zus_yearbook_lines`: the PDF text layer (pypdf,
layout mode) as one row per non-empty line with page and line number; a file that is not a PDF, is encrypted or has
no text layer is rejected into `bronze.zus_yearbook_rejected_files`. Silver `silver.zus_yearbook_tables`: contribution
payers (Tabl. 8) and insured persons in pension insurance (Tabl. 9) by voivodeship at 31 December, 2009-2023, found
by title, the table's half of a two-page spread by the title's column, three years per edition from the header; ZUS
signs kept as `cell_status`, never 0. Insured persons are assigned to voivodeships by the payer's seat in the
2009-2011 edition and by the registered residence from 2012 (`area_basis`, from the footnote; two Gold measures);
from 2012 the voivodeships add up to 97-100 % of Poland. The 2015-2017 text layer prints values without digit-group
spaces and moves the leading group of Poland's first year to the next line; the model puts it back under a guard
(`value_joined`). Gold blocks `payers_voiv` and `insured_voiv` (annual), registered in
`warehouse/macros/sources/zus_yearbook.sql`; KPIs `zus_contribution_payers` (new, draft) and `zus_insured_persons`
(second source after `zus_psz`). Monthly schedule (9th). Licence: `unresolved` (terms not stated for the yearbook;
the ZUS re-use terms name only its BIP and dane.gov.pl), so the source stays internal. Not built: the pension, finance,
benefit, certification and rehabilitation tables.
