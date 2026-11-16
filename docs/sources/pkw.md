# PKW election results (`pkw`)

**Built (16 Nov 2026, register PLG-029, #486).** National Electoral Commission (PKW / KBW) result files of each
election site `https://<election>.pkw.gov.pl/<election>/`, declared as 25 files of the shared `bulk_file` pattern (no
extractor code): Sejm 2023, European Parliament 2024 and President 2025 (rounds 1 and 2) as ZIP with one CSV (UTF-8
with BOM, semicolons), the voivodeship assemblies (sejmiki) 2024 as XLSX; counts and per-cent files by voivodeship,
powiat and gmina, and the sejmiki town/country file (Poland).

- **Route.** The "dane w arkuszach" page is a script shell. File names come from the site's bundle
  (`/<election>/j/<stamp>.min.js`, module `csv_links`; samorzad2024 `SamorzadCsvController`); pe2024 and prezydent2025
  add a stamp, `data/csv/<name>_csv.<ts>.zip`, where `ts` is the file's entry in `data/config.blob` (protobuf index,
  directory `csv`). A wrong name or stamp answers the HTML shell with status 200: `format: zip` rejects it, so a
  changed stamp fails `landing.extract` (and needs the declaration updated) instead of landing HTML.
- **Entity and version.** One file; version = Last-Modified; a copy lands only when its SHA-256 changed; restart
  unit one file. The files are static after the official results: monthly check (3rd, 05:25 UTC).
- **Bronze.** `bronze.pkw_cells`: one row per non-empty cell (member or sheet, row, column, text), the shape of the
  workbook sources; CSV members read by DuckDB, XLSX by `ingestion/common/xlsx_cells.py`; a file that is not UTF-8,
  has rows of different widths or is not a ZIP is rejected (`bronze.pkw_rejected_files`).
- **Silver.** `silver.pkw_results`: one row per value cell with the election, level, area (TERYT code padded to 6
  digits and cut to the level; abroad, ships and code-less rows kept with `area_kind`), header, measure class
  (turnout, eligible voters, ballots issued, valid and invalid votes, list votes and shares, candidate votes,
  other protocol columns) and the value as published.
- **Gold.** `build_pkw` (blocks `sejm`, `pe`, `sejmik`, `prezydent`; frequency D at the election day;
  `pkw:<type>:<unit>:<measure>`): Poland and voivodeships with every modelled measure (lists and candidates as the
  breakdown), powiats and gminas with turnout (and eligible voters and ballots issued for the President, whose files
  print no turnout). Gminas map by the 6-digit code to the TERC gmina of type 1, 2, 3 or 8; voivodeships without a
  code and the EP 2024 rows of cities with powiat rights by name (`macros/sources/pkw.sql`). `pkw_elections`: one row
  per election and round. Draft KPI `voter_turnout`.
- **Left out.** Votes per list or candidate by powiat and gmina (in Silver; about 100,000 series, beyond the shared
  `dim_series` ceiling: per-block coverage, D35), polling-district files (protocols, about 31,000 rows per
  election), candidate-by-area files of Sejm and EP, districts (okręgi), the Senate, local councils and mayors,
  older elections (`danewyborcze.kbw.gov.pl`, not inventoried) and committee-member files (personal data).
- **Licence.** Unresolved: PKW and KBW state no reuse terms; internal only (licence gate). Attribution if used:
  "Źródło: Państwowa Komisja Wyborcza".
