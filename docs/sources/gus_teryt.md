# GUS TERYT (territorial register) (`gus_teryt`)

Catalogues: TERC (territorial units: voivodeship, powiat, gmina with RODZ), SIMC
(localities, `SYM`/`SYMPOD`), ULIC (streets per locality, `SYM_UL`), WMRODZ (locality
types). Each comes in variants: *urzędowy* (official), *adresowy* (for addressing) and,
for SIMC, *statystyczny* (with statistical subdivisions `NMST`, `NMSK`, `SYMSTAT`).

**Route (implemented):** the public full-file page
`https://eteryt.stat.gov.pl/eTeryt/rejestr_teryt/udostepnianie_danych/baza_teryt/uzytkownicy_indywidualni/pobieranie/pliki_pelne.aspx`
serves each catalogue variant as a ZIP holding the same data as XML and as CSV
(`;`-separated, UTF-8 with BOM). The page is an ASP.NET form: one GET for the form
state, then one POST per file with `__EVENTTARGET` = the file's download control
(`ctl00$body$BTERCUrzedowyPobierz` …). No account, no browser automation. The SOAP
service TERYT ws1 offers the same full files plus change files but needs an account
(e-mail to teryt_ws1@stat.gov.pl); not used.

Version = the catalogue state date in the XML (`<catalog … date="YYYY-MM-DD">`): TERC
and SIMC state 1 January unless legal changes intervene; ULIC is restated daily. Sizes
(October 2026, ZIP): TERC 0.13 MB, SIMC 3.2–4.5 MB, ULIC 8.1 MB, WMRODZ 0.05 MB.

Bronze `bronze.teryt_records`: the CSV member, every column as text, mapped onto the
union of catalogue columns. Change files (zmiany) are not loaded; successive full
states give the history.
