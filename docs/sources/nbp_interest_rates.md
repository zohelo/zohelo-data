# Official NBP interest rates (`nbp_interest_rates`)

**Implemented (10 Nov 2026, register PLG-006).** NBP publishes its official rates as two small XML
files on fixed URLs, landed whole through the shared `bulk_file` pattern (version = Last-Modified,
written only when the SHA-256 changed; restart unit: one file):
`https://static.nbp.pl/dane/stopy/stopy_procentowe_archiwum.xml` (entity `rates_archive`: every MPC
change since 1998-02-26, `<pozycje obowiazuje_od>` groups of `<pozycja id oprocentowanie>`; ids `ref`
reference, `lom` lombard, `dep` deposit, `red` rediscount, `dys` discount, `dep` from 2001-12-01, `dys` from 2010-01-01) and
`https://static.nbp.pl/dane/stopy/stopy_procentowe.xml` (entity `rates_current`: the rates in force
with PL/EN names, footnotes and required-reserve rates). The old `www.nbp.pl/xml/…` URL now redirects
to an HTML page. Bronze: `bronze.nbp_interest_rate_items` (one row per `<pozycja>`, attributes as
text, an unknown attribute rejects the file). Silver: `silver.nbp_policy_rates` (rate × effective
date, `valid_to` = next change). Licence: NBP website Terms of Use (source with a link to
www.nbp.pl); public display waits for NBP's answer like the exchange rates. MFI interest-rate
statistics are a separate, not yet built scope.
