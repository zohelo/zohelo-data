# filings.xbrl.org ESEF annual reports (`xbrl_filings`)

**Built (register CD-013, #1500).** XBRL International's repository of ESEF filings has no bulk file per country
(its yearly index files hold every country and less than the API), so the documented JSON:API is the route
(https://filings.xbrl.org/docs/api, no key, no stated rate limit; the repository's identified User-Agent with the
project link, as the provider asks). `ingestion/to_landing/xbrl_filings/` lands two kinds of entity, one request
at a time, 2 s apart, three tries with backoff, a time budget:

- `filings_pl`: the Polish filing list with its issuers in one request (`filter[country]=PL`, one page of up to
  1,000, `include=entity`), exact bytes, version = download time, unchanged bytes skipped. A list that needs a
  second page stops the run (Landing) and is rejected (Bronze).
- `facts_<fxo_id>`: per filing with an xBRL-JSON document, the facts of `ifrs-full:Revenue` and
  `ifrs-full:ProfitLoss` (each fact whole, every dimension), reduced before Landing because the documents are
  0.1 to 9 MB of mostly text blocks (storage rule); version = the filing's `processed` time, so a filing is read
  again only when the repository reprocesses it. A document that cannot be read is reported and read on the next
  run.

Bronze: `bronze.xbrl_filings_filings` (one row per filing, attributes as text, issuer LEI and name),
`bronze.xbrl_filings_facts` (one row per fact, other dimensions as JSON text), rejections with the reason in
`bronze.xbrl_filings_rejected_files`. Silver: `silver.xbrl_filings_filings` (grain `fxo_id`) and
`silver.xbrl_filings_facts` (entity-level facts, grain filing × concept × period × unit; a key with several
different values keeps value null and the range, never a picked value). Gold: entity tables `gold.esef_issuer`
(LEI, joins GLEIF), `gold.esef_filing` (`fxo_id`) and `gold.esef_financial_fact` (revenue and profit or loss of
the reporting period per filing, currency as published), and the block `esef_reports` in
`gold.build_xbrl_filings` (filings and issuers per year of period end, Poland, annual). The repository gives no
KRS, NIP or REGON and no region: none is invented. Language versions and refilings are separate filings with the
next sequence number and no version column, so every filing is kept and nothing is picked between them.

Left out: other countries, other concepts (assets, equity, employees …), dimensional facts (segments, equity
components) and comparative periods in Gold (they stay in Silver), the report packages and viewers. The
repository says it is not complete. Refresh full (D32, #1500); weekly, Thursday 02:25 UTC. Terms: "At present,
there are no restrictions on the ways that the data can be used" (https://filings.xbrl.org/docs/about), no licence
named: `unresolved`, internal (owner question in the register). Restart unit: one filing.
