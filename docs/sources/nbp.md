# Narodowy Bank Polski public statistics (`nbp`)

The [NBP Web API](https://api.nbp.pl/en.html) is not a catalogue of all NBP
statistics. Its documented scope is exchange-rate tables A, B and C plus NBP gold
prices. A complete collection for the selected **NBP public-statistics product** must
combine those feeds with the official statistical pages and their downloadable
distributions. This scope does not mean every document published by the NBP
organisation.

## Machine feeds and FX inventory

```text
# Exchange tables; split history into documented date windows
https://api.nbp.pl/api/exchangerates/tables/A/{START}/{END}/?format=json
https://api.nbp.pl/api/exchangerates/tables/B/{START}/{END}/?format=json
https://api.nbp.pl/api/exchangerates/tables/C/{START}/{END}/?format=json

# Gold
https://api.nbp.pl/api/cenyzlota/{START}/{END}/?format=json

# Official historical XML-file inventory and files
https://static.nbp.pl/dane/kursy/xml/dir.txt
https://static.nbp.pl/dane/kursy/xml/{FILENAME_FROM_DIR}.xml
```

The API supports intervals no longer than 93 days and can return `400 Limit
exceeded`; page the dates and treat valid no-data `404` responses as empty windows,
not deletions. The XML `dir.txt` is an official exact filename inventory for the FX
archive and complements the API, especially for representation-level completeness.

**Implemented (FX tables and gold):** `ingestion/to_landing/nbp/` fetches one entity
per table and calendar quarter (`A_2024Q1` … `GOLD_2026Q4`; 356 requests for the full
history, one at a time with a pause). Version = the latest effective date in the
response. Quarters that ended more than 10 days ago are fetched once; open quarters
are re-fetched and written only when the bytes changed. Bronze: `bronze.nbp_observations`
(one row per quote or gold price, values as source text). The yearly CSV/XLS archives
add only pre-2002 history and the broader statistics below are a separate scope (not
yet built).

## Broader statistics inventory and distributions

Crawl only the following official navigation subtrees, follow their pagination/year
pages, and inventory every linked statistical XLS, XLSX, CSV, XML, ZIP, 7Z, TXT and
methodology PDF on `nbp.pl` or `static.nbp.pl`:

```text
https://nbp.pl/en/statistic-and-financial-reporting/
https://nbp.pl/en/statistic-and-financial-reporting/monetary-and-financial-statistics/
https://nbp.pl/en/statistic-and-financial-reporting/balance-of-payments-statistics/
https://nbp.pl/en/statistic-and-financial-reporting/financial-markets/
https://nbp.pl/en/statistic-and-financial-reporting/instruments/
https://nbp.pl/en/statistic-and-financial-reporting/core-inflation/
https://nbp.pl/en/statistic-and-financial-reporting/inflation-expectations-and-forecasts/
https://nbp.pl/en/statistic-and-financial-reporting/rates/
https://nbp.pl/en/payment-system/statistical-data/
```

The monetary subtree includes, among other distributions, M3 and counterparts,
reserve money, MFI assets and liabilities, interest-rate statistics, financial
accounts, banking-sector financial data and Divisia monetary indexes. The
balance-of-payments subtree includes balance of payments, official reserves,
external debt, international investment position and international trade in
services. Financial markets, central-bank instruments, core inflation, inflation
expectations/forecasts, rates and payment-system statistics are separate inventories
and must not be represented by the four Web API feeds.

Known official durable workbook examples are:

```text
https://static.nbp.pl/dane/bilans-platniczy/bop_m.xlsx
https://static.nbp.pl/dane/bilans-platniczy/bop_q.xlsx
https://static.nbp.pl/dane/bilans-platniczy/bop_q_pln.xlsx
https://static.nbp.pl/dane/aktywa-rezerwowe/rez_m.xlsx
```

The first is monthly balance-of-payments history from January 2004, the next two are
quarterly distributions, and `rez_m.xlsx` is monthly official-reserve history from
January 1998. Discover these links from their current source pages rather than using
the examples as a closed allowlist.

Use the [NBP release calendar](https://nbp.pl/en/statistic-and-financial-reporting/calendar/)
as a second freshness signal, but do not treat it as a machine-readable attachment
catalogue. For each attachment retain the source page, displayed label/update date,
URL, response validators when supplied, byte size, retrieval timestamp and SHA-256.
Re-crawl the navigation pages and compare both URL and content hash. No reviewed NBP
document promises byte ranges, so the portable recovery action is to retry one file.

NBP pages and API documentation say the service is public, but the footer says “All
rights reserved,” and the API documentation grants no commercial redistribution or
database licence. The linked Terms of Use were not established here as a grant for
redistribution. Catalogue/download for private analysis can proceed; public or
commercial redistribution remains unresolved until the applicable terms or written
permission are reviewed. No geography restriction was found.

Primary evidence:

- [NBP Statistics landing page](https://nbp.pl/en/statistic-and-financial-reporting/)
- [NBP release calendar](https://nbp.pl/en/statistic-and-financial-reporting/calendar/)
- [NBP Web API documentation](https://api.nbp.pl/en.html)
- [NBP instructions for the historic exchange-rate file inventory](https://nbp.pl/en/statistic-and-financial-reporting/rates/the-instruction-how-to-retrieve-currency-exchange-rates-from-the-nbp-website/)
- [NBP monetary and financial statistics](https://nbp.pl/en/statistic-and-financial-reporting/monetary-and-financial-statistics/)
- [NBP balance-of-payments statistics](https://nbp.pl/en/statistic-and-financial-reporting/balance-of-payments-statistics/)
- [NBP payment-system statistical data](https://nbp.pl/en/payment-system/statistical-data/)
- [NBP regulations / Terms of Use entry point](https://nbp.pl/en/about-nbp/contact/nbps-regulations/)
