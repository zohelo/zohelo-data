# UK Companies House: Free Company Data Product (`companies_house`)

**Built (12 Nov 2026, register CD-014, #1502).** Route: the producer's bulk feature. The listing page
`https://download.companieshouse.gov.uk/en_output.html` links `BasicCompanyDataAsOneFile-<YYYY-MM-DD>.zip`
(495 MB; one CSV of 2.8 GB) and the same data in 7 parts; the shared `bulk_file` pattern reads the page first
(`url_from`) and downloads the one file: one snapshot, one receipt. Entity: `basic_company_data`. Version: the
snapshot date in the file name (in the receipt's `source_url`, Bronze column `snapshot_date`); the Landing version
token is the file's Last-Modified; a file already held (same name on the page, same Last-Modified and ETag) is not
downloaded again (`ingestion/to_landing/companies_house`), and a file whose reduced bytes are unchanged (SHA-256)
is not landed again. Monthly, on the 10th (the file is "compiled up to the end of the previous month" and
published within 5 working days). The REST and streaming APIs, the PSC snapshot (personal data) and officers are
not used.

Personal data: no natural persons' data is stored. The file is reduced **before Landing** (`reduce:
companies_house`, precedent O33) to 13 of its 55 columns: company name and number, category, status, country of
origin, incorporation and dissolution date, SIC texts 1-4, post town and country of the registered office.
Dropped and stored nowhere (Landing, Archive, Bronze): the registered office's care-of line, PO box, street lines,
county and postcode (a registered office can be a director's home) and the ten previous names (a former name can
be a person's), and, as unused, accounts, returns, mortgages, partnership counts, URI and confirmation statement
dates. So Landing holds a reduced copy, not the producer's bytes: the receipt's `sha256` is that of the reduced
ZIP (`detail.reduced`), the producer's ETag is in `detail.http`. A file that lacks a kept column, or has a row of
another width, is not landed: the run fails and names the reason. Company names are kept as the names of legal
entities (a company can be named after a person; the register publishes the name as the company's).

Bronze `bronze.companies_house_companies`: every company of the newest snapshot (5,711,779 rows, company numbers
unique), the 13 landed columns as text; replaced whole each month
(`ingestion/to_bronze/companies_house/entities.py`, design #875). A landed file with another header, a repeated
company number or no date in its name is rejected (`bronze.companies_house_rejected_files`).

Silver, written by the loader from `warehouse/entity/companies_house_*.sql`:
`silver.companies_house_poland_origin` (one row per company whose `CountryOfOrigin` is `POLAND`, the only spelling
in the data; newest snapshot; 50 rows: overseas companies with a UK establishment, numbers FC… and SF…, and
overseas entities owning UK property, OE…; a UK-incorporated company with Polish owners is "United Kingdom" and is
not there) and `silver.companies_house_snapshot_counts` (companies per snapshot × country of origin × category ×
status, 481 rows a snapshot, with the number incorporated in the month the snapshot closes, which is the month
before the month of the file's date; one file per snapshot date, kept: the
monthly history builds up from the first Landing and a month without a landed snapshot has no row; earlier
snapshots cannot be rebuilt once the Archive retention has passed).

Gold: `gold.company_uk_poland_origin` (entity table keyed `GB-COH:<company number>`; no KRS, NIP, REGON or LEI in
the file, so a join to GLEIF goes through GLEIF's own registration-authority fields, never by name) and
`gold.build_companies_house` (monthly, geography GB, period = the month the snapshot closes):
`companies_house:basic_company_data:companies:on_register` with breakdowns `country_of_origin` (`_T`, `POLAND`),
`company_status` and `company_category` (one at a time), and `…:incorporated_in_month` (UK total): companies
incorporated in the month that are on the register at its end, a lower bound, because a snapshot of live companies
no longer holds those since dissolved; for the same reason earlier months are not counted from a later snapshot.
No KPI. Left out: a company-level table of all 5.7 million companies in Silver and Gold, other countries of origin
in Gold (they are in the Silver counts), SIC and regional splits.

Terms (https://www.gov.uk/guidance/companies-house-data-products, read 12 Nov 2026): "We impose no rules or
requirements on how the information on the public register is used, and we’re not responsible for your use of the
company data. You are responsible for complying with any applicable data protection, copyright and other
legislation and regulations."; page footer: "All content is available under the Open Government Licence v3.0,
except where otherwise stated". Licence row `companies_house_public_register`; attribution "Source: Companies House
(UK), Free Company Data Product". Refresh incremental, unit file (D32). Archive: newest file plus 3 days. Measured in the Dev runs named in the pull request of #1502
(snapshot 2026-11-01: 5,711,779 companies on the register, 66,457 of them incorporated in October 2026; 50 of
Polish origin, none with an LEI registered under its UK number in GLEIF). Restart unit: the file.
