# URE energy price indicators (`ure_prices`)

**Built (register PL-ENV-020, #2870): Landing → Silver scheduled, Gold block registered.** The Energy Regulatory
Office (Urząd Regulacji Energetyki) publishes its price indicators as fixed pages of www.ure.gov.pl, each with one
HTML table holding the whole series, edited in place when a new value is published (the page shows "Data
modyfikacji"). No export, file or API.

- **Route:** three pages, one request each, 5 s apart (`extraction.kind: bulk_file`, `landing.extract` paced in
  `ingestion/to_landing/ure_prices`): 7852 average electricity sale price on the competitive market (yearly from
  2008, quarterly from 2015 Q2, PLN/MWh), 7853 average household electricity price (yearly from 2012, PLN/kWh,
  excise included, VAT excluded), 7859 average purchase prices of imported natural gas (quarterly; EU/EFTA states
  from 2013 Q3, PLN/m3 for 2013-2014 and PLN/MWh from 2015; other states from 2023). robots.txt is empty. Entity = the
  page; version = download time (no Last-Modified), the page's modification date is kept in Bronze and Silver;
  restart unit: the page. Every page ends with two request-time comments (render time, buffer digest) that change
  on every download; `reduce: ure_prices` removes exactly that trailer after `</html>`, so an unchanged page lands
  nothing. Checked monthly (17th, 07:35 UTC); quarterly values appear 6-12 weeks after the quarter, the household
  price in March.
- **Bronze** `bronze.ure_prices_cells`: one row per data cell (year, column header text through rowspan/colspan,
  cell text as shown) with the page's modification and publication dates. The known layout of each page (number of
  tables, exact column headers, one year per row, no merged data cells) is in `ingestion/to_bronze/ure_prices/load.py`;
  any other layout rejects the page into `bronze.ure_prices_rejected_files` with the reason.
- **Silver** `silver.ure_prices_indicators`: indicator × period (a year and its quarters are separate periods),
  decimal comma read, unit from the header or the cell (`1,31 zł/m3`); an empty cell (not published yet) and the
  figure dash `‒` (no such price) keep value null with `value_status` `empty` / `dash`, never 0. The URE notice
  numbers of each row are kept (`notice_refs`).
- **Gold** `gold.build_ure_prices`, block `energy_prices`, Poland: one measure per indicator × frequency × unit
  (the yearly and quarterly competitive-market prices are separate measures), values and units as published. Draft
  KPIs (environment.energy): `electricity_price_competitive_annual`, `electricity_price_competitive_quarterly`,
  `electricity_price_households`, `gas_import_price_eu_efta` (PLN/MWh measure only).
- **Left out:** the eight other electricity indicator pages of the list (sector compensations 12418, art. 49aa price
  11063, price outside public sale 7851, tariff averages 10600, wholesale prices 8425 (frozen since 2020, 15 small
  tables), coal cost 7854, capacity charge 9165, capacity penalty 9962), the gas price from non-EU/EFTA states as a
  KPI (in Gold as a measure), and the small-installation reports (documents on bip.ure.gov.pl, not tables).
- **Licence:** not stated. The pages read (indicator pages, privacy policy, auction rules, footer) give no licence,
  attribution wording or reuse terms; `unresolved`, internal (licence gate). Attribution if used: "Źródło: Urząd
  Regulacji Energetyki, www.ure.gov.pl".
