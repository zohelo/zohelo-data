# MRiRW ZSRIR market bulletins (`mrirw_zsrir`)

**Built (16 Nov 2026, register PLS-032, #2484).** Route: the ministry's ZSRIR portal `zsrir.minrol.gov.pl`, which
publishes every weekly market bulletin as XLSX and PDF since August 2024 through its own public endpoints:
`/api/ZsrirData/GetReportFileList?id=<report>` (file id, period, publication time, file name) and
`/api/ZsrirData/DownloadReportFile?id=<file>`; no sign-in. The legacy gov.pl market pages only point to dane.gov.pl,
which does not answer runners. The portal sends its leaf certificate without the intermediate; the extractor adds
the CA's published intermediate (Certum DV TLS G2 R39 CA, from the address in the leaf's Authority Information Access
field) to certifi's roots and verifies chain and host name as usual (`ingestion/to_landing/mrirw_zsrir`).

Entity = one market's bulletin (cereals report 2, milk 1, pork 14), version = download time (no Last-Modified); only
the newest XLSX issue is landed, written when the newest file id changed and the SHA-256 differs; restart unit one
file. Bronze `bronze.mrirw_zsrir_cells` (`ingestion/common/xlsx_cells.py`): one row per non-empty cell; a workbook
without its monthly sheet (found by name) or that sheet's grid header is rejected into
`bronze.mrirw_zsrir_rejected_files`. Silver `silver.mrirw_zsrir_prices`: monthly average purchase prices, Poland, one
row per market × product × month, unit and currency as published, cells without a price kept with their text and no
value. Gold block `gold.build_mrirw_zsrir` (`warehouse/macros/sources/mrirw_zsrir.sql`), draft KPIs
`milk_purchase_price`, `pig_purchase_price`, `wheat_purchase_price`. Weekly check (Saturday). Licence: the portal and
the workbooks state no terms of use: `unresolved`, internal only. Left out: the weekly tables, macroregion tables, the
other bulletins (poultry, beef, eggs, oilseeds, fruit, vegetables, feed, sugar, mutton, tobacco, hops), the
cross-market summary and the PDF copies.
