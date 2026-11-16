# NID register of immovable monuments (`nid_monuments`)

**Built (Nov 2026, register PLS-026, #2471).** Route: NID's own lists on
https://nid.pl/zasoby/rejestr-zabytkow-zasoby/ ("Wykaz zabytków nieruchomych wpisanych do rejestru zabytków (księga
A)"), one PDF per voivodeship and one for Warsaw (17 files, `<CODE>-rej_<date>.pdf`, state of 30 June 2026 published
in August 2026). The register's copy on dane.gov.pl (dataset 2627) times out from GitHub Actions, NID's INSPIRE
service of the register is a WMS only and zabytek.pl disallows robots (Dev probes 43343138018, 43797381655,
43797698239, 43798051945, 43798254885). `ingestion/to_landing/nid_monuments/` reads the page once and lands the lists
through the shared bulk_file pattern, 3 s apart. Entity = one list; version = Last-Modified; unchanged bytes
skipped; restart unit: one list.

Bronze `bronze.nid_monuments_items`: the PDF text layer (pypdf), one row per list item (wrapped lines joined) with the
page header (powiat as printed; the Warsaw list by district), the date of the list and the heading above the item
(locality), all as text; a page without the expected header rejects the list. Silver
`silver.nid_monuments_items` (object, register part, role: numbered, "jw." = same as above, part of an ensemble,
unnumbered; powiat, locality, list date; no street address) and `silver.nid_monuments_decisions` (register number and
decision date of each item, `register_key` = list:number:date). Gold: entity table `gold.nid_monument_register`
(one row per register entry: number, date, objects, powiat or voivodeship, localities) and block `register` of
`poc_build_nid_monuments` (objects and register entries by powiat, voivodeship and Poland on the date of the lists,
frequency D); powiats matched by name within the voivodeship (`macros/sources/nid_monuments.sql`). Lists of
30.06.2026 (Dev trial 43803614937): 80,657 items, 75,198 objects with a decision (numbered, "jw.", parts of an
ensemble), 5,459 unnumbered, 54,914 register entries; 523 decisions without a readable full date keep the printed
text (dates before 1918 or after the list's date are misprints and get none). Left out: the
dating of objects (free text) and the archaeological and movable registers. Quarterly check (lists updated twice a
year). Terms: the NID page states no licence (site footer "All rights reserved"); licence `unresolved`, internal
(dane.gov.pl's copy is CC BY 4.0 but not reachable).
