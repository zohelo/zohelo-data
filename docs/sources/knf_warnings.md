# KNF public warning list (`knf_warnings`)

Register PLG-019, Task #3341. Route: the fixed page https://www.knf.gov.pl/dla_konsumenta/ostrzezenia_publiczne holds
the whole list (817,615 bytes of HTML, 668 entries in 13 sections on 27 Nov 2026); robots.txt allows every agent;
there is no export, file or API. One GET per run (timeouts 30 s / 120 s, at most 3 tries); a refusal or an unknown
layout stops the run and is reported. Code: `ingestion/to_landing/knf_warnings/extract.py`.

Page layout (Dev probes 45945506291, 45945655242): one table per legal-basis section, each headed "Zawiadomienia o
podejrzeniu popełnienia przestępstwa z art. … ustawy …"; six columns: Lp., the entity's name, its identifier (KRS,
NIP or REGON, or "podmiot nie jest zarejestrowany w Polsce", "brak danych"), the competent prosecutor's office,
mentions of final rulings ("-" when none), important information. The page states its total ("Liczba znalezionych
ostrzeżeń: 668").

Reduced before Landing (register build limits, data minimisation): an entry names the entity, which can be a natural
person, and the entries are notifications of a suspected offence, not findings. Never stored, also not in Landing or
the Archive: names, the free-text columns, NIP and REGON numbers. Kept per entry: the section key (the legal basis up
to the first parenthesis, as a slug) and the section heading, Lp., the identifier kind (`krs`, `nip_regon`,
`not_registered_pl`, `no_data`, `other`), the KRS numbers (exactly ten digits after "KRS"), the prosecutor's office as
listed (closed character set, must name a "Prokurat…") and whether a final ruling is mentioned. The entries read must
equal the stated total and Lp. must not repeat within a section.

Entity = `warning_list`; version = download time; a copy is written only when the reduced document's SHA-256 changed.
The page keeps only the current list, so the source is an accumulating snapshot: Bronze `bronze.knf_warnings_entries`
loads every copy (shared loader, all versions; another document shape is rejected into
`bronze.knf_warnings_rejected_files`), and history starts with the first copy. Silver `silver.knf_warnings_entries`
(grain copy × section × Lp.) adds the download date, the office without its parenthesised note and the office level
(krajowa, regionalna, okręgowa, rejonowa, other). Gold: entity table `gold.knf_warning` (newest copy, keyed by section
× Lp.; KRS numbers link to KRS entity tables by number) and build stage `build_knf_warnings`, block `warning_list`:
entries in total, by legal basis, by prosecutor level, by identifier kind, and those mentioning a final ruling; Poland,
frequency D at the copy's date (a date has rows only when the list changed). kpi false. Schedule: weekly, Friday 14:35
UTC (O46). Licence: `unresolved` (the page states no reuse terms): internal only.

Left out: names, NIP and REGON numbers, the free-text columns, the dates and texts of rulings, and any reading of an
entry as a finding. History before the first copy does not exist at the source.
