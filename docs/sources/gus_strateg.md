# GUS STRATEG indicators (`gus_strateg`)

**Built (26 Nov 2026, register PLS-003, #2430).** STRATEG is Statistics Poland's system of indicators for monitoring
development strategies and programmes (https://strateg.stat.gov.pl/). It has no bulk download; its REST API
`https://strateg.stat.gov.pl/api/` (spec `https://strateg.stat.gov.pl/strateg_en.json`; the host
api-strateg.stat.gov.pl named in older pages does not resolve) serves the lists and the data as JSON.

Route (`ingestion/to_landing/gus_strateg/`): for strategy 78, the Strategy for Responsible Development (SOR,
2017-2030): the SOR indicator list in Polish and English (`strategies-programmes/78`, one indicator dimension per
indicator, with unit, objective and the strategy's base, intermediate and target values), each indicator's levels and
years (`indicators/<id>`), and the observed values of its dimension at level 0, the EU aggregates and the EU
countries, and level 2, the voivodeships (`indicators/<id>/dimensions/<dimension>?level_id=<L>`, `real_values`
by year and 12-digit territory code). About 190 answers, one request at a time (anonymous quota: 5 a second,
100 per 15 minutes). The API answers a request over the quota with HTTP 404 "Nie znaleziono żądanego zasobu", not
429 (a trial paced 9.5 s apart got it on its 101st request; the same address answered HTTP 200 seven minutes
later), so the extractor sends bursts of at most 90 requests, each followed by 16 minutes without one, and asks a
404 once more after such a rest: about 38 minutes a read. Entity `sor` = the whole selection as one ZIP of the
exact answers; version = time the read started (the API gives no update time); a read equal to the newest copy writes
nothing; restart unit: the whole read (a read that fails part-way lands nothing; connect timeouts happen and are
retried with backoff).

Bronze `bronze.gus_strateg_records` (`indicator` rows from the list, `value` rows from `real_values`, all text).
Silver `silver.gus_strateg_indicators` (one row per indicator dimension; targets as published JSON text, not
observations) and `silver.gus_strateg_observations` (indicator dimension x level x territory x year; a value that
is not a number keeps `value` null, `value_status = 'not_numeric'`, never 0). Gold block `sor`
(`warehouse/macros/sources/gus_strateg.sql`): one annual measure per indicator dimension, values and units as
published (D33); Poland from level 0, voivodeships from level 2; EU-28 (`030000000000`) has no Gold area and is not
built. Size (26 Nov 2026): 83 indicator dimensions, all with values for Poland, 21 also for the 16 voivodeships;
20,869 values, 2003-2025; 72 indicators carry SOR targets. Units as published, mapped (D33) where the code is plain
(`%`, `dni`, `lata`, `km`); `osoby na 100 ludności` stays unmapped because the English list calls it "persons per
1000 population". No KPI rows yet: the SOR indicators that match existing concepts (R&D expenditure in % of GDP,
the at-risk-of-poverty-or-social-exclusion rate, the employment rate) can join those KPIs as another source (O31). Values overlap
GUS BDL and Eurostat by design; the source is kept, never deduplicated.

Left out (next steps, same API): the other 100+ strategies and programmes (national, supra-regional, voivodeship,
ZIT, partnership agreement, operational programmes), the other levels of the SOR indicators (3 regions NUTS 2,
4-6 powiats and gminas, M and O), the cohesion-policy and thematic lists, and indicator metadata (`metrics/<id>`).
Weekly schedule (Wednesday 08:45 UTC, O46).

Licence: the STRATEG API spec (`strateg_en.json`, `info.license`) names the "Creative Commons Attribution 4.0
International Public License" (https://creativecommons.org/licenses/by/4.0/legalcode), `cc_by_4_0_gus_strateg`;
attribution "Źródło: GUS, STRATEG" / "Source: Statistics Poland, STRATEG". The general GUS terms
(https://stat.gov.pl/copyright) also allow reuse with the source given.
