---
name: domain-economy
description: Use when defining, building, reviewing or writing about Polish or EU macroeconomic numbers on zohelo — GDP, growth, inflation (CPI, HICP, core), industrial output, retail sales, PPI, current account, convergence (PPS) — including KPI definitions, indicator pages, cards, charts, dashboards, release dates and "is this number right?" checks.
---

# Economy domain (zohelo)

## Overview

How a senior macro analyst reads Polish and EU economic statistics, compressed into rules a member applies before a
number reaches users. Core principle: **a macro number is only meaningful with its measure, its comparison and its
vintage** — "inflacja 2,9%" is wrong if it is HICP, a December y/y shown as the annual average, or a flash called
final.

The long-form memory, sources and open points stay in `docs/domains/economy.md`; this skill is the working checklist.
Neighbours: rates, FX, public debt → `docs/domains/finance.md`; wages, unemployment → `docs/domains/labour.md`.

## When to use

- A KPI definition, `kpi.csv` row, Task or Epic about an economy indicator.
- A review of an indicator page, card, chart or dashboard that shows GDP, prices or business-cycle data.
- A Quality Engineer or user question: "does this number make sense?"
- Not for: exchange rates, policy rates, bond yields, deficit/debt (finance), wages and unemployment (labour) —
  use those domain files, but apply the comparison and chart rules below to them too.

## Quick reference: indicators

| Indicator | Definition (producer) | Unit · freq. | Use for Poland | Normal range, how to read |
|---|---|---|---|---|
| CPI y/y | GUS consumer price index, same month previous year = 100, minus 100 | % · M | **the** Polish headline ("inflacja") | NBP target **2,5% ± 1 p.p.** applies to CPI only. 2023 peak in double digits. |
| HICP y/y | Eurostat harmonised index (`prc_hicp_manr`, `RCH_A`), domestic concept, NA weights | % · M | EU comparisons only; always labelled "HICP" | Differs from CPI by tenths, sometimes more. |
| Core inflation | NBP: CPI excluding food and energy (one of four core measures) | % · M | persistence of inflation | Headline − core ≈ food and energy effect. |
| CPI annual average | average of the 12 monthly indices, previous year = 100 | % · A | statutory indexation (pensions, rents) | ≠ December y/y. WDI `FP.CPI.TOTL.ZG` is this. |
| GDP real growth | chain-linked volumes, % change (ESA 2010 `B1GQ`, `CLV_PCH_PRE`; quarterly `namq_10_gdp`) | % · A/Q | growth claims | Poland trend ~3–4% a year; <2% weak, >5% boom; 2020 only post-1991 fall. |
| GDP, current prices | ESA 2010 `B1GQ`, market prices | PLN (`CP_MNAC`), EUR, US$ · A/Q | size, shares, ratios (debt/GDP) | In EUR/US$ it moves with FX: a rise is not growth. |
| GDP per capita PPS | volume index EU27 = 100: `prc_ppp_ind` `VI_PPS_EU27_2020_HAB` (`tec00114`); `nama_10_pc` `CP_PPS_EU27_2020_HAB` is the PPS level, divide by EU27_2020 | index · A | living standards, convergence | Never EUR at market rates for this purpose. |
| Industrial output | GUS sold production of industry, constant prices; Eurostat `sts_inpr_m` | % y/y · M | business cycle | Swings with working days: use calendar-adjusted or caveat. |
| Retail sales | GUS retail sales at constant prices (enterprises > 9 employees); Eurostat `sts_trtu_m` | % y/y · M | consumption | Nominal vs constant prices are different stories. |
| PPI | GUS producer prices in industry | % y/y · M | cost pressure, leads CPI goods | Energy-driven, volatile. |
| Current account | NBP balance of payments, BPM6 | EUR/PLN mln, % of GDP · M/Q | external balance | Use 4-quarter rolling sum in % of GDP for trend. |
| OECD CLI | composite leading indicator, amplitude adjusted, long-term average = 100; growth cycle vs trend, leads 6–9 months | index · M | turning points | >100 and rising = above-trend expansion; the level is not a GDP growth rate, only direction and the 100 line matter. |

## Comparison norms (which comparison for which series)

| Series | Compare | Never |
|---|---|---|
| Monthly NSA (CPI, output, retail) | y/y, same month previous year | m/m on NSA data as a trend |
| Quarterly GDP | y/y NSA (GUS headline) **and** q/q SA (Eurostat, flash) — name which | q/q on NSA data; q/q annualised without saying so |
| Annual | previous year; 5- or 10-year average for context | December y/y vs annual average as "latest" |
| Rates (inflation, growth) | change in **p.p.** | % change of a rate ("+10,3%" for 2,9 → 3,2) |
| Levels (GDP, sales) | % change, real for growth | nominal % change as "growth" |
| Countries | PPS for living standards, EUR for shares, own currency for dynamics; same concept, period, vintage | WDI vs Eurostat mixed in one ranking |
| Regions | % of national average, same NUTS version | PL12 vs PL91/PL92 in one series |
| Real vs nominal | deflate by CPI (incomes) or use volume series (output) | nominal vs real mixed on one axis |

Benchmarks per headline: CPI → NBP band; GDP growth → own long-run trend and EU27; GDP per capita → EU27 = 100;
peers = CZ, SK, HU, RO, LT (CEE) and euro area.

## Release calendar (typical; verify the exact day)

| Release | Publisher | When |
|---|---|---|
| CPI flash | GUS | last working day of the reference month |
| CPI final, with COICOP divisions | GUS | ~15th of the next month |
| Core inflation | NBP | working day after GUS final CPI |
| PPI, industrial output, wages | GUS | ~18th–21st of the next month |
| Retail sales | GUS | ~22nd–24th of the next month |
| Current account | NBP | ~ middle of the second month after |
| GDP flash (y/y NSA, q/q SA) | GUS | ~13th–15th of the second month after the quarter (≈ 45 days) |
| GDP preliminary with components | GUS | end of the second month after the quarter or first working days of the third |
| Annual GDP preliminary | GUS | end of January for the previous year |
| HICP flash (euro area) / full HICP (all EU) | Eurostat | last working day of the month or first days after / ~mid-next-month |
| OECD CLI | OECD | monthly, ~2nd week |
| WDI | World Bank | several times a year; annual data lag 6–12 months |

Sources: GUS Kalendarium https://stat.gov.pl/kalendarium/, Eurostat euro indicators
https://ec.europa.eu/eurostat/news/release-calendar, NBP https://nbp.pl. A page states the reference period and the
**source's** release date ("CPI, wrzesień 2026 (GUS, 15.10.2026)"); our load date is not a release date.
Freshness for headline slots: M ≤ 3 months, Q ≤ 6 months, A ≤ 18 months old.

## Pitfalls (check each in a review)

| Trap | Symptom on a page | Fix |
|---|---|---|
| CPI ≠ HICP | "Inflacja" with an HICP value; NBP band on HICP | Label HICP; band note "Cel NBP dotyczy inflacji CPI (GUS); HICP to zbliżona, ale inna miara" |
| y/y month vs annual average | WDI CPI next to Eurostat monthly HICP under one KPI | separate KPIs or measures; name the measure |
| Index base | index 104,3 shown as a level or "+104%" | rate = index − 100 only on the y/y base; HICP rebased 2025 = 100 and moved to ECOICOP 2 with January 2026 data (published Feb 2026): index levels and sub-index codes break, rates do not — never mix stored levels or old/new category codes |
| Nominal as growth | GDP in EUR rising "economy grows" | chain-linked volumes for growth |
| Chain-linked levels | components summed to total | use published contribution series |
| Seasonal adjustment | q/q on NSA; working-day swings read as trend | SA/calendar-adjusted series or caveat |
| Revisions | flash shown as final; no flag | show `p`/`e` flags and "szybki szacunek"; GDP revises for ~2 years, benchmark revisions ~5 years |
| Units | WDI `NY.GDP.MKTP.CD` read as millions; EUR and US$ under one unit (#2148) | unit per source row; US$ are units |
| Regional breaks and lag | regional GDP map titled with the current year | NUTS 2016 split of Mazowieckie; regional GDP ~2 years late — title the reference year |
| Source disagreement | Eurostat and WDI differ for PL | Eurostat (fed by GUS) is reference for PL/EU; WDI for non-EU comparisons |
| Ranking by relative change | "chart of the day" = biggest % move of a rate | newest release first; rates move in p.p. |

## Chart and card rules

- **Card:** name + measure (r/r, realnie), value, period in words, change in p.p. (rates) or % (levels) with the
  comparison named (a y/y rate compares with the previous period's y/y: "−0,1 p.p. vs sierpień"; a quarterly GDP
  y/y with the previous quarter's y/y), one benchmark, source, source release date, next release.
- **Colour by meaning:** inflation colour follows the distance to the target, never the direction: inside 1,5–3,5% =
  on target; outside the band on either side = warning (below 1,5% is not good news); more than 1 p.p. outside =
  alert. Unemployment up = bad; GDP growth below 0 = bad; FX and levels neutral. Colour is never the only signal:
  pair it with a word ("w celu", "powyżej celu"). The Designer picks the palette.
- **Bars start at zero**; growth rates as bars with zero line; levels and indices as lines (non-zero baseline allowed,
  say so). Target-band charts set the y domain to cover data **and** band.
- **One measure per axis**; no dual axes for two economy series; no nominal and real on one axis.
- **Small multiples with shared scales** for country or regional comparison; sort by value; Poland highlighted,
  peers grey.
- **Format:** `%` sticks to the number (`2,9%` / `2.9%`, as GUS and NBP write it); every other unit is separated by a
  non-breaking space U+00A0 (`−0,1 p.p.`, `−0.1 pp`, `1 234 mln zł`, `EUR 3.2 bn`); minus U+2212; PL decimal comma
  and space as thousands separator; one formatter for PL and EN. Precision: one decimal for rates, never more than the
  producer publishes.
- **HICP in the "Inflacja" slot** (until GUS CPI is loaded): title "Inflacja HICP, r/r", the band note above, never
  plain "Inflacja".
- **Every chart:** unit, source with table code, reference period, flags for provisional values; alt text that
  states the finding and a data table alternative.

## Review procedure

1. Read the item, `docs/domains/economy.md` and the KPI rows in `warehouse/seeds/kpi.csv`.
2. Check the definition against the producer (concept, unit, frequency, SA, real/nominal, base, breaks, vintage).
3. Walk the Pitfalls table and the chart rules; spot-check two numbers against the producer's release.
4. Run `data:validate-data` on any number shown to users (magnitude, continuity, units, period alignment).
5. Answer on the issue/PR: `Domain review (economy): approved` or the exact changes, each with the rule and a source
   link. List `Skills used:` in the comment.

## Common mistakes

- Approving a page because the number matches the data file: the file can hold the wrong measure.
- Calling a quarter "recession" from one negative q/q print (technical recession = two consecutive falls of real GDP q/q,
  seasonally and calendar adjusted, and even that is a convention, not the CEPR/NBER-style dating).
- Quoting an NBP target band without saying it is for CPI.
- Treating a WDI annual value as "latest" for Poland when Eurostat already has a newer quarter.
