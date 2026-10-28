# Economy

Knowledge file of the Domain expert for the `economy` domain (docs/team.md). Written for whoever defines, builds,
reviews or writes about economy KPIs: how a macro analyst reads these numbers and which traps to avoid.

## 1. Scope

Domain `economy` in `warehouse/seeds/domain.csv`, groups:

| Group | Covers | KPIs today (`warehouse/seeds/kpi.csv`) |
|---|---|---|
| `economy.national_accounts` (subgroup `gdp`) | GDP level, growth, per capita, components | `gdp_current_prices` (published), `gdp_volume_growth`, `gdp_per_capita` (draft) |
| `economy.business_cycle` | short-term indicators: industrial output, retail sales, construction, sentiment | none |
| `economy.prices` (subgroup `consumer_prices`) | consumer and producer prices, inflation | `inflation` (published) |
| `economy.investment` | gross fixed capital formation, investment outlays | none |
| `economy.trade_bop` | foreign trade, current account, balance of payments | none |

Neighbours: interest rates, exchange rates and public debt are `finance`; wages and unemployment are `labour`;
industry and construction output by enterprise are `business`. Stories about the economy usually combine them
(real wages = labour wage ÷ economy CPI).

## 2. Key indicators

| Indicator | Definition | Unit | Freq. | What we use today | How to read it |
|---|---|---|---|---|---|
| GDP, current prices | Gross domestic product at market prices (ESA 2010, B1GQ) | million EUR (Eurostat), US$ (WDI) | A | `eurostat:NAMA_10_GDP:CP_MEUR:B1GQ`, `world_bank_wdi:NY.GDP.MKTP.CD` | Size of the economy. In EUR or USD it moves with the exchange rate, so a rise is not growth. Poland's own headline is in PLN (Eurostat `CP_MNAC`). |
| GDP volume growth | % change of chain-linked volumes on the previous year | % | A | `eurostat:NAMA_10_GDP:CLV_PCH_PRE:B1GQ`, WDI `NY.GDP.MKTP.KD.ZG` | Real growth. Poland's long-run trend is roughly 3–4 % a year; 2020 was the only post-1991 contraction. Below 2 % is weak for Poland, above 5 % is a boom. |
| GDP per capita | GDP ÷ average population | EUR, US$ per person | A | `eurostat:NAMA_10_PC:CP_EUR_HAB:B1GQ`, WDI `NY.GDP.PCAP.CD` | For living-standard comparisons professionals use **PPS, EU27 = 100** (Eurostat `NAMA_10_PC`, unit `CP_PPS_EU27_2020_HAB`, or `prc_ppp_ind` volume index), not EUR at market rates. |
| Inflation (headline) | In Poland: **CPI** of GUS, y/y change of the consumer price index, same month of the previous year = 100 | % | M | not loaded yet; we publish **HICP** `eurostat:PRC_HICP_MANR:RCH_A` and WDI `FP.CPI.TOTL.ZG` (CPI, annual average) | The NBP inflation target is **2.5 % ± 1 pp for CPI** ([NBP](https://nbp.pl/wp-content/uploads/2022/09/cel-polityki-pienieznej.pdf)). Inside the band is "on target"; the 2023 peak was in double digits. |
| Core inflation | CPI excluding food and energy (NBP publishes four core measures) | % | M | not loaded | What the Monetary Policy Council (RPP) watches for persistent pressure. Headline minus core ≈ the food and energy effect. |

Not yet in the platform but expected by any analyst (backlog candidates, in order of value): quarterly GDP
(`namq_10_gdp`) with its demand components; GUS CPI with COICOP divisions; core inflation (NBP); industrial
production (sold output of industry); retail sales at constant prices; PPI; current-account balance (NBP);
GDP per capita in PPS.

## 3. Methodology traps

- **CPI ≠ HICP.** GUS CPI weights come from the household budget survey
  ([GUS methodology](https://stat.gov.pl/metainformacje/opis-wskaznikow-gus/wielkosci-i-wskazniki-oglaszane-gus/wskaznik-cen-towarow-i-uslug-konsumpcyjnych));
  HICP follows the domestic concept (spending on the territory, including non-residents) with weights mainly from
  national accounts ([Eurostat HICP metadata, example](https://ec.europa.eu/eurostat/cache/metadata/EN/prc_hicp_esmshi4_de.htm)).
  The two can differ by several tenths of a point and occasionally more. A Polish reader who sees "inflacja"
  expects CPI. Label HICP always as "HICP" and use it only for EU comparisons.
- **Monthly y/y vs annual average.** `PRC_HICP_MANR` is the y/y rate of one month; WDI `FP.CPI.TOTL.ZG` is the
  annual-average rate. December y/y and the annual average of the same year are different numbers. Never put them
  side by side as "the latest inflation".
- **Index bases.** GUS publishes the CPI as an index (e.g. 104.3 = +4.3 %) against several bases: same month of the
  previous year = 100, previous month = 100, December of the previous year = 100, and annual average. The rate is the
  index minus 100 only for the y/y base. HICP index levels were rebased to **2025 = 100 from January 2026** together
  with ECOICOP version 2 (same Eurostat metadata); rates are unaffected, but index levels stored before and after
  the rebasing cannot be mixed.
- **Real vs nominal.** Current-price GDP grows with prices; growth claims use chain-linked volumes (`CLV_*`).
  Chain-linked levels are not additive (components do not sum to the total) — compute contributions from the
  published contribution series, not by adding levels.
- **Currency.** GDP in EUR or US$ mixes real growth, inflation and the exchange rate. A strong złoty can "raise"
  Poland's GDP in EUR without any extra output. Use PLN for Polish dynamics, EUR for EU shares, PPS for living
  standards. WDI `NY.GDP.MKTP.CD` is in US dollars, not millions (#2148).
- **Seasonal adjustment.** GUS headline quarterly GDP growth is y/y on not seasonally adjusted data; q/q growth is
  only meaningful on seasonally (and calendar) adjusted data. Monthly industry and retail data swing with working
  days; y/y comparisons need the calendar-adjusted series or a caveat.
- **Revisions.** GDP goes flash → preliminary → revised over about two years, plus benchmark revisions every five
  years or so. Eurostat flags provisional values `p` and estimates `e`; show the flag, and never call a flash value
  final. HICP is generally not revised.
- **Regional breaks.** NUTS 2016 split Mazowieckie into PL91 Warszawski stołeczny and PL92 Mazowiecki regionalny;
  regional GDP for the old PL12 does not continue. Regional GDP (`nama_10r_2gdp`, GUS) arrives about two years after
  the reference year, so a regional GDP map is always old.
- **Source disagreement.** WDI repackages Eurostat/GUS data with a lag and its own revisions, and converts with
  annual average exchange rates. When Eurostat and WDI differ for Poland, Eurostat (fed by GUS) is the reference;
  WDI is for comparisons with non-EU countries.

## 4. Standard analyses

| Analysis | Chart | KPIs needed |
|---|---|---|
| Growth over time | bars of y/y real GDP growth, recession years marked | `gdp_volume_growth` (quarterly when loaded) |
| Contributions to growth | stacked bars: consumption, investment, government, net exports, inventories | quarterly GDP components (not loaded) |
| Inflation vs target | line of CPI y/y with the 1.5–3.5 % band shaded, core inflation as a second line | GUS CPI, NBP core (not loaded) |
| Inflation decomposition | stacked contributions by COICOP division (food, energy, core) | CPI by division (not loaded) |
| Convergence | GDP per capita in PPS, EU27 = 100, Poland and CEE peers (CZ, SK, HU, RO, LT) | GDP per capita PPS (not loaded) |
| Peer comparison | HICP y/y and GDP growth for Poland vs CEE and euro area | `inflation` (HICP), `gdp_volume_growth` |
| Real wages | wage growth deflated by CPI | `average_gross_wage` (labour) and CPI |
| Regional | GDP per capita by voivodeship as % of the national average | regional GDP (not loaded) |

## 5. Release calendar

Typical timing; check exact days against the GUS release calendar
([stat.gov.pl, Kalendarium](https://stat.gov.pl/kalendarium/)) and Eurostat's
([euro indicators](https://ec.europa.eu/eurostat/news/release-calendar)).

| Release | Publisher | When |
|---|---|---|
| CPI flash estimate | GUS | last working day of the reference month |
| CPI final (with divisions) | GUS | around the 15th of the following month |
| Core inflation | NBP | the working day after the GUS final CPI |
| HICP (all EU countries) | Eurostat | about 15–18 days after the reference month |
| GDP flash (quarterly, y/y NSA and q/q SA) | GUS | about 45 days after the quarter |
| GDP preliminary with components | GUS | end of the second month after the quarter |
| Annual GDP preliminary estimate | GUS | end of January for the previous year |
| Annual national accounts (`nama_10_*`) | Eurostat | updated after each quarterly transmission |
| WDI | World Bank | several updates a year; annual data lag 6–12 months |

Pages must say the reference period and the publication date, e.g. "CPI, September 2026 (GUS, 15 Oct 2026)".

## 6. Audience and questions

- **Business journalists:** what is inflation now, is it above target, what will the RPP do, is growth speeding up.
- **Analysts and investors:** quarterly growth and its drivers, core vs headline, Poland vs CEE peers, revisions.
- **Small firms and local government:** prices they pay (energy, food), regional position, indexation (CPI annual
  average is used for statutory indexation, e.g. pensions and rents).
- **Students and teachers:** long series, how GDP and CPI are defined, convergence with the EU.
- **Foreign readers (EN):** Poland in EU comparison: GDP per capita in PPS, growth, HICP.

## 7. Open points

- #2148 — `gdp_current_prices` latest.json labels EUR and US$ values with one unit (`mln_ncu`).
- `inflation` mixes HICP monthly y/y with WDI CPI annual average under one KPI and lacks GUS CPI, the Polish
  headline (proposal on Product inbox #2074).
- Release days in § 5 are typical timings; stat.gov.pl calendar pages could not be read from agent sessions
  (robots). Confirm them when a calendar source is loaded.
- Live economy pages (data.zohelo.com, zohelo.com) were not reviewed on 28 Oct: the session could not fetch them.
