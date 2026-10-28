# Population

Knowledge file of the Domain expert for the `population` domain (docs/team.md). Written for whoever defines, builds,
reviews or writes about population KPIs: how a demographer reads these numbers and which traps to avoid.

## 1. Scope

Domain `population` in `warehouse/seeds/domain.csv`, groups:

| Group | Covers | KPIs today (`warehouse/seeds/kpi.csv`) |
|---|---|---|
| `population.stock` (subgroup `population`) | population size, age and sex structure, density, ageing | `population` (published), `population_density` (draft) |
| `population.vital` | births, deaths, fertility, life expectancy, natural change | `live_births`, `deaths`, `life_expectancy` (draft) |
| `population.migration` | internal and international migration, net migration | `net_migration` (draft) |
| `population.households` | families and households | none |
| `population.census` | census results (NSP 2011, NSP 2021) | none |

Neighbours: labour force, employment and income are `labour`; dwellings and area are `housing`; health status and
causes of death are `health`. Per-capita KPIs of every other domain divide by this domain's population, so its
definition choice leaks everywhere (see § 3).

## 2. Key indicators

| Indicator | Definition | Unit | Freq. | What we use today | How to read it |
|---|---|---|---|---|---|
| Population | Number of residents at a reference date | persons | A (GUS also half-yearly) | Eurostat `DEMO_PJAN` (1 Jan, usual residence), `DEMO_R_PJANAGGR3` (NUTS), GUS BDL `P2137` and DBW (31 Dec, national definition), WDI `SP.POP.TOTL` (mid-year) | Poland's population has been falling since the mid-2010s; recent falls are about 120–160 thousand a year. GUS reported about **37.3 million** at the end of 2025 (national definition, −157 k y/y); Eurostat about **36.5 million** on 1 Jan 2025 (usual residence). Both are right — they count different people (§ 3). |
| Live births | Births of live children to residents in the year | persons | A (GUS monthly, preliminary) | Eurostat `DEMO_GIND` `LBIRTH`, GUS BDL `P1873` | About 238 thousand in 2025, down from over 400 thousand around 2017; every year since 2018 has set a new post-war low. |
| Deaths | Deaths of residents in the year | persons | A (GUS weekly/monthly, preliminary) | Eurostat `DEMO_GIND` `DEATH` | About 406 thousand in 2025. COVID-19 pushed them to about 477 thousand in 2020 and about 519 thousand in 2021; read excess mortality against a pre-2020 baseline, not against 2021. |
| Natural change | Live births − deaths | persons, per 1 000 | A | derivable from the two above | Negative in most years since 2013 and every year since 2018; about −168 thousand (−4.5 ‰) in 2025. The main driver of the decline, not emigration. |
| Total fertility rate (TFR) | Births per woman implied by this year's age-specific fertility rates | children per woman | A | not loaded | Replacement level is about 2.1. Poland is around 1.1–1.2 in 2023–2024, among the lowest in the EU. A period measure: it falls when women postpone births even if completed fertility does not. |
| Life expectancy at birth | Mean years lived by a hypothetical cohort exposed to this year's age-specific mortality | years | A | WDI `SP.DYN.LE00.IN` (both sexes) | GUS 2024: **74.9 men, 82.3 women**, a 7.3-year sex gap, wide by EU standards; regions span about 2.5 years ([GUS, Trwanie życia w 2024 r., via PAP](https://zdrowie.pap.pl/sites/default/files/202607/trwanie-zycia-w-2024-roku.pdf)). Always show it by sex; a "both sexes" figure hides the most-discussed fact. |
| Net migration | Immigrants − emigrants; in Eurostat `CNMIGRAT` = total change − natural change, so it absorbs statistical adjustments | persons | A | Eurostat `DEMO_GIND` `CNMIGRAT`, WDI `SM.POP.NETM` | For Poland a residual of definitions as much as a migration measure (§ 3). 2022 jumps with refugees from Ukraine. |
| Population density | Population ÷ total area | persons per km² | A | GUS BDL `P2425` | About 120 per km² nationally; gminas range from under 20 to several thousand. Uses total area (with inland waters); WDI uses land area. |

Not yet in the platform but expected by any demographer (backlog candidates, in order of value): TFR (Eurostat
`demo_find`, GUS); population by age group with the old-age dependency ratio (65+ ÷ 15–64, Eurostat `demo_pjanind`)
and median age; life expectancy by sex from GUS life tables or Eurostat `demo_mlexpec`; natural change per 1 000;
GUS population projection 2023–2060; persons under temporary protection (Eurostat `migr_asytpsm`).

## 3. Methodology traps

- **Two populations of Poland.** GUS's national definition counts people **registered for permanent residence**
  (plus a few adjustments). The figure Poland sends to Eurostat counts **usual residents**: people who live or intend
  to live in Poland for 12 months or more, which removes registered people living abroad for 12+ months and adds
  long-term immigrants and persons under **temporary protection from Ukraine**. The gap is about **1 million** (36.5 vs
  37.5 million at 31 Dec 2024, [Eurostat metadata for Poland](https://ec.europa.eu/eurostat/cache/metadata/EN/demo_urespop_esms_pl.htm)).
  A Polish reader who sees "ludność Polski" expects the GUS national figure; EU comparisons and EU per-capita
  indicators use usual residence. Never plot the two as one series, and say which one a page shows.
- **1 January vs 31 December.** Eurostat `DEMO_PJAN` year *Y* is the stock on 1 Jan *Y*; GUS BDL year *Y* is the
  stock on 31 Dec *Y*, i.e. one day earlier than Eurostat *Y+1*. Our Gold builds give both the period label *Y*, so a
  chart that overlays sources shifts one of them by a year. WDI `SP.POP.TOTL` is a mid-year estimate (UN World
  Population Prospects base for many countries), a third reference date.
- **Census rebasing.** NSP 2021 replaced NSP 2011 as the base of the population balance; GUS and Eurostat re-based
  their series (Eurostat's Polish monthly labour data, for example, were revised for Jan 2019–Jun 2023 in March 2024,
  [Eurostat `une_rt_m` metadata](https://ec.europa.eu/eurostat/cache/metadata/EN/une_rt_m_esms.htm)). Rates per
  1 000 and per-capita values computed on old and new bases can jump at the switch; GUS life tables use NSP 2021
  populations from 2022. Check the break year before computing long-run changes.
- **Registered migration is badly incomplete.** GUS international migration statistics count registrations and
  deregistrations of permanent residence; most emigrants never deregister and many immigrants never register.
  Registered net migration is therefore tiny and says little about real flows. Eurostat `CNMIGRAT` is a residual and
  inherits every definitional change of the stock (including the 2022 inclusion of temporary-protection holders);
  WDI `SM.POP.NETM` is a UN model estimate. Show net migration with a caveat, never as an exact count.
- **Mid-year vs end-of-year denominators.** Crude rates (births, deaths per 1 000) use the mid-year (30 June) or
  average population; per-capita KPIs of other domains often use 31 Dec or 1 Jan. Mixing them shifts per-capita
  values by up to a few tenths of a percent — small, but visible in rankings of close regions.
- **Period vs cohort.** TFR and life expectancy at birth are period measures: they describe this year's rates, not
  what a real generation experiences. A falling TFR during postponement or a COVID-year life-expectancy dip is not
  a forecast.
- **Territorial changes.** Gmina boundary changes (TERYT, every 1 January) move population between units; NUTS 2016
  split Mazowieckie into PL91 and PL92. Regional series need the boundary vintage (`housing.territory`).
- **Preliminary vs final.** GUS publishes a preliminary year-end population and vital statistics at the end of
  January and the final figures in the middle of the year; monthly births and deaths in the Statistical Bulletin are
  revised. Label preliminary values.

## 4. Standard analyses

| Analysis | Chart | KPIs needed |
|---|---|---|
| Population over time | line from 1990, with the census years and the 2021 rebasing marked; one definition per line | `population` (one source per line) |
| Components of change | bars of natural change and net migration per year, total change as a line | `live_births`, `deaths`, `net_migration` |
| Births and deaths | two lines, the crossing point (2013) marked; COVID-19 years shaded | `live_births`, `deaths` |
| Population pyramid | horizontal bars by 5-year age group and sex, now vs 2000 vs projection | population by age and sex (not loaded) |
| Ageing | old-age dependency ratio and share 65+, Poland vs EU and CEE peers | `demo_pjanind` (not loaded) |
| Fertility | TFR line with the 2.1 replacement level, Poland vs EU | TFR (not loaded) |
| Life expectancy | lines by sex, Poland vs EU; map by voivodeship | life expectancy by sex (not loaded) |
| Regional change | map of % population change 2011–2025 by powiat or gmina (suburbs of big cities grow, eastern and small-town Poland shrinks) | `population` at gmina/powiat level (GUS BDL) |
| Density | map by gmina | `population_density` |

## 5. Release calendar

Typical timing; check exact days against the GUS release calendar
([stat.gov.pl, Kalendarium](https://stat.gov.pl/kalendarium/)) and Eurostat's
([release calendar](https://ec.europa.eu/eurostat/news/release-calendar)).

| Release | Publisher | When |
|---|---|---|
| Preliminary population at 31 Dec, births, deaths of the year | GUS | end of January of the next year (e.g. 30 Jan 2026 for 2025) |
| Population by sex, age and territory (final, 31 Dec and 30 Jun) | GUS | mid-year for 31 Dec; later in the year for 30 Jun |
| Monthly births and deaths (preliminary) | GUS Statistical Bulletin | about 3–4 weeks after the month |
| Life expectancy (life tables) of the previous year | GUS | July |
| Usual-resident population sent to Eurostat | GUS → Eurostat | within 8 months of the reference year; published in Poland in June |
| EU population on 1 January and demographic balance (`demo_pjan`, `demo_gind`) | Eurostat | first release around July; final data later in the year |
| WDI population, life expectancy, net migration | World Bank | annual update around July; demographic series follow UN WPP revisions |

Pages must say which population and which date, e.g. "Ludność, 31 XII 2025, definicja krajowa (GUS, 30 I 2026)".

## 6. Audience and questions

- **Journalists and the public:** how many of us are there, how fast are we shrinking, why so few births, how long
  do we live, how many Ukrainians live here.
- **Local government:** population and its age structure per gmina for schools, care homes, transport and budgets;
  which places grow (suburbs) and which shrink.
- **Business and investors:** market size by region, ageing, labour supply (with `labour`).
- **Researchers and students:** long series, components of change, projections, EU comparison.
- **Foreign readers (EN):** Poland vs EU on fertility, ageing, life expectancy, migration since 2022.

## 7. Open points

- `population` mixes two definitions and three reference dates under one KPI: Eurostat usual residence (1 Jan),
  GUS national definition (31 Dec) and WDI mid-year, with GUS 31 Dec *Y* and Eurostat 1 Jan *Y+1* labelled as
  different years (#2209). Same class as #2184 for labour.
- `life_expectancy` has only WDI (both sexes, model-based); GUS life tables or Eurostat `demo_mlexpec` by sex are
  the reference for Poland.
- `net_migration` sources (Eurostat residual, UN model) differ in concept; no GUS measure. Needs a caveat on every
  page.
- `live_births` and `deaths` BDL measure patterns end in `%` and may catch breakdowns (same open point as the
  labour file's BDL pins).
- UN World Population Prospects is in source review (#2164, #2172); it would add projections and harmonised
  long series.
- Exact break year of the NSP 2021 rebasing in the GUS BDL series is to be confirmed from GUS notes (stat.gov.pl
  pages could not be read from agent sessions: robots).
- Live population pages (data.zohelo.com, bi.zohelo.com) were not reviewed on 28 Oct: the session could not fetch
  them.
