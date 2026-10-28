# Labour & income

Knowledge file of the Domain expert for the `labour` domain (docs/team.md). Written for whoever defines, builds,
reviews or writes about labour KPIs: how a labour-market analyst reads these numbers and which traps to avoid.

## 1. Scope

Domain `labour` in `warehouse/seeds/domain.csv`, groups:

| Group | Covers | KPIs today (`warehouse/seeds/kpi.csv`) |
|---|---|---|
| `labour.employment` (subgroups `lfs`, `registered_unemployment`) | employment, unemployment, activity, vacancies | `unemployment_rate` (published), `employment_rate`, `registered_unemployment_rate` (draft) |
| `labour.wages` | wages, labour costs, minimum wage | `average_gross_wage` (draft) |
| `labour.income_living` | household income, poverty, inequality (EU-SILC, household budget survey) | none |
| `labour.social_protection` | pensions, benefits, social insurance (ZUS, KRUS) | none |

Neighbours: CPI (for real wages) and GDP (for productivity) are `economy`; population denominators are
`population`; enterprise employment by size class is `business`.

## 2. Key indicators

| Indicator | Definition | Unit | Freq. | What we use today | How to read it |
|---|---|---|---|---|---|
| Unemployment rate (LFS, ILO) | Unemployed (no work in the reference week, looked for work in the last 4 weeks, available within 2 weeks) ÷ economically active, ages 15–74 | % of labour force | A, M (Eurostat), Q (GUS BAEL) | `eurostat:UNE_RT_A:PC_ACT` (priority 1), `eurostat:UNE_RT_M:PC_ACT` (monthly, SA), WDI `SL.UEM.TOTL.ZS` | Poland is among the lowest in the EU: about 3 % (3.3 % in Aug 2026 vs EU 6.2 %; record low 2.6 % in Jan 2025, per Eurostat). Moves of 0.1–0.2 pp are within the survey noise. |
| Registered unemployment rate | Registered unemployed (labour offices) ÷ **civilian** economically active population, excluding budget units in defence and public security ([GUS definition](https://stat.gov.pl/metainformacje/opis-wskaznikow-gus/wielkosci-i-wskazniki-oglaszane-gus/przecietna-stopa-bezrobocia-w-kraju-oraz-na-obszarze-powiatow/)) | % | M (GUS), A end of year (BDL) | `gus_bdl:P2392:%` (end of year, down to powiat) | The number Polish media call "bezrobocie". About twice the LFS rate (5.9 % in May 2026). Strongly seasonal (peaks Jan–Feb). Range across voivodeships about 4 % (Wielkopolskie) to 9 % (Warmińsko-mazurskie). It is the only unemployment measure available for powiats. |
| Employment rate | Employed ÷ population of the age group | % of population | A (Eurostat), Q (GUS) | `eurostat:LFSI_EMP_A:PC_POP:EMP_LFS` (**20–64**), WDI `SL.EMP.TOTL.SP.ZS` (**15+**, ILO modelled) | The EU 2030 headline target is 78 % (ages 20–64). Poland was 78.8 % in 2025, with Warsaw region the EU's highest at 86.9 %. The 20–64 and 15+ rates differ by more than 20 pp and are different indicators. |
| Average gross wage | Gross monthly pay of employees (incl. bonuses, before PIT and employee social contributions) | PLN | M (enterprise sector), Q and A (national economy) | `gus_bdl:P2497:%` (annual, voivodeship and powiat) | Nominal. Wage growth only means more purchasing power after deflating with CPI (real wage = nominal index ÷ CPI index). The mean is pulled up by high earners: about two-thirds of employees earn less than the average. |
| Real wage growth | Nominal wage index ÷ CPI index | % y/y | M, A | not loaded | The indicator that matters for households and consumption. It was negative in 2022–early 2023 (inflation above wage growth). |
| Activity rate | Labour force ÷ population of the age group | % | A, Q | not loaded (Eurostat `lfsi_emp_a`, indic `ACT`) | Pairs with employment: rising activity can raise unemployment while employment also grows. |

Not yet in the platform but expected by any analyst (backlog candidates, in order of value):

- GUS monthly enterprise-sector wage and employment (the "przeciętne wynagrodzenie" headline, around the 20th).
- Real wage index.
- Registered unemployment monthly, at national and voivodeship level.
- Minimum wage (yearly regulation).
- Labour cost index (Eurostat `lc_lci_r2_q`).
- Job vacancies (GUS "Popyt na pracę", Eurostat `jvs_q_nace2`).
- LFS unemployment by NUTS 2 (`lfst_r_lfu3rt`).
- Median wage (GUS wage-distribution survey).
- At-risk-of-poverty rate (EU-SILC `ilc_li02`).

## 3. Methodology traps

- **LFS unemployment ≠ registered unemployment.** LFS (BAEL) is a household survey on the ILO definition. Registered
  unemployment is an administrative count. Some registered people work informally or are not looking for work, and
  some LFS unemployed never register. The two rates differ in level (≈3 % vs ≈6 %) and can move in opposite
  directions. Never plot them as one series or under one title, and name the measure in every title ("BAEL / LFS"
  vs "rejestrowane / registered").
- **The 2025 labour-market act is a break in registered unemployment.** The act on the labour market and
  employment services is in force since **1 June 2025** (Dz.U. 2025 poz. 620). It lets people register at their
  place of residence instead of their registered address, opens registration to farmers regardless of farm size,
  and requires contact with the office only every 90 days ([MRPiPS reply to interpellation 15491](https://api.sejm.gov.pl/sejm/term10/interpellations/attachment/ATTDVAHZ3/i15491-o1.pdf)).
  Registered unemployment rose by about 120–134 thousand in a year (5.4 % in March 2025 → 6.1 % in March 2026). Commentators
  attribute this mainly to the act, not to a weaker labour market ([Parkiet, 24 Apr 2026](https://www.parkiet.com/gospodarka-krajowa/art44229521-stopa-bezrobocia-w-marcu-bez-zmian-niski-sezonowy-spadek-liczby-bezrobotnych)).
  Pages showing 2025+ registered unemployment must say so. To compare trends across 2025, use the LFS rate.
- **BAEL 2021 redesign.** From 2021 BAEL follows the EU IESS framework regulation. Contributing family workers in a
  relative's business are now counted as employed, as are people on parental leave with a job and part-season
  workers. Subsistence farmers producing mainly for their own use are no longer counted ([GUS notice](https://stat.gov.pl/obszary-tematyczne/rynek-pracy/pracujacy-bezrobotni-bierni-zawodowo-wg-bael/informacja-glownego-urzedu-statystycznego-na-temat-zmian-wprowadzanych-od-2021-r-w-bael,35,1.html)).
  GUS published recalculated results for 2010–2020. Eurostat series carry break flags (`b`) for 2021. The jump in
  the employment rate 20–64 from 72.7 % (2020) to 75.4 % (2021) is partly this break. Never compute 2020→2021
  changes from unadjusted series.
- **Age groups.** Eurostat unemployment covers ages 15–74. The EU employment target uses 20–64. GUS BAEL headline
  tables use ages 15–89 since 2021. WDI uses 15+ and ILO modelled estimates (not the national survey). Rates for
  different age groups are different indicators, so state the age group in the title.
- **Monthly LFS unemployment is modelled.** Eurostat's monthly rates (`UNE_RT_M`) are estimated from the quarterly
  LFS, are revised, and come as NSA, SA and trend series ([metadata](https://allthatstats.com/en/statistics/une_rt_m/unemployment-by-sex-and-age-monthly-data/notes)).
  Use SA for month-on-month reading and NSA annual data for yearly levels. Never mix monthly and annual points in
  one line (both are `source_system=eurostat` under `unemployment_rate`).
- **Registered rate denominators** are estimates of the civilian active population that GUS updates. Treat
  published monthly rates as subject to revision when a new denominator is introduced, and prefer the latest
  vintage over stored old values.
- **Wage coverage.** GUS's monthly "sektor przedsiębiorstw" wage covers entities with **more than 9 employees**.
  The quarterly and annual "gospodarka narodowa" average is broader. Voivodeship and powiat wages in BDL exclude
  entities employing up to 9 persons ([GUS Warszawa, Wynagrodzenia 2024](https://warszawa.stat.gov.pl/files/gfx/warszawa/pl/defaultstronaopisowa/936/1/2/wynagrodzenia_2024.pdf)).
  These are different numbers. Name the coverage, and never compare a monthly enterprise-sector wage with an annual
  national-economy wage.
- **Wage seasonality.** December (year-end bonuses) and March (annual bonuses, mining and energy) spike, so
  month-on-month changes mislead. Use y/y.
- **Nominal vs real.** All PLN wages are nominal. Comparisons over more than a year need CPI deflation (GUS CPI, not
  HICP, for a Polish audience; see [economy.md](economy.md)).
- **Capital effect.** Mazowieckie's average wage and employment rate are lifted by Warsaw. NUTS 2 splits
  Mazowieckie into PL91 (Warszawski stołeczny) and PL92 (Mazowiecki regionalny), so Eurostat regional series do not
  map 1:1 to the 16 voivodeships. Treat this in the geography mapping, not by summing.
- **Regional LFS reliability.** LFS estimates for voivodeships have wide sampling errors, and Eurostat flags them
  (`u`, low reliability). Show flags, and do not rank voivodeships on differences below about 0.5 pp.
- **Open BDL measure patterns.** `kpi_measure.csv` patterns like `gus_bdl:P2497:%` match every measure of the BDL
  subgroup (all units and labels). If the subgroup holds a relative measure (Polska = 100) or several
  sub-indicators, the KPI mixes them. Pin the unit before publishing.

## 4. Standard analyses

- **Headline trio:** LFS unemployment, employment rate 20–64 and activity rate on one page, each against EU27 and
  CEE peers (DE, CZ, SK, LT, HU).
- **Two unemployment measures side by side** (separate charts, same period axis), with the June 2025 act marked.
- **Regional map and ranking:** registered unemployment by voivodeship and powiat (the only measure at powiat
  level), with the national rate as a reference line; spread (max − min) over time.
- **Real wages:** nominal wage y/y, CPI y/y and their difference (real wage growth). Wage by voivodeship relative to
  the national average (Polska = 100).
- **Wage vs minimum wage:** the minimum-to-average ratio, which is policy-relevant because the minimum wage rises
  faster.
- **Seasonality-aware reading:** y/y for registered unemployment and wages; SA series for month-on-month changes.
- **Demography link:** labour force vs working-age population (with `population`) to show labour shortages.

## 5. Release calendar

Check exact days against the [GUS release calendar](https://stat.gov.pl/kalendarz-wynikowy/) and
[Eurostat's](https://ec.europa.eu/eurostat/news/release-calendar).

| Release | Publisher | When (typical; confirm in the calendar) |
|---|---|---|
| Registered unemployment, flash estimate | MRPiPS | first days of the following month |
| Registered unemployment, official rate | GUS (monthly "Informacja o sytuacji społeczno-gospodarczej kraju") | around the 24th–25th of the following month |
| Enterprise-sector wages and employment | GUS | around the 20th of the following month |
| National-economy average wage | GUS | quarterly, in the month after the quarter; annual in February (announced in Monitor Polski) |
| BAEL quarterly results | GUS | about 2–3 months after the quarter |
| Monthly unemployment, euro indicators (`une_rt_m`) | Eurostat | about one month after the reference month |
| Annual LFS (`une_rt_a`, `lfsi_emp_a`) | Eurostat | April–May of the following year |
| BDL annual data (`P2392`, `P2497`) | GUS BDL | gradually, mostly 6–12 months after the year |
| Registered unemployment for powiats (reference 30 June) | GUS, Monitor Polski | by 31 December each year |

Pages must name the reference period and the publisher's date, e.g. "Stopa bezrobocia rejestrowanego, koniec 2025,
GUS BDL".

## 6. Audience and questions

- **Journalists and general public:** is unemployment rising, what is the average wage, how does my voivodeship or
  powiat compare, are real wages growing.
- **Job seekers and people moving:** where are wages higher, where is unemployment low.
- **Employers and HR:** wage growth in my region and sector, labour shortages, minimum wage changes.
- **Local government:** powiat unemployment against peers (it also enters EU-funds and labour-office allocation).
- **Analysts and economists:** labour-market tightness (unemployment vs vacancies), real wages vs productivity, the
  effect of migration and demography on labour supply, comparisons with CEE.
- **Foreign readers (EN):** Poland's very low LFS unemployment and the gap with the registered figure.

## 7. Open points

- `unemployment_rate` combines `UNE_RT_A` and `UNE_RT_M` under one source system. Published views and `latest.json`
  must pick one frequency (annual for the KPI default `A`). See the domain review on zohelo-bi#224.
- `employment_rate` combines Eurostat 20–64 and WDI 15+. Either split into two KPIs or keep WDI out of the
  default views (zohelo-bi#224).
- `registered_unemployment_rate` unit is `pct`, while LFS uses `pct_labour_force`. Decide whether it needs its own
  unit key (denominator: civilian economically active population).
- Pin `gus_bdl:P2497:%` (average wage) and `gus_bdl:P2392:%` to the intended measure before publishing. Confirm with
  a number check against GUS: national average for 2024 and one voivodeship.
- The unemployment fixture `warehouse/published/fixtures/kpi/unemployment_rate/latest.json` shows a Eurostat
  voivodeship value, but `UNE_RT_A` is national only. Regional LFS needs `lfst_r_lfu3rt` (NUTS 2) and the PL91/PL92
  mapping.
- GUS's monthly median wage (wage-distribution survey, published since 2024) is not researched yet. Find its table
  and terms before proposing a KPI.
- Some stat.gov.pl pages refuse automated reading (robots), including the BAEL 2010–2020 recalculation notice and
  GUS wage-methodology comments. Verify their details by hand or through a Dev run.
