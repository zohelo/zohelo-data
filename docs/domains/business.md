# Business & industry

Knowledge file of the Domain expert for the `business` domain (docs/team.md). Written for whoever defines, builds,
reviews or writes about business KPIs: how a business-cycle, credit or regional analyst reads these numbers and which
traps to avoid.

## 1. Scope

Domain `business` in `warehouse/seeds/domain.csv`, groups:

| Group | Covers | KPIs today (`warehouse/seeds/kpi.csv`) |
|---|---|---|
| `business.enterprises` | business registers (REGON, CEIDG, KRS), VAT taxpayers, LEIs, business demography (births, deaths, survival) | none |
| `business.industry_construction` | sold production of industry, construction-assembly output, business tendency in manufacturing and construction | `dwellings_completed` (draft, `gus_bdl:P3824`) |
| `business.trade_services` | retail sales, wholesale, services, business tendency in trade and services | none |
| `business.agriculture` | crops, livestock, agricultural prices and land | none |
| `business.tourism` | accommodation: tourists, nights spent, occupancy | none |
| `business.finances` | financial results of non-financial enterprises (revenues, costs, net profit, margins) | none |

What we hold: GUS BDL and GUS DBW (regional and national business tables), the full Eurostat bulk download (short-term
statistics `sts_*`, business demography `bd_*`, business surveys `ei_bs*`), the GLEIF golden copy (all LEIs, daily) and
the MF VAT white list flat file (daily). Only `dwellings_completed` has a KPI, and it is a housing indicator (open
point 1). Neighbours: GDP and investment are `economy`; producer prices are `economy.prices`; employment and wages in
enterprises are `labour`; housing stock and prices are `housing`.

## 2. Key indicators

| Indicator | Definition | Unit | Freq. | What we use today | How to read it |
|---|---|---|---|---|---|
| Sold production of industry | Sales value of products and services of industry (sections B mining, C manufacturing, D energy, E water and waste), enterprises with **more than 9 employees**, index at constant prices ([GUS regional yearbook notes](https://krakow.stat.gov.pl/download/gfx/krakow/pl/defaultstronaopisowa/1121/1/1/14_przemysl_i_budownictwo.pdf)) | index, % y/y | M | not loaded (DBW / Eurostat `sts_inpr_m`) | The headline monthly activity number. Sept 2026: +4.6 % y/y, +3.3 % y/y seasonally adjusted ([XYZ, 20 Oct 2026](https://xyz.pl/?p=264288)). Manufacturing (C) is the large majority of it and drives the cycle; energy (D) moves with weather. |
| Construction-assembly output | Work done under contract by construction enterprises with more than 9 employees, constant prices; split into buildings, civil engineering, specialised works | index, % y/y | M | not loaded (DBW / `sts_copr_m`) | Very volatile and lumpy (public investment, EU funds cycle). Sept 2026: −9.0 % y/y, −7.2 % SA ([XYZ](https://xyz.pl/?p=264288)); analysts flagged irregular settlement timing that GUS has not quantified. Read 3-month averages, never one month. |
| Retail sales | Sales of goods by retail enterprises with more than 9 employees, current and constant prices, by product group | % y/y | M | not loaded (DBW / `sts_trtu_m`) | Proxy for household consumption. June 2026: +6.2 % y/y constant prices ([BANK.pl](https://bank.pl/gus-sprzedaz-detaliczna-w-cenach-stalych-w-czerwcu-26-wzrosla-o-62-proc-r-r/)). Car and fuel sales swing the total; analysts also read the series without vehicles. |
| Business climate indicator (koniunktura) | Arithmetic mean of the balances of firms' assessments of their current and expected general economic situation; balance = % "improvement" − % "deterioration" ([GUS, V Kongres Statystyki 2025](https://kongres2025.stat.gov.pl/Content/Docs/Prezentacje/Sess2/04%20-%20V%20Kongres%20Statystyki%20Polskiej%20-%20badanie%20koniunktury%20gospodarczej.pdf)) | balance, −100 to +100 | M | not loaded (DBW; EU-harmonised series in Eurostat `ei_bs*`) | Soft data, out ~1 week before the hard data of the same month. Oct 2026: manufacturing −6.6, construction −4.2, retail −0.3, finance +29.4 ([XYZ, 22 Oct 2026](https://xyz.pl/dzieje-sie/koniunktura-gospodarcza-w-pazdzierniku-2026-r-sa-dane-gus-4157)). Below 0 = more pessimists than optimists; read the level against the sector's own long-term average, not against zero across sectors. |
| Entities in REGON | Stock of national-economy entities in the REGON register at the end of the period, by legal form, NACE section, territory | number, per 10 000 population | M (national), A (BDL regional) | not loaded (BDL / DBW) | 5.44 M at end-November 2025, of which 15.1 % with suspended activity ([podatki.biz on the GUS release](https://www.podatki.biz/artykuly/podmioty-gospodarki-narodowej-w-rejestrze-regon-listopad-2025-r_16_60697.htm)). A register count, not active firms (trap below). Per 10 000 population is the standard regional comparison of entrepreneurship. |
| Registered VAT taxpayers | Number of hashed entries of active and exempt VAT taxpayers in the MF white list | number | D | Silver `biala_lista_taxpayer_hashes` (no KPI) | 3.33 M active and 0.35 M exempt on 24 Oct 2026 (`docs/sources.md`). A daily, near-real-time count of the VAT base; no geography or NACE (hashes only). |
| LEIs of Polish entities | LEI records with legal-address country PL, by registration status | number | D | Silver `gleif_legal_entities` (no KPI) | Counts entities that trade in financial instruments or report under EU rules, not the business population. Lapsed LEIs stay in the file. |
| Dwellings completed | Dwellings in new and extended buildings handed over for use, by investor form | dwellings | M (national), A (BDL) | `gus_bdl:P3824` → `dwellings_completed` (draft) | Lagging housing-cycle indicator (open point 1); read with starts and permits, which lead it by 1–2 years for multi-family buildings. |

## 3. Methodology traps

- **The "more than 9 employees" cut.** GUS monthly industry, construction and retail data cover enterprises with more
  than 9 employees only; annual data and Eurostat STS aim at all enterprises. Monthly growth from GUS and annual or
  Eurostat growth for the same year will differ. Always state the coverage in the KPI definition.
- **Working days and seasonality.** Unadjusted m/m changes are dominated by calendar and season (Sept 2026 industry:
  +17 % m/m unadjusted vs +3.4 % seasonally adjusted). Compare unadjusted data only y/y, and say how many working days
  differ; use GUS's seasonally adjusted series for m/m. Never seasonally adjust ourselves and label it as GUS.
- **Constant vs current prices.** Production and construction indices are volume measures at constant prices; retail
  sales are published in both. A "sales up 5 %" headline in current prices can be a volume fall when inflation is
  high. Regional yearbook tables of sold production are in **current** prices: do not mix them with the monthly
  volume indices.
- **Revisions.** The monthly releases are preliminary and the previous month is revised with the next release; index
  bases change with rebasing. Store the publication date and replace whole series, never append.
- **Register counts are not active firms.** REGON keeps entities with suspended activity (about 15 %, 96 % of them
  sole traders) and dormant entities that were never deregistered, so it overstates the business population. Active
  enterprises come from GUS structural surveys and Eurostat business demography (`bd_*`, annual, with a lag of about
  two years). Name the measure: "registered entities", "active enterprises" or "VAT taxpayers" are three different
  numbers.
- **Registrations are driven by law, not only the economy.** Tax changes (e.g. lump-sum and health-contribution
  rules), the "mały ZUS" thresholds and B2B contracting push sole-proprietorship registrations up or down. A jump in
  new registrations needs a check for a legal change before it is called an entrepreneurship boom.
- **Climate indicators are not comparable across sectors or with the EU ESI.** The GUS climate indicator averages
  current and expected general situation; the EU industrial confidence indicator averages production expectations,
  order books and stocks (inverted). Same survey programme, different formula: never put the GUS climate and the
  ESI on one axis.
- **White-list hashes.** The flat file carries SHA-512 hashes of date + NIP + account, not NIPs: one taxpayer with
  several accounts appears several times. The count of hashes is therefore a count of (taxpayer, account) pairs, not
  of taxpayers, unless we prove otherwise (open point 3).
- **LEI coverage is regulatory.** LEI counts jumped with MiFID II (January 2018) and fall when LEIs are not renewed
  (status LAPSED). Never present them as the number of companies.
- **Dwellings are counted when handed over**, not when built; completions bunch in December. Use annual totals or
  12-month rolling sums.

## 4. Standard analyses

- **Monthly activity dashboard**: industry, construction and retail, each as y/y unadjusted and m/m seasonally
  adjusted, with a 3-month average for construction; the GUS climate indicator next to them as the leading signal.
  KPIs needed: `industrial_production`, `construction_output`, `retail_sales` (constant prices), `business_climate`
  per sector.
- **Contributions by section and division**: which manufacturing divisions drive industrial growth (automotive,
  food, metals, electronics); needs the index by NACE division and weights.
- **Regional entrepreneurship**: REGON entities and sole traders per 10 000 population by voivodeship and powiat
  (BDL); a choropleth with the national average as reference.
- **Business demography**: registrations, deregistrations, suspensions per month; birth and death rates and 3-year
  survival from Eurostat `bd_*` for EU comparison.
- **VAT base tracker**: daily active and exempt white-list counts, 7-day change; the only daily business indicator we
  hold.
- **EU comparison**: Eurostat STS indices (2021 = 100 base in Eurostat) for Poland vs EU and neighbours.

## 5. Release calendar

GUS publishes the monthly business data in the third and fourth week of the following month (dates in the
[GUS release calendar](https://stat.gov.pl/kalendarium/)):

| Release | Typical date | Example |
|---|---|---|
| Sold production of industry | ~ 19th–21st of month t+1 | Sept 2026 on 20 Oct 2026 |
| Construction-assembly output, housing construction | 1–2 days after industry | |
| Business tendency (koniunktura), all sectors | ~ 22nd of the **same** month | Oct 2026 on 22 Oct 2026 |
| Retail sales | ~ 22nd–24th of month t+1 | Sept 2026 around 22 Oct 2026 |
| REGON monthly information | about 3 weeks after month end | |
| Eurostat STS (`sts_*`) | about 6 weeks after month end | |
| Business demography (`bd_*`), BDL annual tables | annual, 1–2 years after the reference year | |
| MF white list, GLEIF | daily (MF ~22:30 Warsaw the evening before; GLEIF 3× a day) | |

Our DBW and BDL pipelines check daily for updated files; a page should show the GUS reference month and our load
date, not "today".

## 6. Audience and questions

- **Business owners and SMEs**: "Is demand in my sector growing?", "How many competitors are there in my powiat?"
- **Bank, credit and investment analysts**: "Is industry turning?", "What does the climate indicator say about next
  month?", "Which sectors are pessimistic?"
- **Local governments and investors choosing a location**: "How entrepreneurial is this region?", "Are firms opening
  or closing here?"
- **Journalists**: "Record number of new firms?", "Construction in recession?" — they need the context traps above.
- **Compliance users**: VAT white-list and LEI questions are about single entities; zohelo publishes aggregates only.

## 7. Open points

1. `dwellings_completed` sits in `business.industry_construction`; users look for it under housing
   (`housing.housing`). Proposed to the Product Owner on Product inbox #2074.
2. No KPI yet for industrial production, construction output, retail sales, business climate or REGON entities: the
   DBW indicator ids and BDL subgroups that hold them are not yet mapped (to do in the KPI proposal).
3. White list: confirm whether a taxpayer with several accounts gives several hashes (spec:
   [MF flat-file specification](https://www.gov.pl/attachment/68670a01-3b94-44ac-a9fd-4ce3b4f0b519)) before a
   "number of VAT taxpayers" KPI is defined.
4. Base year of GUS constant-price indices and of the DBW series as loaded: check against the DBW metadata before the
   first index KPI.
5. GLEIF Silver: confirm the legal-address country and registration status fields are typed, so PL counts by status
   are possible.
