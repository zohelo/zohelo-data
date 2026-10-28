# Finance

Knowledge file of the Domain expert for the `finance` domain (docs/team.md). Written for whoever defines, builds,
reviews or writes about finance KPIs: how a treasury, banking or public-finance analyst reads these numbers and which
traps to avoid.

## 1. Scope

Domain `finance` in `warehouse/seeds/domain.csv`, groups:

| Group | Covers | KPIs today (`warehouse/seeds/kpi.csv`) |
|---|---|---|
| `finance.fx_gold` (subgroup `exchange_rates`) | NBP exchange rates (tables A, B, C), NBP gold price | `fx_rate` (published), `gold_price` (draft) |
| `finance.money_banking` | NBP policy rates, money supply (M3), credit, deposits, interbank rates | none |
| `finance.public_finance` | general government deficit and debt (EDP), state public debt, state budget | none |
| `finance.capital_markets` | government bond yields, equity indices | none |
| `finance.insurance_pensions` | insurance and pension funds | none |
| `finance.local_government` | budgets and debt of gminy, powiaty, voivodeships | none |

What we hold: NBP Web API (tables A/B/C since 2002-01-02, gold since 2013-01-02; `ingestion/sources/nbp.yaml`) and
the full Eurostat bulk download, which already contains the finance datasets analysts use (`gov_10dd_edpt1` deficit
and debt, `irt_lt_mcby_m` long-term bond yields, `ert_bil_eur_d` euro reference rates) although no KPI points at them.
Neighbours: inflation and GDP are `economy` (real rates and debt-to-GDP need both); wages are `labour`.

## 2. Key indicators

| Indicator | Definition | Unit | Freq. | What we use today | How to read it |
|---|---|---|---|---|---|
| Average exchange rate (table A) | NBP mid rate of ~32 currencies; EUR from the market at 11:00, others from EUR crosses at 11:00 | PLN per 1 unit (see trap on multipliers) | D (business days) | `nbp:fx:A:mid` → `fx_rate` | **The** official rate in Poland: used for tax, accounting and court conversions (rate of the previous business day). A rise of EUR/PLN = weaker złoty. EUR/PLN has stayed mostly in the 4.2–4.7 band since 2010; a 1 % move in a day is large. |
| Average exchange rate (table B) | Mid rate of ~115 other currencies, set weekly | PLN per 1 or more units | W (Wednesday) | Bronze/Silver only (`nbp_fx_rates`, table B) | Reference for exotic currencies; one value a week, so never chart it with daily table A rates without saying so. |
| Buy/sell rates (table C) | NBP bid and ask of 13 currencies: market at 16:00, ± 1 % | PLN per unit | D | `nbp:fx:C:bid`, `nbp:fx:C:ask` | A reference spread, not what a bank or kantor charges. Customs and some contracts use it. |
| Gold price | "Wyliczona w NBP cena 1 g złota (w próbie 1000)" | PLN per gram | D | `nbp:gold:price` → `gold_price` | Moves with the USD gold price and USD/PLN together; a gold rally in PLN can be mostly a weak złoty. Not the price of coins or bars (those carry premiums). |
| NBP reference rate | Main policy rate set by the Monetary Policy Council (RPP); with lombard, deposit and rediscount rates | % per year | at decisions | not loaded | 3.75 % after the October 2026 meeting ([bank.pl, 7 Oct 2026](https://bank.pl/wszystkie-stopy-procentowe-na-niezmienionym-poziomie-pazdziernik-26/)). Real policy rate = reference rate − CPI y/y; negative means loose policy. |
| General government deficit and debt (EDP) | ESA 2010, Maastricht definition, % of GDP | % of GDP, million PLN | A (Q) | not loaded (in Eurostat bulk: `gov_10dd_edpt1`) | 2025: deficit 7.3 %, debt 59.7 % of GDP ([GUS, April 2026 EDP notification](https://new.stat.gov.pl/en/publications-portal/general-government-deficit-and-debt-years-2022-2025)). EU reference values 3 % and 60 %; Poland is in the excessive deficit procedure since 2024. |
| State public debt (PDP) | Polish national definition (ustawa o finansach publicznych) | million PLN, % of GDP | M (MF), A | not loaded | Lower than EDP debt (it excludes e.g. funds at BGK and PFR). The Polish legal limits (60 % constitutional, 55 % prudential) apply to this figure, not to EDP debt. |
| 10-year government bond yield | Maastricht convergence yield | % | M | not loaded (in Eurostat bulk: `irt_lt_mcby_m`) | Cost of state borrowing; the spread to the German Bund is the market's risk premium for Poland. |
| M3 money supply | Broad money, NBP monetary statistics | million PLN, % y/y | M | not loaded | Credit and deposit dynamics; nominal, so compare with inflation. |

## 3. Methodology traps

- **Quotation multipliers.** NBP tables quote some currencies per 100 or 10 000 units: in table A JPY, HUF, ISK,
  KRW, CLP and INR per 100, IDR per 10 000 ([table A](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-a/));
  table B has 45 such currencies, e.g. VND and IRR per 10 000
  ([table B](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-b/)). Our Silver treats API values as PLN per
  1 unit. Any comparison with the HTML or XML tables must apply the multiplier, and the unit label must say
  "per 1 unit" (open point below).
- **Effective date vs trading date.** Table A/B `effectiveDate` is the publication day. Table C is computed from
  16:00 quotes and published for the **next** business day: its `tradingDate` is the day before its `effectiveDate`.
  Never line up table A and table C of the same `effective_date` as one market moment.
- **"Kurs NBP z dnia poprzedzającego".** Polish tax and accounting law convert at the table A rate of the last
  business day **before** the transaction. A page that answers "which rate do I use for an invoice dated X" must show
  the previous business day's table and its number (e.g. `210/A/NBP/2026`), not the rate of day X.
- **No rates on holidays.** Tables are not published on Polish public holidays and weekends; a daily series has gaps.
  Monthly or annual averages must be averages of published days, not calendar days, and must say so. On
  low-liquidity days (Good Friday, 24 and 31 December) table C is computed like table A (resolution of the NBP
  Management Board; since January 2026 resolution 44/2025, previously 51/2002).
- **Direction of quotes.** NBP rates are PLN per foreign unit (EUR/PLN 4.25 = 4.25 zł for 1 euro). Eurostat and the
  ECB quote foreign units per 1 euro (`ert_bil_eur_d`: PLN per EUR, but USD per EUR). Inverting or crossing them
  changes the reading of "up" and "down".
- **Two debts, two limits.** EDP (Maastricht) general government debt follows ESA 2010 and the Manual on Government
  Deficit and Debt; state public debt (PDP) follows the Public Finance Act. The constitutional 60 % limit
  (Konstytucja art. 216 ust. 5) and the 55 % prudential threshold apply to PDP; the EU 60 % reference value applies to
  EDP debt. For 2025 EDP debt was 59.7 % of GDP while the Ministry of Finance projected PDP under 55 %
  ([ISBnews via inwestycje.pl, Aug 2025](https://inwestycje.pl/gospodarka/dlug-sektora-general-govt-to-604-pkb-na-koniec-2025-i-668-pkb-na-koniec-2026/)). Always name which debt a chart shows.
- **Debt-to-GDP moves with GDP.** The ratio falls with high nominal GDP growth (inflation) even when debt rises.
  Show debt in PLN and its ratio side by side; use the same GDP vintage as the debt release.
- **Revisions and notifications.** EDP figures are notified twice a year (April, October) and revised for four
  years back; GUS published a correction a day after the April 2026 release. Store the notification vintage.
- **Nominal vs real rates.** Interest rates and M3 growth are nominal. Compare with CPI (Polish headline), not HICP,
  for the Polish real rate.
- **Reference-rate reform.** WIBOR is to be replaced by a risk-free overnight rate (first WIRON; in 2025 the
  National Working Group chose POLSTR instead); the final conversion point was moved to the end of 2027
  ([rp.pl on the KNF/NGR decision of 25 Oct 2023](https://www.rp.pl/banki/art39324121-wibor-zostanie-z-nami-na-dluzej-knf-przedluza-konwersje-na-wiron)).
  A long WIBOR series will end; plan the successor series before building a "loan rate" page on WIBOR.
- **Gold in PLN.** The NBP gold price is a calculated PLN price per gram of pure gold; its exact method (quote source
  and time) is not stated on the NBP page. Do not compare it to the LBMA USD/oz price without converting units
  (1 troy ounce = 31.1035 g) and the exchange rate of the same moment.

## 4. Standard analyses

| Analysis | Chart | KPIs needed |
|---|---|---|
| Złoty against EUR, USD, CHF, GBP | daily line of table A mid, 1-year and 5-year range; table number and date in the tooltip | `fx_rate` |
| Złoty vs CEE peers | index (start = 100) of EUR/PLN, EUR/CZK, EUR/HUF; up = weaker | `fx_rate` or Eurostat `ert_bil_eur_d` |
| Rate for a date | lookup: date → previous business day's table A rate and number (accounting use) | `fx_rate` |
| Gold in PLN vs gold in USD | two indexed lines; the gap is the złoty effect | `gold_price`, `fx_rate` (USD) |
| Policy rate and inflation | step line of the reference rate, CPI y/y, real rate shaded | reference rate (not loaded), CPI (economy) |
| Deficit and debt | bars of deficit % GDP, line of debt % GDP, 3 % and 60 % reference lines; EDP and PDP named | `gov_10dd_edpt1` (not a KPI), PDP (not loaded) |
| EU fiscal comparison | Poland vs CEE and EU27 debt and deficit, latest notification | `gov_10dd_edpt1` |
| Bond spread | PL 10Y minus DE 10Y, monthly | `irt_lt_mcby_m` (not a KPI) |
| Local-government debt | debt per inhabitant by voivodeship / gmina | local-government finance (not loaded) |

## 5. Release calendar

Check exact days against the [NBP release calendar](https://nbp.pl/en/statistic-and-financial-reporting/calendar/)
and [Eurostat's](https://ec.europa.eu/eurostat/news/release-calendar).

| Release | Publisher | When |
|---|---|---|
| Table A (and gold price) | NBP | every business day, about 11:45–12:15 Warsaw (`nbp.yaml`) |
| Table C | NBP | every business day around 08:00 Warsaw, effective that day, from the previous day's 16:00 quotes |
| Table B | NBP | Wednesdays (the previous business day if Wednesday is a holiday) |
| RPP interest-rate decisions | NBP | about 11 meetings a year; decision announced on the last day of the meeting, the NBP President's press conference the next day |
| M3 and monetary statistics | NBP | monthly, in the second half of the following month (typical; confirm in the calendar) |
| EDP deficit and debt | GUS, then Eurostat | twice a year: about 22 April and 21–22 October |
| Quarterly government debt | Eurostat | about four months after the quarter (typical; confirm) |
| State public debt | Ministry of Finance | monthly, with a lag of about one to two months (typical; confirm) |
| Long-term bond yields (`irt_lt_mcby_m`) | Eurostat | monthly, early in the following month |

Pages must say the table number or reference period and the publication date, e.g. "EUR/PLN 4.2500, table
210/A/NBP/2026 of 28 Oct 2026".

## 6. Audience and questions

- **Accountants, small firms, sole traders:** which NBP rate applies to my invoice, what was the rate on a date,
  monthly averages for reports.
- **Households and borrowers:** what is the NBP rate, will instalments fall, how does the złoty move (CHF and EUR
  loans, holidays), what happens to WIBOR.
- **Journalists:** is public debt above the limit, which limit, how does Poland compare in the EU, what did the RPP
  decide.
- **Analysts and investors:** real rates, bond spreads, złoty vs CEE peers, fiscal trajectory under the excessive
  deficit procedure.
- **Local government:** own debt and its ratios vs peers.
- **Foreign readers (EN):** EUR/PLN and PLN/USD history, EDP deficit and debt vs EU.

## 7. Open points

- `fx_rate` unit: confirm with a number check that API values for JPY, HUF, IDR and other multiplied currencies are
  per 1 unit and that published pages label them so (Quality Engineer number check against table A).
- `gold_price` is draft; its exact NBP method (quote source and time) is not documented on the NBP page. Ask NBP
  or find the resolution before writing a definition.
- Table B is in Silver but not in Gold (`poc_build_nbp` takes tables A and C only); decide whether `fx_rate` should
  include table B currencies with frequency W.
- Finance data already in Eurostat Bronze (`gov_10dd_edpt1`, `irt_lt_mcby_m`) has no KPI; the NBP policy rates,
  M3 and the MF state public debt are not loaded (proposals on Product inbox #2074).
- zohelo.com home shows an illustrative "Polish benchmarks" panel (EUR/PLN, USD/PLN, WIBOR 3M 5.85 %) labelled
  "not live"; real values are planned in zohelo#44. WIBOR is not a source we hold.
- Live finance pages on data.zohelo.com and bi.zohelo.com were not reviewed on 28 Oct: robots block agent fetches.
- Release days marked "typical" in § 5 need confirming against the NBP and MF calendars.
