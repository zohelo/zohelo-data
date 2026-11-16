# NFZ contracts API (`nfz_contracts`)

**Built (12 Nov 2026, register PLS-014, #2955).** NFZ publishes its register of agreements with healthcare providers
only through the API `https://api.nfz.gov.pl/app-umw-api/` (version 1.2; no bulk file). `/agreements` lists one
contract year and one regional branch at a time (both required), at most 25 a page; `/available-years` serves
2008–2026. Route: `ingestion/to_landing/nfz_contracts/`, one request at a time, 0.3 s apart (the terms allow 10 a
second per IP), bounded retries. Entity = one contract year and branch (`agreements_<year>_<branch>`), every page of
its list landed as one ZIP of the exact answers; version = download time, written only when the pages' `data`
differ from the newest copy (the API has no update date: `meta.date-modified` is the request time). Restart unit: one
year and branch. Each weekly run reads the current and the previous contract year again (NFZ amends agreements
during and after their year), any later year the API already serves, and any older year once a year. Dev trial
43814206385 (16 Nov 2026): the first run landed 144 lists (13,163 pages, 49.8 MB) in 108.6 minutes; the weekly
re-read (48 lists, 2025–2027) took 29.0 minutes and wrote nothing (data unchanged). An empty list (a year or branch
without agreements yet) comes as `"data": null` with `meta.count` 0 and is landed as an empty copy.

**Scope.** Contract years from 2019 to the newest year the API serves (16 Nov 2026: 2019–2027, 327,206
agreements; 2027 had just opened with 2 agreements in 2 branches). Left out: contract
years 2008–2018 (528,886 more agreements: the agreements table would pass the 1 M full-rebuild limit, #822; they can
be added once Silver is incremental); each agreement's plans by contract product, their months and packages
(settlement-unit quantities and prices: one request per agreement or plan, about 327,000 requests); the provider
dictionary with addresses and TERYT commune; the contract-product, service-type and orthopedic-supply dictionaries.

**Layers.** Bronze `bronze.nfz_contracts_agreements`: one row per agreement record, every attribute as the exact
JSON text, except the provider's identity (below). Silver `silver.nfz_contracts_agreements` (grain: agreement id,
one per agreement and contract year; amount in PLN as published: null where NFZ gives none, 0.00 where it publishes
0.00) and `silver.nfz_contracts_providers` (grain branch × NFZ provider code: first and last contract year, contract
years, agreements). Gold block `agreements` (`gold.build_nfz_contracts`, registered in
`warehouse/macros/sources/nfz_contracts.sql`): annual counts of agreements and of provider codes and the sum of the
published amounts, by NFZ branch (= voivodeship, branch codes 01–16 in the voivodeships' alphabetical order) and
Poland, totals over the NFZ service types only (every row of a block enters `poc_fact_annual`, which is above its
warning ceiling, #2294; the breakdown by service type waits for the per-block Gold load, D35). **These counts and sums are zohelo calculations**, not values NFZ
publishes; the amounts are contract values for the year (ceilings), not expenditure, and agreements without an
amount (e.g. medical devices, service type 12) are not in the sum. Draft KPIs `nfz_agreements`,
`nfz_contracted_providers`, `nfz_contracted_amount`; no `kpi_v2_map` row, so nothing is published until the reading
of the no-modification clause for derived figures is decided (register research blocker).

**Licence.** Terms of "API Umowy NFZ" v1.2 (https://api.nfz.gov.pl/app-umw-api/terms): the source must be given with
the address https://api.nfz.gov.pl/; the user may not modify the data; no overloading (10 requests per second per IP);
no licence name and no clause on commercial reuse. Licence row `nfz_api_terms_umw`; Silver keeps the published
values unchanged.

**Provider identity is not loaded.** The API repeats each provider's name, NIP, REGON, registry number and place on
every agreement. Providers include natural persons (individual and group practices of doctors, nurses and midwives),
whose name and NIP/REGON are personal data, and the register entry gives no basis for storing them. Bronze checks
that these attributes are present (an unknown layout still rejects the file) but stores none of them; Silver and Gold
know a provider only by its NFZ provider code and branch. The exact Landing bytes keep them, as every Landing file
keeps the source's bytes. Loading any of them needs an owner GDPR review first (as for REGON in `register.yaml` and
`warehouse/dimensions/person.yaml`).

**Amounts of 0.00.** NFZ publishes `amount` 0.0 on 54,239 of the 327,206 agreements (Dev trial 43814206385). They
are numbers, not a marker: the API's only "no amount" is null (36,459 agreements, all of service type 12), and the same
service types carry positive amounts down to a few grosze. They are concentrated in a few service types (code:
zeros of agreements with an amount): 01 43,948 of 76,723, 21 7,718 of 9,038, 18 1,472 of 15,750, every other type
under 3%; 2019–2021 about 9,000 a year, 2022–2026 3,500–6,500; no agreement code has 0 in one year and a positive
amount in another. Silver keeps them as published (the terms forbid modifying data); in Gold they add nothing to a sum,
and no Gold value is 0 (0 rows). Whether 0.0 means "no fixed amount in the plan" (e.g. capitation) is not stated by
NFZ; a measure that counts or averages agreements with an amount must decide it first.

**Completeness.** `tests/nfz_contracts_agreements_complete.sql` fails when a year and branch before the newest landed
year has no agreement; `..._complete_newest.sql` only warns for the newest year (a year NFZ has just opened can have
branches without an agreement, and a run can stop on its time budget). Landing reads missing lists newest year
first, so a first production run stopped by its time budget would leave the oldest years missing and fail the error
test until the next run lands them (low risk: the whole scope took 108.6 minutes of the 330-minute budget).
