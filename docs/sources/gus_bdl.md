# GUS Local Data Bank (BDL) (`gus_bdl`)

**Extraction (owner decision 13 Oct 2026).** Entity = subgroup (P####, an API `subject`
with variables); version = its `lastUpdate` (`/subjects/{id}`, Warsaw local time). The
initial load is the website's subgroup export ("CSV – tablica relacyjna" ZIP; large
subgroups in parts), one browser and one route, no IP rotation. Changed subgroups are
refreshed whole through the API with the registered key, so every Landing version is a
complete subgroup and the newest version wins, whichever channel produced it. Website
CSV columns = `Kod;Nazwa;<subgroup dimensions>;Rok;Wartosc;Jednostka miary;Atrybut`; the
dimension values equal the API variables' name components n1…n5, which is how Silver
maps website rows to variable ids.

The reviewed [BDL v1 OpenAPI](https://bdl.stat.gov.pl/api/v1/swagger/doc/swagger.json)
has no bulk export and no changed-variable feed. Complete collection must enumerate
the API. The public API exposes the same statistical scope as the BDL application,
including annual and short-period statistics from 1995 onward.

```text
BASE=https://bdl.stat.gov.pl/api/v1

# Variables: zero-based pages, stable source-ID ordering
$BASE/variables?lang=pl&page={PAGE}&page-size={PAGE_SIZE}&sort=id
$BASE/variables/{VARIABLE_ID}?lang=en

# All observations for a variable: omit year and exhaust pages
$BASE/data/by-variable/{VARIABLE_ID}?lang=en&page={PAGE}&page-size={PAGE_SIZE}

# Metadata catalogues and details, in pl and en where labels are translated
$BASE/subjects?lang={LANG}&page={PAGE}&page-size={PAGE_SIZE}&sort=id
$BASE/subjects/{SUBJECT_ID}?lang={LANG}
$BASE/units?lang={LANG}&page={PAGE}&page-size={PAGE_SIZE}&sort=id
$BASE/units/{UNIT_ID}?lang={LANG}
$BASE/units/localities?parent-id={12_CHARACTER_MUNICIPALITY_ID}&lang={LANG}&page={PAGE}&page-size={PAGE_SIZE}
$BASE/units/localities/{LOCALITY_ID}?lang={LANG}
$BASE/attributes?lang={LANG}
$BASE/aggregates?lang={LANG}
$BASE/levels?lang={LANG}
$BASE/measures?lang={LANG}
$BASE/years?lang={LANG}
```

The live catalogue reported **172,573 variables** and **4,694 ordinary units** on
8 September 2026. Localities have no valid root listing: enumerate ordinary units,
then call `/units/localities` for every level-6 municipality using its 12-character
BDL ID. Preserve leading zeroes in unit IDs. Fetch Polish and English metadata, and
retain `lastUpdate`, descriptions, dimensions, units, aggregates, attributes,
precision and formatted values.

The official anonymous limits are 5 requests/second, 100/15 minutes, 1,000/12 hours,
and 10,000/seven days; with a registered key (`X-ClientId`, secret `SOURCE_GUS_BDL_API_KEY`)
10/s, 500/15 min, 5,000/12 h and 50,000/7 days. Response headers `X-Rate-Limit-Limit`,
`X-Rate-Limit-Remaining`, and `X-Rate-Limit-Reset` may require a slower rate. A
complete run is necessarily multi-cycle: the known minimum already exceeds 534,000
requests before continuation pages. Checkpoint each validated page. Conditional
requests and resource `lastUpdate` help, but neither proves global completeness or
identifies every changed variable.

BDL data are published under CC BY 4.0. Attribute GUS/Statistics Poland and identify
BDL as the source. The official documentation states no geography restriction.

Primary evidence:

- [Official English BDL API guide](https://api.stat.gov.pl/Home/BdlApi?lang=en)
- [BDL v1 OpenAPI](https://bdl.stat.gov.pl/api/v1/swagger/doc/swagger.json)
