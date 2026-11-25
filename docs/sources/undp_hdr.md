# UNDP Human Development Data Center (HDI) (`undp_hdr`)

**Built (11 Nov 2026, register GL-028).** The downloads page
(`https://hdr.undp.org/data-center/documentation-and-downloads`) links one CSV with every composite index
and component for every country and aggregate (HDR25: `…/2025_HDR/HDR25_Composite_indices_complete_time_series.csv`,
2.0 MB). Its URL changes with each report, so `undp_hdr.yaml` finds the link on the page (`url_from`);
a copy is written only when the SHA-256 changed. Bronze: `bronze.undp_hdr_values`, one row per non-empty
cell (id columns, source column name, value as text). Silver: `silver.undp_hdr_indices` (iso3 × indicator ×
year; indicators hdi, le, eys, mys, gnipc, gdi, gii, ihdi, phdi, components and sex variants; aggregates
`ZZ…`): 200,004 values, 206 areas, 1990–2023 (Dev trial 43093051337; Poland HDI 0.906, rank 35 in 2023).
Each report recalculates the whole series, so values compare within one release; the Archive keeps the
previous report 400 days. Terms: CC BY 3.0 IGO, licence row `cc_by_3_0_igo_undp_hdr`. Monthly check.
Restart unit: the file.
Gold (#2861): `gold.build_undp_hdr` — HDI, life expectancy at birth, expected and mean years of schooling and GNI
per capita (total, female, male), IHDI and GII for the EU27 members and World; additional source of KPI
`life_expectancy` (next to Eurostat, GUS BDL, UN WPP, WHO and the World Bank), draft KPI `hdi`.
