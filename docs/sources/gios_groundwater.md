# GIOŚ groundwater quality monitoring (`gios_groundwater`)

**Built (Nov 2026, register PL-ENV-012, #2870).** Route: GIOŚ's own result files of the groundwater quality
monitoring of the State Environmental Monitoring (PMŚ) on dane.gov.pl, dataset 1405 "Państwowy Monitoring Środowiska -
monitoring jakości wód podziemnych" (https://dane.gov.pl/pl/dataset/1405, CC BY 4.0, update frequency "Rocznie"): one
file per year x indicator group (inorganic, organic) x monitoring kind, titled "Wyniki badań w <year>r. wskaźników
fizykochemicznych NIEORGANICZNYCH|ORGANICZNYCH - monitoring jakości wód podziemnych - monitoring
diagnostyczny|operacyjny". On 26 Nov 2026: 12 files, 2019-2024 (diagnostic network 2019 and 2022, operational network
2020, 2021, 2023, 2024), CSV for 2019-2020 and XLSX from 2021, 95-853 kB each (Dev probes 45810255139, 45810873298).

The producer's portal https://mjwp.gios.gov.pl/ (yearly result pages 1991-2025, XLSX workbooks with hash names, the
2025 workbooks and the quality-class workbooks) answered every request on 26 Nov 2026, robots.txt and the 2025 file
URLs included, with HTTP 403 and a maintenance page ("trwają prace serwisowe … dostęp do jego zasobów może być czasowo
ograniczony lub niemożliwy"; Dev probes 45809388367, 45809752001). It is not used and not worked around. Left out
until it answers again: 2025, 2008-2018 (only on mjwp) and the quality classes I-V per point (only on mjwp), so the
Gold block counts nitrate exceedances, not quality classes.

`ingestion/to_landing/gios_groundwater/` reads the dataset's resource list once per run from the portal's public API
(`/1.4/datasets/1405/resources`) and lands every result file whose file URL is not landed yet through the shared
bulk_file pattern, 3 s apart (a re-issued file gets a new dated URL and replaces its entity). Entity =
`<year>_<inorganic|organic>_<diagnostic|operational>`; version = Last-Modified; restart unit: one file. The dataset's
WMS and ATOM resources are listed, not landed; two resources for one entity, or a result file that is neither XLSX nor
CSV, fail the run.

Bronze `bronze.gios_groundwater_records` (`ingestion/to_bronze/gios_groundwater/load.py`): row 1 of a file is the
column names, then one row per water sample. Rows `sample` (one per sample: point numbers by MONBADA, ID Monitoring
(2022 on), SOH/SOBWP and CBDH, EU point identifiers and groundwater bodies of the 161, 172 and 174 divisions, PUWG 1992
X and Y, voivodeship, powiat, gmina, locality, river basin, RZGW, stratigraphy, depths, water table, aquifer medium,
point kind, land use, year, sampling date) and `value` (one per non-empty indicator cell: the column name as published,
e.g. `Azotany [mgNO3/l]`, and the cell text). Columns are matched by name; an unknown column before the sampling date,
a missing required column, a repeated indicator name, a year other than the entity's or a sample without a sampling
date or point number rejects the file (`bronze.gios_groundwater_rejected_files`).

Silver:

- `silver.gios_groundwater_results`: point x indicator x sampling date. `point_key` = the MONBADA number, else
  `IDM:<ID Monitoring>` (from 2022 about 2 % of the samples have no MONBADA number). Markers: a number → `value`;
  `<0,002` (below the limit of quantification) → `value` NULL, `below_limit` true, `limit_value` 0.002 (never 0);
  `n.o.` (not determined) is not loaded. `indicator_key` folds case and white space of the column name (organic names
  are spelt differently between years).
- `silver.gios_groundwater_points`: point x year, the attributes as published (no TERYT codes are published; the
  coordinates are PUWG 1992 / EPSG:2180, X being the easting in these files); an attribute with several values in one
  year is NULL and named in `conflicting_attributes`.

Gold: block `nitrate` of `poc_build_gios_groundwater`, annual, Poland and voivodeships (by the voivodeship name the
file gives for the point, mapped to TERC through `dim_geography`): monitoring points with nitrate results, points with
at least one sample above 50 mg/l NO3 in the year, and their share (%), one measure per monitoring kind (the
diagnostic and operational networks are never mixed in one series). A result below the limit of quantification is
below 50 mg/l (a Silver test fails on a nitrate limit above 50). KPI `groundwater_nitrate_above_50_share` (draft,
environment.condition, v2 map ready).

Dev trial 45816380441 (26 Nov 2026): 12 files, 4,292,064 bytes; 438,268 Bronze rows (4,504 samples, 433,764 indicator
cells), 0 rejected; second Landing and Bronze runs wrote nothing. Markers: 269,702 below the limit (`<`), 19,686 `n.o.`,
142,376 numbers (31 of them published as 0). Silver 412,078 results (0 duplicate keys) and 4,187 point-years (0
conflicts; 81 samples keyed by ID Monitoring); Gold 285 rows, 0 duplicates by series_key x date_key, every voivodeship
name matched. Poland: 61 of 1,289 points above 50 mg/l in 2019 (4.73 %), 67 of 1,404 in 2022 (4.77 %, diagnostic);
20 of 362 in 2024 (5.52 %, operational).

Schedule: weekly, Friday 06:20 UTC (O46; one API request and no download when nothing is new; the yearly files were
uploaded in December or January). Refresh full (D32). Terms: dane.gov.pl dataset 1405 states CC BY 4.0
(`license_name`); attribution as the producer asks on mjwp: "opracowano na podstawie danych Inspekcji Ochrony
Środowiska uzyskanych w ramach Państwowego Monitoringu Środowiska"; licence key `cc_by_4_0_gios_groundwater`, status
open-conditional.
