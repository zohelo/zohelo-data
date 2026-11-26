# GUGiK PRG (national register of boundaries) (`gugik_prg`)

**Route (implemented):** the national package
`https://opendata.geoportal.gov.pl/prg/granice/00_jednostki_administracyjne.zip` (~377 MB,
shapefiles, geographic ETRS89 coordinates per the `.prj`): A00 country (1), A01 voivodeships (16), A02 powiats (380), A03
gminas (2,479), A05 cadastral units (3,227), A06 cadastral precincts (54,264), counts of
October 2026. Every layer carries the JPT attribute set (`JPT_KOD_JE` = TERYT code,
`JPT_NAZWA_`, `JPT_POWIER` area, validity `WAZNY_OD/DO`, version `WERSJA_OD/DO`, `REGON` for
A01–A03). The server sends no Last-Modified or ETag: every run downloads (~7 minutes from
GitHub Actions) and writes Landing only when the SHA-256 changed. GUGiK states the data is
free for any purpose (Regulation of the Council of Ministers of 16 July 2021, Dz.U. 2021
poz. 1373); updated yearly (state 1 January) with corrections. WFS
(`https://mapy.geoportal.gov.pl/wss/service/PZGIK/PRG/WFS/AdministrativeBoundaries`, GML,
1,000 features per request) is the alternative, not used.

Bronze `bronze.prg_boundaries`: one row per feature of every layer, attributes as text,
geometry as WKB in the source CRS with the `.prj` text (60,367 features, ~434 MB Parquet); any reprojection is a Silver rule.
