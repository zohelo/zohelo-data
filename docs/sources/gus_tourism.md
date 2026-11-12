# GUS tourism tables (`gus_tourism`)

**Built (12 Nov 2026, register PLS-035, #2893).** "Turystyka w <year> r." is published yearly with a ZIP of five
chapter workbooks. Same route, version and restart unit as `gus_culture` (`ingestion/common/gus_publications.py`,
editions from 2019, entity `turystyka_<year>`). Silver `silver.gus_tourism_tables`: tourist accommodation
establishments and bed places (31 July, TABL. I/1, I/2), tourists accommodated and overnight stays (TABL. II/17,
II/19), Poland and voivodeships. Gold block `accommodation`, registered in `warehouse/macros/sources/gus_tourism.sql`;
KPIs `tourists_accommodated`, `tourist_overnight_stays`, `tourist_bed_places` (draft). Monthly check. Licence: GUS terms.
