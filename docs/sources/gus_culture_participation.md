# GUS participation in culture survey (`gus_culture_participation`)

**Built (12 Nov 2026, register PLS-028, #2893).** "Uczestnictwo ludności w kulturze w <year> r." is a survey
published every five years (2019, 2024 in range) with a tables file (one workbook in 2024, a ZIP of chapter
workbooks in 2019; the precision workbook is not landed). Same route, entity (`uczestnictwo_<year>`), version and
restart unit as `gus_culture` (`ingestion/common/gus_publications.py`). Silver
`silver.gus_culture_participation_tables`: visiting museums in Poland, using a library, attending theatre, in % of
persons 15+, Poland, total and by sex (rows OGÓŁEM, Mężczyźni, Kobiety); estimates marked 'v' are
`low_reliability`. Gold block `participation`, registered in `warehouse/macros/sources/gus_culture_participation.sql`;
KPIs `museum_visiting_share`, `theatre_attendance_share` (draft). Monthly check. Licence: GUS terms.
