-- Silver: GUS DBW observations of one bulk file (indicator × cross-section × presentation ×
-- periodicity [× year]), typed. Applied by the loader to ONE Bronze file; `bronze` is that file.
-- The result replaces the file's partition in silver.dbw_observations (design #875). Labels of
-- the ids (dimensions, positions, periods, presentations, flags) come from the DBW dictionaries
-- in Gold. Value: decimal comma → double; empty → null (id_brak_wartosci says why).
select
    entity_id,
    try_cast(id_zmienna as integer) as indicator_id,
    try_cast(id_przekroj as integer) as section_id,
    try_cast(id_wymiar_1 as integer) as dimension_1_id,
    try_cast(id_pozycja_1 as bigint) as position_1_id,
    try_cast(id_wymiar_2 as integer) as dimension_2_id,
    try_cast(id_pozycja_2 as bigint) as position_2_id,
    try_cast(id_wymiar_3 as integer) as dimension_3_id,
    try_cast(id_pozycja_3 as bigint) as position_3_id,
    try_cast(id_wymiar_4 as integer) as dimension_4_id,
    try_cast(id_pozycja_4 as bigint) as position_4_id,
    try_cast(id_wymiar_5 as integer) as dimension_5_id,
    try_cast(id_pozycja_5 as bigint) as position_5_id,
    try_cast(id_wymiar_6 as integer) as dimension_6_id,
    try_cast(id_pozycja_6 as bigint) as position_6_id,
    try_cast(id_wymiar_7 as integer) as dimension_7_id,
    try_cast(id_pozycja_7 as bigint) as position_7_id,
    try_cast(id_wymiar_8 as integer) as dimension_8_id,
    try_cast(id_pozycja_8 as bigint) as position_8_id,
    try_cast(id_wymiar_9 as integer) as dimension_9_id,
    try_cast(id_pozycja_9 as bigint) as position_9_id,
    try_cast(id_okres as integer) as period_id,
    try_cast(id_sposob_prezentacji_miara as integer) as presentation_id,
    try_cast(id_daty as integer) as year,
    try_cast(id_brak_wartosci as integer) as missing_reason_id,
    try_cast(id_tajnosci as integer) as confidentiality_id,
    try_cast(id_flaga as integer) as flag_id,
    try_cast(replace(nullif(trim(wartosc), ''), ',', '.') as double) as value,
    try_cast(precyzja as integer) as value_precision,
    raw_sha256,
    source_version
from bronze
