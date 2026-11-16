-- Step of the GUS DBW Gold build (docs/gold-coverage.md): the labels of the ids one block uses, from the
-- dictionary files of its Silver files (`dictionary`: the block's files of bronze.dbw_dictionary_items;
-- every DBW bulk file carries the labels of its own ids). One row per dictionary column and id. Where the
-- block's files word a label differently, the wording of the newest file is taken (file ids sort by
-- year) and `wordings` counts them. Only this step reads the dictionary files.
select
    lower(nazwa_kolumny) as column_name,
    id_elementu as element_id,
    arg_max(opis, (entity_id, opis)) as label,
    count(distinct opis) as wordings
from dictionary
where nazwa_kolumny is not null and id_elementu is not null
group by lower(nazwa_kolumny), id_elementu
