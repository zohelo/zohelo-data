-- GUGiK PRG address points (silver.prg_address_points, written per package by the loader, #3106): the table holds one
-- package, between 7 and 11 million points (8,647,096 in the package of 26 Nov 2026, Dev probe 45811745316), with
-- points in all 16 voivodeships and in at least 2,400 gmina codes (about 2,500 published). A package cut short or a
-- layer of another region would thin it silently otherwise. Reads only the source's own table. Rows returned = what
-- breaks it.
with held as (
    select
        count(*) as points,
        count(distinct raw_sha256) as packages,
        count(distinct left(gmina_code, 2)) as voivodeships,
        count(distinct gmina_code) as gminas
    from {{ source('gugik_prg_silver', 'prg_address_points') }}
)

select *
from held
where points not between 7000000 and 11000000 or packages <> 1 or voivodeships <> 16 or gminas < 2400
