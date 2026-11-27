-- gios_surface_water: all 19 layers of the five status services have rows (Dev probe 45922872272); within a layer
-- the FIDs are 0 to n-1 without a gap or a repeat, its pages are 1 to k, and every page but the last holds 1,000
-- features (a lost, stale or cut page fails here). One test per Silver model (it reads only this model).
with layers as (
    select
        water_category || '_' || assessment as layer,
        count(*) as features,
        count(distinct fid) as ids,
        min(fid) as first_fid,
        max(fid) as last_fid,
        max(page) as pages,
        count(distinct page) as distinct_pages
    from {{ ref('gios_surface_water_status') }}
    group by 1
),

pages as (
    select
        water_category || '_' || assessment as layer,
        page,
        count(*) as features
    from {{ ref('gios_surface_water_status') }}
    group by 1, 2
)

select
    'layers' as problem,
    count(*) as found
from layers
having count(*) <> 19
union all
select
    'fids or pages of ' || layer as problem,
    features as found
from layers
where ids <> features or first_fid <> 0 or last_fid <> features - 1 or distinct_pages <> pages
union all
select
    'short page ' || p.layer || ' ' || p.page as problem,
    p.features as found
from pages as p
inner join layers as l on p.layer = l.layer
where p.page < l.pages and p.features <> 1000
