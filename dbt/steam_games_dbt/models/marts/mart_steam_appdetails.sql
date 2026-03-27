with source as (
    select * from {{ref('stg_steam_appdetails')}}
),

base as (
    select
        appid,
        name,
        is_free,
        is_coming_soon,
        SAFE.PARSE_DATE('%b %e, %Y', release_date) as cleaned_release_date,
        developers,
        publishers,
        exists(select 1 from unnest(categories) where category_name = 'Multi-player') as is_multiplayer,
        exists(select 1 from unnest(categories) where category_name = 'Co-op') as is_coop,
        exists(select 1 from unnest(genres) where genre_name = 'Indie') as is_indie,
        array_to_string(developers, ', ') = array_to_string(publishers, ', ') as is_self_published
    from source
)

select 
    *,
    case when is_indie and is_self_published then true else false end as is_very_indie
from base
where cleaned_release_date is not null
    and cleaned_release_date <= current_date()