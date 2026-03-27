with source as (
    select
        ad.appid,
        g.genre_name,
        ad.is_free,
        ad.is_coming_soon,
        SAFE.PARSE_DATE('%b %e, %Y', ad.release_date) as cleaned_release_date,
        ad.original_price,
        ad.discount_price
    from {{ref('stg_steam_appdetails')}} ad, unnest(genres) as g
)

select
    *
from source
where cleaned_release_date is not null
    and cleaned_release_date <= current_date()