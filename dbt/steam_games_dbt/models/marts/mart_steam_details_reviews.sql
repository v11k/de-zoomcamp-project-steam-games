with appdetails as (
    select * from {{ref('mart_steam_appdetails')}}
),

reviews as (
    select * from {{ref('mart_steam_reviews')}}
)

select
    ad.appid,
    ad.name,
    ad.is_free,
    ad.is_coming_soon,
    ad.is_coop,
    ad.is_indie,
    ad.is_self_published,
    ad.is_multiplayer,
    ad.cleaned_release_date,
    ad.developers,
    ad.publishers,
    r.total_reviews,
    r.good_reviews,
    r.good_review_ratio

from appdetails ad
left join reviews r
    on ad.appid = r.appid
