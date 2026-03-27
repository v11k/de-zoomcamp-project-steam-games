with source as (
    select * from {{ref('stg_steam_reviews')}}
)

select
    *,
    good_reviews / nullif(total_reviews, 0) as good_review_ratio
from source