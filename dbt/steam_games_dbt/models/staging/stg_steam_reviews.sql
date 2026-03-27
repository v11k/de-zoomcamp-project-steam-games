with source as (
    select * from {{source('raw_steam', 'steam_reviews')}}   
),

last_ingestion as (
    select
        appid,
        max(ingestion_date) as last_ingestion_date
    from source
    group by appid
)

select distinct
    s.appid,
    s.total_reviews,
    s.good_reviews,
    s.bad_reviews,
    s.review_score,
    s.review_score_desc
from source s
left join last_ingestion li
    on s.appid = li.appid
    and s.ingestion_date = li.last_ingestion_date
where li.appid is not null