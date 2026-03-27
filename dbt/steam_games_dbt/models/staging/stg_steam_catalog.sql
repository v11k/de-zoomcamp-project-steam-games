with source as (
    select * from {{source('raw_steam', 'steam_catalog')}}
),
latest_ingestion as (
    select
        appid,
        max(partition_date) as latest_ingestion_date
    from source
    group by appid
)

select distinct
    s.appid,
    s.name
from source s
left join latest_ingestion li
    on s.appid = li.appid
    and s.partition_date = li.latest_ingestion_date
where li.appid is not null