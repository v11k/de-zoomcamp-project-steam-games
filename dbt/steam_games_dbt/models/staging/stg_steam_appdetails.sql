with source as (
    select * from {{source('raw_steam', 'steam_appdetails')}}
),
latest_ingestion as (
    select
        appid,
        max(ingestion_date) as latest_ingestion_date
    from source
    where success is true
    group by appid
)

select distinct
    s.appid,
    s.name,
    cast(json_value(s.raw, '$.is_free') as bool) as is_free,
    cast(json_value(s.raw, '$.release_date.coming_soon') as bool) as is_coming_soon,
    json_value(s.raw, '$.release_date.date') as release_date,
    json_extract_string_array(s.raw, '$.developers') as developers,
    json_extract_string_array(s.raw, '$.publishers') as publishers,
    json_value(s.raw, '$.price_overview.currency') as currency,
    cast(json_value(s.raw, '$.price_overview.initial') as float64) / 100 as original_price,
    cast(json_value(s.raw, '$.price_overview.final') as float64) /100 as discount_price,
    array(
      select as struct
        cast(json_value(item, '$.id') as int64) as id,
        json_value(item, '$.description') as category_name,
      from unnest(json_query_array(s.raw, '$.categories')) as item
    ) as categories,
    array(
      select as struct
        cast(json_value(item, '$.id') as int64) as id,
        json_value(item, '$.description') as genre_name,
      from unnest(json_query_array(s.raw, '$.genres')) as item
    ) as genres
from source s
left join latest_ingestion li
    on s.appid = li.appid
    and s.ingestion_date = li.latest_ingestion_date
where li.appid is not null
    and s.success is true