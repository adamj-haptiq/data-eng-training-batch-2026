with source as (
    select * from {{ source('raw', 'raw_ticker_scores') }}
)

select
    ticker,
    total_events,
    favorable_events,
    consistency_ratio,
    lookback_years,
    loaded_at
from source
where total_events >= 3 -- With 1–2 events, consistency_ratio isn’t very meaningful
