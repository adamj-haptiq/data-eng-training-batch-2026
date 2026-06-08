with ranked as (
    select
        *,
        row_number() over (
            order by consistency_ratio desc, total_events desc
        ) as rank
    from {{ ref('stg_ticker_scores') }}
),

best_two as (
    select ticker
    from ranked
    where rank <= 2
)

select
    e.ticker,
    e.ex_dividend_date,
    e.dividend_amount,
    e.close_day_before,
    e.close_on_ex_div,
    e.price_drop,
    e.is_favorable,
    s.consistency_ratio,
    s.total_events,
    s.favorable_events,
    e.loaded_at
from {{ ref('int_ex_dividend_analysis') }} e
inner join best_two b on e.ticker = b.ticker
inner join {{ ref('stg_ticker_scores') }} s on e.ticker = s.ticker
