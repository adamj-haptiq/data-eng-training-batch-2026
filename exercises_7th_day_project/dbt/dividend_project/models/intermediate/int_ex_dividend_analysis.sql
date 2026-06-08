select
    ticker,
    ex_dividend_date,
    dividend_amount,
    close_day_before,
    close_on_ex_div,
    price_drop,
    price_drop < dividend_amount as is_favorable,
    loaded_at
from {{ ref('stg_dividend_events') }}
