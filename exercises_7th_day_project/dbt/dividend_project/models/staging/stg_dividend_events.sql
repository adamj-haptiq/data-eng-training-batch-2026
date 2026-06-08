with source as (
    select * from {{ source('raw', 'raw_dividend_events') }}
),

filtered as (
    select
        ticker,
        cast(ex_dividend_date as date) as ex_dividend_date,
        dividend_amount,
        close_day_before,
        close_on_ex_div,
        price_drop,
        loaded_at
    from source
    where ticker is not null
      and ex_dividend_date is not null
      and dividend_amount is not null
      and ex_dividend_date >= current_date - interval '5 years'
)

select * from filtered
