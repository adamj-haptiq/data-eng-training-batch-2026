-- Fails when the mart does not contain exactly 2 distinct best tickers
select count(distinct ticker) as ticker_count
from {{ ref('mart_best_dividend_tickers') }}
having count(distinct ticker) != 2
