select
  symbol,
  sum(last_sale_size * last_sale_price) as notional,
  count(*) as trade_count
from {{ ref('stg_trades') }}
group by symbol
