with tops as (
  select * from {{ ref('stg_tops') }}
)
select
  trade_date,
  symbol,
  avg(last_sale_price) as avg_price,
  avg(ask_price - bid_price) as avg_spread,
  sum(last_sale_size) as total_volume
from tops
group by 1,2
