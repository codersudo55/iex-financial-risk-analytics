with raw as (
  select * from {{ source('raw','iex_tops') }}
)
select
  symbol,
  cast(ts as timestamp) as ts,
  cast(date as date) as trade_date,
  cast(bid_price as float) as bid_price,
  cast(ask_price as float) as ask_price,
  cast(last_sale_price as float) as last_sale_price,
  cast(bid_size as bigint) as bid_size,
  cast(ask_size as bigint) as ask_size,
  cast(last_sale_size as bigint) as last_sale_size,
  case when last_sale_size>0 then 1 else 0 end as is_trade
from raw
