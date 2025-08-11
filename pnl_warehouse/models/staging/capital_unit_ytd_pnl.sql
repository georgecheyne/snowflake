{{ config(
   materialized="dynamic_table",
   on_configuration_change="apply",
   target_lag="1 hour",
   snowflake_warehouse="PNL_WAREHOUSE_DBT_WH",
   refresh_mode="INCREMENTAL" ,
   initialize="ON_CREATE" ,
) }}

with pnl as (
select
    pnl.capunit_key,
    pnl.value_date,
    pnl.value
from 
    {{ ref('profit_and_loss') }} pnl 
inner join {{ ref('pnl_period') }} p on p.period_key = pnl.period_key
inner join {{ ref('pnl_type') }} ty on ty.PNL_TYPE_KEY = pnl.PNL_TYPE_KEY
inner join {{ ref('source') }} s on s.source_key = pnl.source_key
WHERE 1 = 1
and p.period = 'YTD'
and ty.pnl_type = 'PnL'
)

select capunit_key,
    value_date,
    sum(value) as YtdPnL
from pnl
group by capunit_key, value_date