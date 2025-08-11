{{ config(
   materialized="dynamic_table",
   on_configuration_change="apply",
   target_lag="1 hour",
   snowflake_warehouse="PNL_WAREHOUSE_DBT_WH",
   refresh_mode="INCREMENTAL" ,
   initialize="ON_CREATE" ,
) }}

with perm_adj as (
select 
    perm.capunit_key,
    perm.value_date,
    perm.value
from {{ref('permanent_adjustment')}} perm
inner join {{ ref('pnl_period') }} p on p.period_key = perm.period_key
inner join {{ ref('pnl_type') }} ty on ty.PNL_TYPE_KEY = perm.PNL_TYPE_KEY
where p.period = 'YTD' 
and ty.pnl_type = 'PnL'

)

select capunit_key,
    value_date,
    sum(value) as YtdPnl
from perm_adj
group by capunit_key, value_date