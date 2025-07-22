with perm_adj as(

select 
    to_date(VALUEDATE, 'DD/MM/YYYY HH24:MI:SS') as value_date
    ,CAPITALUNIT
    ,DESK
    ,STRATEGY
    ,FUND
    ,'DTD' AS period
    ,'Permanent Adjustment' as source
    ,'Pnl' as type
    ,DAILYADJUSTMENT AS VALUE 
from {{ source('pnl', 'permanent_adjustment') }}
union
select 
    to_date(VALUEDATE, 'DD/MM/YYYY HH24:MI:SS') as value_date
    ,CAPITALUNIT
    ,DESK
    ,STRATEGY
    ,FUND
    ,'MTD' AS period
    ,'Permanent Adjustment' as source
    ,'Pnl' as type
    ,MTDADJUSTMENT AS VALUE 
from {{ source('pnl', 'permanent_adjustment') }}
union
select 
    to_date(VALUEDATE, 'DD/MM/YYYY HH24:MI:SS') as value_date
    ,CAPITALUNIT
    ,DESK
    ,STRATEGY
    ,FUND
    ,'YTD' AS period
    ,'Permanent Adjustment' as source
    ,'Pnl' as type
    ,YTDADJUSTMENT AS VALUE 
from {{ source('pnl', 'permanent_adjustment') }}
union
select 
    to_date(VALUEDATE, 'DD/MM/YYYY HH24:MI:SS') as value_date
    ,CAPITALUNIT
    ,DESK
    ,STRATEGY
    ,FUND
    ,'DTD' AS period
    ,'Permanent Adjustment' as source
    ,'NAV' as type
    ,NAVADJUSTMENT AS VALUE 
from {{ source('pnl', 'permanent_adjustment') }}
)

select value_date,
        {{ dbt_utils.generate_surrogate_key(['FUND']) }} as fund_key,
        {{ dbt_utils.generate_surrogate_key(['STRATEGY']) }} as strategy_key,
        {{ dbt_utils.generate_surrogate_key(['DESK', 'CAPITALUNIT']) }} as capunit_key,
        {{ dbt_utils.generate_surrogate_key(['period']) }} as period_key,
        {{ dbt_utils.generate_surrogate_key(['type']) }} as pnl_type_key,
        {{ dbt_utils.generate_surrogate_key(['source']) }} as source_key,
        VALUE as value   
from perm_adj