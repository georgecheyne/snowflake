with raw_values as (
select distinct fund 
from {{ source('pnl', 'transaction_value') }}
union
select distinct fund 
from {{ source('pnl', 'daily_adjustment') }}
union
select distinct fund 
from {{ source('pnl', 'permanent_adjustment') }}
)

select {{ dbt_utils.generate_surrogate_key([
        'fund']) }} as fund_key,
        fund 
from raw_values