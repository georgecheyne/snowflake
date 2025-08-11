with cap_alloc as (
    select * from 
    {{ source('capital_allocation', 'capital_allocation')}}
)

select
    {{ dbt_utils.generate_surrogate_key(['DESK', 'CAPITALUNIT']) }} as capunit_key,
    {{ dbt_utils.generate_surrogate_key(['FUND']) }} as fund_key,
    CAPITALALLOCATION as value,
    STARTDATE as start_date,
    ENDDATE as end_date
from cap_alloc