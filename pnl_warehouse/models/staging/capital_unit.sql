with capunit
as (
	select desk
		,capitalunit
		,valuedate
	from {{ source('pnl', 'transaction_value') }}
	)
	,distinct_capunit
as (
	select desk
		,capitalunit
		,MIN(valuedate) as startdate
	from capunit
	group by desk
		,capitalunit
	)
select {{ dbt_utils.generate_surrogate_key([
            'desk', 
        'capitalunit', 
        'startdate']) }} as capunit_key
	,desk
	,capitalunit
	,startdate
	,cast(lag(startdate, 1, '2050-01-01') over (
			partition by desk
			,capitalunit order by startdate desc
			) as datetime) endDate
from distinct_capunit
