with capunit
as (
	select desk
		,capitalunit
		,to_date(valuedate, 'DD/MM/YYYY HH24:MI:SS') as valuedate_parsed
	from {{ source('pnl', 'transaction_value') }}
	)
	,distinct_capunit
as (
	select desk
		,capitalunit
		,MIN(valuedate_parsed) as startdate
	from capunit
	group by desk
		,capitalunit
	)
select {{ dbt_utils.generate_surrogate_key([
            'desk', 
        'capitalunit']) }} as capunit_key
	,desk
	,capitalunit
	,startdate
	,lag(startdate, 1, '2050-01-01') over (
			partition by desk
			,capitalunit order by startdate desc
			) endDate
from distinct_capunit
