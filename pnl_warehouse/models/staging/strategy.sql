with strat
as (
	select strategy
		,TO_DATE(SUBSTR(valuedate, 1, 10), 'dd/MM/yyyy') as parsed_valuedate
	from {{ source('pnl', 'transaction_value') }}
	)
	,distinct_strat
as (
	select strategy
		,min(parsed_valuedate) as startdate
	from strat
	group by strategy
	)
select {{ dbt_utils.generate_surrogate_key([
        'strategy', 
        'startdate']) }} as strategy_key
	,strategy
	,startdate
	,lag(startdate, 1, '2050-01-01') over (
			partition by strategy order by startdate desc
			) endDate
from distinct_strat
