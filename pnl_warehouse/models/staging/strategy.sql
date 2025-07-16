with strat
as (
	select strategy
		,valuedate
	from {{ source('pnl', 'transaction_value') }}
	)
	,distinct_strat
as (
	select strategy
		,min(valuedate) as startdate
	from strat
	group by strategy
	)
select {{ dbt_utils.generate_surrogate_key([
        'strategy', 
        'startdate']) }} as strategy_key
	,strategy
	,startdate
	,cast(lag(startdate, 1, '2050-01-01') over (
			partition by strategy order by startdate desc
			) as datetime) endDate
from distinct_strat
