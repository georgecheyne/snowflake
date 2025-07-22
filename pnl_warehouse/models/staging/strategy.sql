with strat
as (
	select strategy
		,to_date(valuedate, 'DD/MM/YYYY HH24:MI:SS') as parsed_valuedate
	from {{ source('pnl', 'transaction_value') }}
    union
    select strategy
		,to_date(valuedate, 'DD/MM/YYYY HH24:MI:SS') as parsed_valuedate
	from {{ source('pnl', 'daily_adjustment') }}
	)
	,distinct_strat
as (
	select strategy
		,min(parsed_valuedate) as startdate
	from strat
	group by strategy
	)
select {{ dbt_utils.generate_surrogate_key([
        'strategy']) }} as strategy_key
	,strategy
	,startdate
	,lag(startdate, 1, '2050-01-01') over (
			partition by strategy order by startdate desc
			) endDate
from distinct_strat
