with capunit
as (
	select tv.desk
		,tv.capitalunit
		,to_date(tv.valuedate, 'DD/MM/YYYY HH24:MI:SS') as valuedate_parsed
	from {{ source('pnl', 'transaction_value') }} tv 
    union 
    select adj.desk
		,adj.capitalunit
		,to_date(adj.valuedate, 'DD/MM/YYYY HH24:MI:SS') as valuedate_parsed
    from {{ source('pnl', 'daily_adjustment') }} adj 
    union 
    select perm.desk
		,perm.capitalunit
		,to_date(perm.valuedate, 'DD/MM/YYYY HH24:MI:SS') as valuedate_parsed
    from {{ source('pnl', 'permanent_adjustment') }} perm 
)
	,distinct_capunit as (
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
