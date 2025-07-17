with instrument_type
as (
	select instrumentname
		,assettype
		,investmenttype
		,TO_DATE(SUBSTR(valuedate, 1, 10), 'dd/MM/yyyy') as parsed_valuedate
	from {{ source('pnl', 'transaction_value') }}
	)
	,distinst_investment_type
as (
	select instrumentname
		,assettype
		,investmenttype
		,min(parsed_valuedate) as startdate
	from instrument_type
	group by instrumentname
		,assettype
		,investmenttype
	)
	
    select {{ dbt_utils.generate_surrogate_key (
	[
        'instrumentname', 
        'assettype', 
        'investmenttype']
	) }}
as investment_type_key
	,instrumentname
	,assettype
	,investmenttype 
    ,startdate
	,lag (
	startdate
	,1
	,'2050-01-01'
	) over (
	partition by instrumentname, assettype, investmenttype order by startdate desc
	) endDate
    from distinst_investment_type
