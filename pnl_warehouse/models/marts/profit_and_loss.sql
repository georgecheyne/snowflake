with transaction_value_by_period as (
SELECT 
    ValueDate as VALUEDATE,
    'Arcesium' as SOURCESYSTEM,
    'Unknown' as DESK,
    businessunit as CAPITALUNIT,
    bundle as STRATEGY,
    'BC69' as FUND,
    SecurityDescription AS INSTRUMENTNAME,
    SfsType as ASSETTYPE,
    SubType as INVESTMENTTYPE,
    '' as INVESTMENTSUBTYPE,
    'Final' as DAILYVALUESTATUS,
	'NA' as MONTHENDVALUESTATUS,
    'General Ledger' as source,
    'DTD' AS period,
    'NAV' as type,
    DailyEndBookNav AS VALUE
FROM {{ ref('arcesuim_export') }}

union all

SELECT 
    ValueDate as VALUEDATE,
    'Arcesium' as SOURCESYSTEM,
    'Unknown' as DESK,
    businessunit as CAPITALUNIT,
    bundle as STRATEGY,
    'BC69' as FUND,
    SecurityDescription AS INSTRUMENTNAME,
    SfsType as ASSETTYPE,
    SubType as INVESTMENTTYPE,
    '' as INVESTMENTSUBTYPE,
    'Final' as DAILYVALUESTATUS,
	'NA' as MONTHENDVALUESTATUS,
    'General Ledger' as source,
    'DTD' AS period,
    'PnL' as type,
    Dtd AS VALUE
FROM {{ ref('arcesuim_export') }}

union all

SELECT 
   ValueDate as VALUEDATE,
    'Arcesium' as SOURCESYSTEM,
    'Unknown' as DESK,
    businessunit as CAPITALUNIT,
    bundle as STRATEGY,
    'BC69' as FUND,
    SecurityDescription AS INSTRUMENTNAME,
    SfsType as ASSETTYPE,
    SubType as INVESTMENTTYPE,
    '' as INVESTMENTSUBTYPE,
    'Final' as DAILYVALUESTATUS,
	'NA' as MONTHENDVALUESTATUS,
    'General Ledger' as source,
    'MTD' AS period,
    'PnL' as type,
    Mtd AS VALUE
FROM {{ ref('arcesuim_export') }}

union all

SELECT 
   ValueDate as VALUEDATE,
    'Arcesium' as SOURCESYSTEM,
    'Unknown' as DESK,
    businessunit as CAPITALUNIT,
    bundle as STRATEGY,
    'BC69' as FUND,
    SecurityDescription AS INSTRUMENTNAME,
    SfsType as ASSETTYPE,
    SubType as INVESTMENTTYPE,
    '' as INVESTMENTSUBTYPE,
    'Final' as DAILYVALUESTATUS,
	'NA' as MONTHENDVALUESTATUS,
    'General Ledger' as source,
    'YTD' AS period,
    'PnL' as type,
    Ytd AS VALUE
FROM {{ ref('arcesuim_export') }}

UNION ALL

SELECT 
    to_date(VALUEDATE, 'DD/MM/YYYY HH24:MI:SS') as VALUEDATE,
    SOURCESYSTEM,
    DESK,
    CAPITALUNITREPORTINGNAME as CAPITALUNIT,
    STRATEGY,
    FUND,
    INSTRUMENTNAME,
    ASSETTYPE,
    INVESTMENTTYPE,
    INVESTMENTSUBTYPE,
    DAILYVALUESTATUS,
	MONTHENDVALUESTATUS,
    'Daily Adjustment' as source,
    'DTD' AS period,
    'PnL' as type,
    DAILYADJUSTMENT AS VALUE
FROM {{ source('pnl', 'daily_adjustment') }}

UNION ALL

SELECT 
    to_date(VALUEDATE, 'DD/MM/YYYY HH24:MI:SS') as VALUEDATE,
    SOURCESYSTEM,
    DESK,
    CAPITALUNITREPORTINGNAME as CAPITALUNIT,
    STRATEGY,
    FUND,
    INSTRUMENTNAME,
    ASSETTYPE,
    INVESTMENTTYPE,
    INVESTMENTSUBTYPE,
    DAILYVALUESTATUS,
	MONTHENDVALUESTATUS,
    'Daily Adjustment' as source,
    'MTD' AS period,
    'PnL' as type,
    MTDADJUSTMENT AS VALUE
FROM {{ source('pnl', 'daily_adjustment') }}

UNION ALL

SELECT 
    to_date(VALUEDATE, 'DD/MM/YYYY HH24:MI:SS') as VALUEDATE,
    SOURCESYSTEM,
    DESK,
    CAPITALUNITREPORTINGNAME as CAPITALUNIT,
    STRATEGY,
    FUND,
    INSTRUMENTNAME,
    ASSETTYPE,
    INVESTMENTTYPE,
    INVESTMENTSUBTYPE,
    DAILYVALUESTATUS,
	MONTHENDVALUESTATUS,
    'Daily Adjustment' as source,
    'YTD' AS period,
    'PnL' as type,
    YTDADJUSTMENT AS VALUE
FROM {{ source('pnl', 'daily_adjustment') }}

UNION ALL

SELECT 
    to_date(VALUEDATE, 'DD/MM/YYYY HH24:MI:SS') as VALUEDATE,
    SOURCESYSTEM,
    DESK,
    CAPITALUNITREPORTINGNAME as CAPITALUNIT,
    STRATEGY,
    FUND,
    INSTRUMENTNAME,
    ASSETTYPE,
    INVESTMENTTYPE,
    INVESTMENTSUBTYPE,
    DAILYVALUESTATUS,
	MONTHENDVALUESTATUS,
    'Daily Adjustment' as source,
    'DTD' AS period,
    'NAV' as type,
    NAVADJUSTMENT AS VALUE
FROM {{ source('pnl', 'daily_adjustment') }}

UNION ALL

SELECT 
    to_date(VALUEDATE, 'DD/MM/YYYY HH24:MI:SS') as VALUEDATE,
    SOURCESYSTEM,
    DESK,
    CAPITALUNIT,
    STRATEGY,
    FUND,
    INSTRUMENTNAME,
    ASSETTYPE,
    INVESTMENTTYPE,
    INVESTMENTSUBTYPE,
    DAILYVALUESTATUS,
	MONTHENDVALUESTATUS,
    'General Ledger' as source,
    'DTD' AS period,
    'PnL' as type,
    TOTALBOOKPLDAILY AS VALUE
FROM {{ source('pnl', 'transaction_value') }}

UNION ALL

SELECT 
    to_date(VALUEDATE, 'DD/MM/YYYY HH24:MI:SS') as VALUEDATE,
    SOURCESYSTEM,
    DESK,
    CAPITALUNIT,
    STRATEGY,
    FUND,
    INSTRUMENTNAME,
    ASSETTYPE,
    INVESTMENTTYPE,
    INVESTMENTSUBTYPE,
    DAILYVALUESTATUS,
	MONTHENDVALUESTATUS,
    'General Ledger' as source,
    'MTD' AS period,
    'PnL' as type,
    TOTALBOOKPLMTD AS VALUE
FROM {{ source('pnl', 'transaction_value') }}

UNION ALL

SELECT 
    to_date(VALUEDATE, 'DD/MM/YYYY HH24:MI:SS') as VALUEDATE,
    SOURCESYSTEM,
    DESK,
    CAPITALUNIT,
    STRATEGY,
    FUND,
    INSTRUMENTNAME,
    ASSETTYPE,
    INVESTMENTTYPE,
    INVESTMENTSUBTYPE,
    DAILYVALUESTATUS,
	MONTHENDVALUESTATUS,
    'General Ledger' as source,
    'YTD' AS period,
    'PnL' as type,
    TOTALBOOKPLYTD AS VALUE
FROM {{ source('pnl', 'transaction_value') }}

UNION ALL

SELECT 
    to_date(VALUEDATE, 'DD/MM/YYYY HH24:MI:SS') as VALUEDATE,
    SOURCESYSTEM,
    DESK,
    CAPITALUNIT,
    STRATEGY,
    FUND,
    INSTRUMENTNAME,
    ASSETTYPE,
    INVESTMENTTYPE,
    INVESTMENTSUBTYPE,
    DAILYVALUESTATUS,
	MONTHENDVALUESTATUS,
    'General Ledger' as source,
    'DTD' AS period,
    'NAV' as type,
    ENDBOOKNAV AS VALUE
FROM {{ source('pnl', 'transaction_value') }}

)

select VALUEDATE as value_date,
        {{ dbt_utils.generate_surrogate_key(['FUND']) }} as fund_key,
        {{ dbt_utils.generate_surrogate_key(['STRATEGY']) }} as strategy_key,
        {{ dbt_utils.generate_surrogate_key(['DESK', 'CAPITALUNIT']) }} as capunit_key,
        {{ dbt_utils.generate_surrogate_key(['INSTRUMENTNAME', 'ASSETTYPE', 'INVESTMENTTYPE']) }} as instrument_key,
        {{ dbt_utils.generate_surrogate_key(['period']) }} as period_key,
        {{ dbt_utils.generate_surrogate_key(['type']) }} as pnl_type_key,
        {{ dbt_utils.generate_surrogate_key(['source']) }} as source_key,
        DAILYVALUESTATUS as daily_value_status,
        MONTHENDVALUESTATUS as month_end_value_status,
        VALUE as value
    
from transaction_value_by_period