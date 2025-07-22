with transaction_value_by_period as (

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
    'Adjustments' as source,
    'DTD' AS period,
    'Pnl' as type,
    DAILYADJUSTMENT AS VALUE
FROM {{ source('pnl', 'daily_adjustment') }}

UNION

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
    'Adjustments' as source,
    'DTD' AS period,
    'Pnl' as type,
    MTDADJUSTMENT AS VALUE
FROM {{ source('pnl', 'daily_adjustment') }}

UNION

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
    'Adjustments' as source,
    'DTD' AS period,
    'Pnl' as type,
    YTDADJUSTMENT AS VALUE
FROM {{ source('pnl', 'daily_adjustment') }}

UNION

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
    'Adjustments' as source,
    'DTD' AS period,
    'NAV' as type,
    NAVADJUSTMENT AS VALUE
FROM {{ source('pnl', 'daily_adjustment') }}

UNION

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
    'General Ledger' as source,
    'DTD' AS period,
    'Pnl' as type,
    TOTALBOOKPLDAILY AS VALUE
FROM {{ source('pnl', 'transaction_value') }}

UNION

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
    'General Ledger' as source,
    'MTD' AS period,
    'Pnl' as type,
    TOTALBOOKPLMTD AS VALUE
FROM {{ source('pnl', 'transaction_value') }}

UNION

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
    'General Ledger' as source,
    'YTD' AS period,
    'Pnl' as type,
    TOTALBOOKPLYTD AS VALUE
FROM {{ source('pnl', 'transaction_value') }}

UNION

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
        VALUE as value
    
from transaction_value_by_period