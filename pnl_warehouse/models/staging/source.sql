with raw_values as (
    select 'General Ledger' as source
    union
    select 'Daily Adjustment' as source
    union 
    select 'Permannent Adjustment' as source
)

select {{ dbt_utils.generate_surrogate_key([
        'source']) }} as source_key,
        source
        from raw_values