with raw_values as (
    select 'General Ledger' as source
)

select {{ dbt_utils.generate_surrogate_key([
        'source']) }} as source_key,
        source
        from raw_values