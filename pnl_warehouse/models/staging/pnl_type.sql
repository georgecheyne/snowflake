with raw_values as (
select 'PnL' as pnl_type
)
select {{ dbt_utils.generate_surrogate_key([
        'pnl_type']) }} as pnl_type_key,
        pnl_type
        from raw_values