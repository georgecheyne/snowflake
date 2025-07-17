with raw_values as (
select 'DTD' as period
union
select 'MTD' as period
union
select 'YTD' as period
)
select {{ dbt_utils.generate_surrogate_key([
        'period']) }} as period_key,
        period
        from raw_values
