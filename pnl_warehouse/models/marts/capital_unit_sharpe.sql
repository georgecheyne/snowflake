{{ config(materialized='view') }}
with pnl_adj as (
    select value_date,
            capunit_key,
            ytdpnl
     from {{ref('capital_unit_ytd_pnl')}}
     union all
         select value_date,
            capunit_key,
            ytdpnl
     from {{ref('capital_unit_ytd_perm_adj')}}
),
pnl_adj_distinct as (
    select value_date, capunit_key, sum(ytdpnl) as ytdpnl
        from pnl_adj
    group by value_date, capunit_key     
),
 ytd_chg as (
    select 
        tv.value_date, 
        tv.capunit_key, 
        tv.ytdpnl,
        lag(tv.ytdpnl, 1, 0) over (partition by tv.capunit_key order by tv.value_date) as YtdPnLT1,
        ca.value cap_alloc
    from 
        pnl_adj_distinct tv
        inner join {{ ref('capital_allocation') }} ca on tv.value_date between ca.start_date and ca.end_date and tv.capunit_key = ca.capunit_key
    where
        ca.value != 0
    ),
dly_rtn as (
    select
        *,
        ytdpnl-YtdPnLT1 YtdChg,
        (ytdpnl-YtdPnLT1)/cap_alloc as DlyRtn
    from
        ytd_chg
),
dly_rtn_vol as(
    select
        *,
        stddev(DlyRtn) over (partition by capunit_key order by value_date range between interval '3 months' preceding and current row) as DlyRtnVol3M,
        count(1) over (partition by capunit_key order by value_date range between interval '3 months' preceding and current row) as Count3M,
        stddev(DlyRtn) over (partition by capunit_key order by value_date range between interval '6 months' preceding and current row) as DlyRtnVol6M,
        count(1) over (partition by capunit_key order by value_date range between interval '6 months' preceding and current row) as Count6M,
        stddev(DlyRtn) over (partition by capunit_key order by value_date range between interval '12 months' preceding and current row) as DlyRtnVol12M,
        count(1) over (partition by capunit_key order by value_date range between interval '12 months' preceding and current row) as Count12M,
        stddev(DlyRtn) over (partition by capunit_key order by value_date range between unbounded preceding and current row) as DlyRtnVolITD,
        count(1) over (partition by capunit_key order by value_date range between unbounded preceding and current row) as CountITD
    from 
        dly_rtn
),
dly_rtn_vol_ann as (
    select
        *,
        sqrt(Count3M) * DlyRtnVol3M as DlyRtnVolAnn3M,
        sqrt(Count6M) * DlyRtnVol6M as DlyRtnVolAnn6M,
        sqrt(Count12M) * DlyRtnVol12M as DlyRtnVolAnn12M,
        sqrt(CountITD) * DlyRtnVolITD as DlyRtnVolAnnITD
    from
        dly_rtn_vol
),
dly_rtn_avg as (
    select
        *,
        avg(DlyRtn) over (partition by capunit_key order by value_date range between interval '3 months' preceding and current row) as DlyRtnAvg3M,
        avg(DlyRtn) over (partition by capunit_key order by value_date range between interval '6 months' preceding and current row) as DlyRtnAvg6M,
        avg(DlyRtn) over (partition by capunit_key order by value_date range between interval '12 months' preceding and current row) as DlyRtnAvg12M,
        avg(DlyRtn) over (partition by capunit_key order by value_date range between unbounded preceding and current row) as DlyRtnAvgITD
    from
        dly_rtn_vol_ann
),
dly_rtn_avg_ann as (
    select
        *,
        Count3M * DlyRtnAvg3M as DlyRtnAvgAnn3M,
        Count6M * DlyRtnAvg6M as DlyRtnAvgAnn6M,
        Count12M * DlyRtnAvg12M as DlyRtnAvgAnn12M,
        CountITD * DlyRtnAvgITD as DlyRtnAvgAnnITD
    from
        dly_rtn_avg
),
sharpe as (
    select 
        *,
        case 
            when DlyRtnVolAnn3M = 0 then 0 
            else DlyRtnAvgAnn3M/DlyRtnVolAnn3M
        end Sharpe3M,
        case 
            when DlyRtnVolAnn6M = 0 then 0 
            else DlyRtnAvgAnn6M/DlyRtnVolAnn6M
        end Sharpe6M,
        case 
            when DlyRtnVolAnn12M = 0 then 0 
            else DlyRtnAvgAnn12M/DlyRtnVolAnn12M
        end Sharpe12M,
        case 
            when DlyRtnVolAnnITD = 0 then 0 
            else DlyRtnAvgAnnITD/DlyRtnVolAnnITD
        end SharpeITD
    from
        dly_rtn_avg_ann
)

select * from sharpe