{{ config(materialized ="view") }}

select
pnl.Date as ValueDate
    ,b.book_name as Book
    ,bu.bundle_name as Bundle
    ,bus.business_unit_name BusinessUnit
    ,ca.custodian_account_name as CustodianAccount
    ,ca.counter_party_name as Counterparty
    ,'BSMA Ltd' as LegalEntity
    ,sc.security_short_description as ExternalId
    ,sc.security_type as SfsType
    ,sc.security_subtype as SubType
    ,pnl.synthetic_security_id as Spn
    ,sc.security_name as SecurityDescription
    ,pnl.day_pnl_book Dtd
    ,pnl.month_to_date_pnl_book as Mtd
    ,pnl.year_to_date_pnl_book as Ytd
    ,pnl.gross_market_value_book as DailyEndBookNav
from {{ source('arcesium', 'derived__bundle_pnl_detail') }} pnl
inner join {{ source('arcesium', 'book') }} b on b.book_id = pnl.book_id and b.knowledge_end_date is null
inner join {{ source('arcesium', 'bundle') }} bu on bu.bundle_id = pnl.bundle_id and bu.knowledge_end_date is null
inner join {{ source('arcesium', 'business_unit') }} bus on bus.business_unit_id = bu.business_unit_id and bus.knowledge_end_date is null
inner join {{ source('arcesium', 'custodian_account') }} ca on ca.custodian_account_id = pnl.custodian_account_id and ca.knowledge_end_date is null
inner join {{ source('arcesium', 'security_common') }} sc on sc.security_id = pnl.synthetic_security_id and sc.knowledge_end_date is null
where 1 = 1
and pnl.knowledge_end_date is null