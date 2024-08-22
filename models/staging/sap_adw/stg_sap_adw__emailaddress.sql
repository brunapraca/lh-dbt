with
    rennamed as (
        select 
        cast(businessentityid as int) as pk_email_cliente
        ,  cast(emailaddressid as int) as fk_email_cliente
        ,  cast(emailaddress as string) as email_cliente
        -- rowguid -- não será necessário para essa análise
        -- modifieddat -- não será necessário para essa análise 
        from {{ source('dbt_brunapraca', 'personemailaddress') }}
    )
select * 
from rennamed