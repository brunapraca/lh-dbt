with
    rennamed as (
        select 
        cast(businessentityid as int) as pk_entidade
        , cast(emailaddressid as int) as pk_email_cliente
        , cast(emailaddress as string) as email_cliente
        , cast(modifieddate as timestamp) as data_modificacao
         -- rowguid -- não será necessário para essa análise
        from {{ source('dbt_brunapraca', 'personemailaddress') }}
    )
select * 
from rennamed
order by pk_entidade