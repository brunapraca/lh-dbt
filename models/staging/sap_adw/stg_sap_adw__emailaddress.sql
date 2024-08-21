with
    rennamed as (
        select
            cast(businessentityid as int) as pk_entidade
            , cast(emailaddressid as int) as fk_email_cliente
            , cast(emailaddress as string) as email_cliente
            -- rowguid -- não será usado nessa análise
            -- modifieddate -- não será usado nessa análise
        from {{ source('sap_adw', 'emailaddress') }}
    )
select *
from rennamed