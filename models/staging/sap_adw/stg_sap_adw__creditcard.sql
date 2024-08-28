with 
    rennamed as (
        select 
            cast(creditcardid as int) as pk_cartao_credito
            , cast (businessentityid as int) as pk_entidade
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('sap_adw', 'personcreditcard') }}
    )
select * 
from rennamed