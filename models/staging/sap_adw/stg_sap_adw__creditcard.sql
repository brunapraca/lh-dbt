with 
    rennamed as (
        select 
            cast(creditcardid as int) as pk_cartao_credito
            , cast (businessentityid as int) as pk_entidade
            -- modifieddate -- não será necessário usar para essa análise 
        from {{ source('sap_adw', 'personcreditcard') }}
    )

select * 
from rennamed