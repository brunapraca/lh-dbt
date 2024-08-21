with 
    rennamed as (
        select 
        cast (businessentityid as int) as pk_entidade
        , cast (persontype as string) as tipo_entidade
        , cast (firstname as string) as nome_entidade
        --middlename - não será necessário para essa análise
        --namestyle - não será necessário para essa análise
        --title - não será necessário para essa análise
        --lastname- não será necessário para essa análise
        --suffix- não será necessário para essa análise
        --emailpromotion- não será necessário para essa análise
        --additionalcontactinfo- não será necessário para essa análise
        --demographics- não será necessário para essa análise
        --rowguid- não será necessário para essa análise
        --modifieddate- não será necessário para essa análise
        from {{ source('sap_adw', 'person') }} 
    )
select * 
from {{ source('sap_adw', 'person') }} 

