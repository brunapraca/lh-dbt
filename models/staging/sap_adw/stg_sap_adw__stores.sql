with 
    rennamed as (
        select 
            cast(businessentityid as int) as pk_loja
            , cast(name as string) as nome_loja
            , cast(salespersonid as int) as pk_vendedor_associado

            -- demographics -- este campo não será necessário para essa análise.
            -- rowguid -- este campo não será necessário para essa análise.
            -- modifieddate -- este campo não será necessário para essa análise.
        from {{ source('sap_adw', 'store') }}
    )
select *
from rennamed
