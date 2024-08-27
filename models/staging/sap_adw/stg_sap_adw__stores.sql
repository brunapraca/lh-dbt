with 
    rennamed as (
        select 
            cast(businessentityid as int) as pk_loja
            , cast(salespersonid as int) as fk_vendedor_associado
            , cast(name as string) as nome_loja
            -- demographics -- este campo não será necessário para essa análise.
            -- rowguid -- este campo não será necessário para essa análise.
            -- modifieddate -- este campo não será necessário para essa análise.
        from {{ source('sap_adw', 'store') }}
    )
select *
from rennamed
