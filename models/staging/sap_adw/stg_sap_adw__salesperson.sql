with 
    rennamed as (
        select 
            cast(businessentityid as int) as pk_vendedor
            , cast(territoryid as int) as fk_territorio
            , cast(salesquota as decimal) as cota_vendas
            , cast(salesytd as decimal) as vendas_ano_atual
            , cast(saleslastyear as decimal) as vendas_ano_anterior
            -- bonus -- este campo não será necessário para essa análise.
            -- comissionpct -- este campo não será necessário para essa análise.
            -- modifieddate --este campo não será necessário para essa análise.
            -- rowguid -- este campo não será necessário para essa análise.
        from {{ source('sap_adw', 'salesperson') }}
    )
select *
from rennamed
