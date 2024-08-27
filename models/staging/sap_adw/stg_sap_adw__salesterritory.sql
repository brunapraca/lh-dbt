with    
    rennamed as (
        select 
            cast (territoryid as int) as pk_territorio
            , cast (name as string) as nome_territorio
            , cast (countryregioncode as string) as codigo_pais
            , cast (salesytd as decimal) as total_vendas_territorio
            -- group -- não será necessário nessa análise
            -- saleslastyear -- não será necessário nessa análise
            -- costytd -- não será necessário nessa análise
            -- costlastyear -- não será necessário nessa análise
            -- rowguid -- não será necessário nessa análise
            -- modifieddate -- não será necessário nessa análise
        from {{ source('sap_adw', 'salesterritory') }}
    )
select * 
from rennamed