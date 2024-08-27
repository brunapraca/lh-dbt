with 
    rennamed as (
        select 
            cast (countryregioncode as string) as pk_pais
            , cast (name as string) as nome_pais
            -- modifieddate -- não é necessário usar essa coluna neste momento. 
        from {{ source('sap_adw', 'countryregion') }}
    )
select * 
from rennamed 