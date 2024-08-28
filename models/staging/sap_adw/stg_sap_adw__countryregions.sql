with 
    rennamed as (
        select 
            cast (countryregioncode as string) as pk_pais
            , cast (name as string) as nome_pais
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('sap_adw', 'countryregion') }}
    )
select * 
from rennamed 