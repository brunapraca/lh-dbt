with  
    rennamed as (
        select 
            cast(addressid as int) as pk_cidade
            , cast(stateprovinceid as int) as fk_provincia
            , cast(city as string) as cidade
            , cast(modifieddate as timestamp) as data_modificacao
            --addressline1 -- não será necessário usar essa coluna agora
            -- addressline2 -- não será necessário usar essa coluna agora
            -- postalcode -- não será necessário usar essa coluna agora
            -- spatiallocation -- não será necessário usar essa coluna agora
            -- rowguid -- não será necessário usar essa coluna agora
        from {{ source('sap_adw', 'address') }}
    )
select * 
from rennamed