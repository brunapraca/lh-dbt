with
    rennamed as (
        select 
        cast(stateprovinceid as int) as pk_provincia
        , cast(stateprovincecode as string) as code_provincia
        , cast(countryregioncode as string) as fk_code_regiao
        , cast(territoryid as int) as fk_territorio
        , cast(modifieddate as timestamp) as data_modificacao
        -- isonlystateprovinceflag -- não será usado para essa análise
        -- name -- não será usado para essa análise
        -- rowguid -- não será usado para essa análise
        from {{ source('sap_adw', 'stateprovince') }}
    )
select * 
from rennamed