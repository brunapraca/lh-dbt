with 
    rennamed as (
        select 
        cast(customerid as int) as pk_cliente
        , cast(personid as int) as fk_pessoa
        , cast(territoryid as int) as fk_territorio
        , cast(modifieddate as timestamp) as data_modificacao
       -- storeid - não será necessário para essa análise
       -- rowguid - não será necessário para essa análise
        from {{ source('sap_adw', 'customer') }} 
    )
select * 
from rennamed

