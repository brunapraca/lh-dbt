with 
    rennamed as (
        select 
         cast(productcategoryid as int) as pk_produto_categoria
        , cast(name as string) as nome_produto_categoria
        -- rowguid -- não vamos utilizar nesse análise
        -- modifieddate -- não vamos utilizar nesse análise
        from {{ source('sap_adw','productcategory') }}
    )
select * 
from rennamed