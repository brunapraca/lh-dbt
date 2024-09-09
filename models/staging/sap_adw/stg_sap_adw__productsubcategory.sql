with 
    rennamed as (
        select 
        cast (productsubcategoryid as int) as subcategoria_pk
        , cast (productcategoryid as int) as fk_produto_categoria
        , cast (name as string) as nome_subcategoria
        -- rowguid -- não será utilizado nesse análise
        -- modifieddate -- não será utilizado nessa análise
        from {{ source('sap_adw', 'productsubcategory') }}  
    )
select * 
from rennamed