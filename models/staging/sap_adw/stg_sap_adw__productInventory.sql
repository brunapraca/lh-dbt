with 
    rennamed as (
        select 
            cast(ProductID as int) as pk_produto
            , cast(LocationID as int) as fk_localizacao
            , cast(Quantity as int) as quantidade_estoque
            -- LocationID -- não será necessário para essa análise
            -- Shelf -- não será necessário para essa análise
            -- Bin -- não será necessário para essa análise
            -- rowguid -- não será necessário para essa análise
            -- modifiedDate -- não será necessário para essa análise
        from {{ source('sap_adw', 'productinventory') }}
    )
select * 
from rennamed

