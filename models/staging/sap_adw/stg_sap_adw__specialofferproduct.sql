with 
    rennamed as (
        select 
            cast(SpecialOfferID as int) as pk_promocao
            , cast(ProductID as int) as fk_produto
            --rowguid - não vamos usar essa coluna para essa análise
            --modifiedDate - não vamos usar essa coluna para essa análise
        from {{ source('sap_adw', 'specialofferproduct') }}
    ) 
select * 
from rennamed
