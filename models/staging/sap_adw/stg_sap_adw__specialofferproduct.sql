with 
    rennamed as (
        select 
            cast(SpecialOfferID as int) as pk_promocao
            , cast(ProductID as int) as fk_produto
            , cast(modifieddate as timestamp) as data_modificacao
            --rowguid - não vamos usar essa coluna para essa análise
        from {{ source('sap_adw', 'specialofferproduct') }}
    ) 
select * 
from rennamed
