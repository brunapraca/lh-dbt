with
    rennamed as (
        select
            cast(SpecialOfferID as int) as pk_promocao
            , cast(Description as string) as descricao_promocao
            , cast(DiscountPct as decimal) as percentual_desconto
            , cast(Type as string) as tipo_promocao
            , cast(Category as string) as categoria_promocao
            , cast(StartDate as timestamp) as data_inicio
            , cast(EndDate as timestamp) as data_fim
            , cast(modifieddate as timestamp) as data_modificacao
            --MinQty  - não será necessário para essa análise
            --MaxQty - não será necessário para essa análise
            --rowguid - não será necessário para essa análise
        from {{ source('sap_adw', 'specialoffer') }}   
    )
select * 
from rennamed