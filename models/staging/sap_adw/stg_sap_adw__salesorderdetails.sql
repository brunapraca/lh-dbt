with
    rennamed as (
        select 
        cast(salesorderid as int) as pk_pedido
            , cast(salesorderdetailid as int) as pk_detalhe_pedido
            , cast(specialofferid as int) as fk_promocao
            , cast(productid as int) as fk_produto
            , cast(orderqty as int) as quantidade_comprada 
            , cast(unitprice as decimal) as preco_unitario
            , cast(unitpricediscount as decimal) as desconto_unitario
            , cast(modifieddate as timestamp) as data_modificacao
            -- CarrierTrackingNumber -- este campo não será necessário para essa análise.
            -- Rowguid -- este campo não será necessário para essa análise.
            from {{ source('sap_adw', 'salesorderdetail') }}
    )
select *
from rennamed
