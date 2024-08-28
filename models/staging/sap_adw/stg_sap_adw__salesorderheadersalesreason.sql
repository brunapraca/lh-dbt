with
    rennamed as (
        select 
        cast(salesorderid as int) as pk_pedido
        , cast(salesreasonid as int) as pk_motivo_venda
        , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('sap_adw', 'salesorderheadersalesreason') }}
    )
select * 
from rennamed