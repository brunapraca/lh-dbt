with
    rennamed as (
        select 
        cast(salesorderid as int) as pk_pedido
        , cast(salesreasonid as int) as pk_motivo_venda
        -- modifieddate -- não será necessário nessa análise
        from {{ source('sap_adw', 'salesorderheadersalesreason') }}
    )
select * 
from rennamed