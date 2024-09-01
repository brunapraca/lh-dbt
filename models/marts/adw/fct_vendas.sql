with 
    vendas as (
        select *
        from {{ ref('stg_sap_adw__salesorderheaders') }}
    )

    , vendas_detalhes as (
        select *
        from {{ ref('stg_sap_adw__salesorderdetails') }}
    )

    , int_sales as (
        select * 
        from {{ ref('int_sales') }}
    )

    , joined as (
        select 
            int_sales.sk_vendas
            , vendas.pk_pedido
            , vendas.fk_cliente
            , vendas.fk_vendedor
            , vendas.fk_cartao_credito
            , vendas_detalhes.fk_promocao
            , vendas_detalhes.pk_detalhe_pedido
            , vendas_detalhes.fk_produto
            , vendas.data_pedido      
            , vendas.data_envio
            , int_sales.status_pedido
            , vendas.flag_pedido_online           
            , vendas_detalhes.quantidade_comprada
            , vendas_detalhes.preco_unitario
            , vendas_detalhes.desconto_unitario
            , int_sales.nome_status_pedido
            , int_sales.valor_com_desconto
            , int_sales.tempo_frete
        from vendas
        left join vendas_detalhes
            on vendas.pk_pedido = vendas_detalhes.pk_pedido
        left join int_sales
            on vendas.pk_pedido = int_sales.pk_pedido
    )

select *
from joined
