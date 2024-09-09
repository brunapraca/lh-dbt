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

    , int_receita as (
        select *
        from {{ ref('int_receita') }}
    )

    , joined as (
        select
            int_sales.produto_pedido_sk 
            , vendas.pk_pedido 
            , vendas.fk_cliente 
            , vendas.fk_endereco 
            , vendas_detalhes.fk_produto 
            , vendas.data_pedido      
            , vendas.data_envio
            , int_sales.status_pedido
            , vendas.flag_pedido_online           
            , vendas_detalhes.quantidade_comprada
            , int_sales.preco_unitario
            , int_sales.desconto_unitario
            , int_sales.valor_com_desconto
            , int_sales.nome_status_pedido
            , int_receita.preco_venda_sugerido
            , (int_sales.valor_com_desconto * vendas_detalhes.quantidade_comprada) as valor_total_pedido
            , (vendas_detalhes.preco_unitario * vendas_detalhes.quantidade_comprada) as valor_pedido_sem_desconto
            , int_sales.tempo_frete
            , int_receita.custo_padrao_produto
            , (int_receita.custo_padrao_produto * vendas_detalhes.quantidade_comprada) as custo_pedido
            , int_receita.receita_total
            , int_receita.receita_total_multiplicada
            
        from vendas
        left join vendas_detalhes
            on vendas.pk_pedido = vendas_detalhes.pk_pedido
        left join int_sales
            on vendas.pk_pedido = int_sales.pk_pedido
        left join int_receita
            on int_receita.produto_pedido_sk = int_sales.produto_pedido_sk
    )

select *
from joined
