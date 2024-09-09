with 
    vendas as (
        select * 
        from {{ ref('int_sales') }}
    )   

    , produto as (
        select 
            pk_produto
            , custo_padrao_produto
            , preco_venda_sugerido
        from {{ ref('stg_sap_adw__products') }}
    )
    
    , joined as (
        select 
            {{ dbt_utils.generate_surrogate_key(
                ['vendas.pk_pedido', 'vendas.fk_produto']
            ) }} as produto_pedido_sk 
            , vendas.fk_produto
            , vendas.pk_pedido
            , vendas.data_pedido      
            , vendas.data_envio
            , vendas.status_pedido
            , vendas.quantidade_comprada
            , vendas.preco_unitario
            , produto.preco_venda_sugerido
            , (vendas.preco_unitario * (1 - vendas.desconto_unitario)) as valor_com_desconto
            , produto.custo_padrao_produto
            , ((vendas.preco_unitario * (1 - vendas.desconto_unitario)) - produto.custo_padrao_produto) as receita_total
            , ((vendas.preco_unitario * (1 - vendas.desconto_unitario)) - produto.custo_padrao_produto) * vendas.quantidade_comprada as receita_total_multiplicada
        from vendas
        left join produto
            on vendas.fk_produto = produto.pk_produto
    )

select 
    produto_pedido_sk
    , data_pedido
    , status_pedido
    , fk_produto
    , quantidade_comprada
    , preco_unitario
    , valor_com_desconto
    , custo_padrao_produto
    , preco_venda_sugerido
    , receita_total
    , receita_total_multiplicada
from joined
where status_pedido in (2, 5)