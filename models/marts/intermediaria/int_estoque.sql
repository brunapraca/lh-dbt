with 
    estoque as (
        select 
        pk_produto
        , data_diponivel_venda
        , data_indisponivel_venda
        , quantidade_estoque
        from {{ ref('dim_produtos') }}
    )

    , vendas as (
        select 
            pk_pedido
            , fk_produto
            , data_pedido
            , max(quantidade_comprada) as quantidade_comprada
            from {{ ref('fct_vendas') }}
            group by
                pk_pedido
                , fk_produto
                , data_pedido
    )

    , joined as (
        select 
        vendas.pk_pedido
        , vendas.fk_produto
        , vendas.data_pedido
        , vendas.quantidade_comprada
        , estoque.data_diponivel_venda
        , estoque.data_indisponivel_venda
        , estoque.quantidade_estoque
        from estoque
        inner join vendas   
            on estoque.pk_produto = vendas.fk_produto
    )
select * 
from joined
where fk_produto IN (708)