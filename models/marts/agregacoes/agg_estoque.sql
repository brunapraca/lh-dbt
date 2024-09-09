with estoque as (
    select pk_pedido
        , fk_produto
        , data_pedido
        , quantidade_comprada
        , data_diponivel_venda
        , data_indisponivel_venda
        , quantidade_estoque
    from {{ ref('int_estoque') }}
),
estoque_inicial as (
    -- Seleciona o estoque inicial para cada produto
    select 
        fk_produto
        , min(data_diponivel_venda) as data_inicio_venda
        , quantidade_estoque as estoque_inicial
    from estoque
    group by fk_produto, quantidade_estoque
),
vendas_acumuladas as (
    -- Calcula as vendas acumuladas até cada data por produto
    select 
        fk_produto
        , data_pedido as data
        , sum(quantidade_comprada) as quantidade_vendida_dia
    from estoque
    group by fk_produto, data_pedido
),
estoque_diario as (
    -- Calcula o estoque diário, subtraindo as vendas acumuladas
    select 
        ei.fk_produto
        , va.data
        , ei.estoque_inicial 
            - coalesce(sum(va.quantidade_vendida_dia) 
                over (partition by ei.fk_produto order by va.data), 0) as estoque_atual
    from estoque_inicial ei
    left join vendas_acumuladas va
        on ei.fk_produto = va.fk_produto
)
-- Seleciona o estoque atual por produto e data
select 
    fk_produto
    , data
    , estoque_atual
from estoque_diario
order by fk_produto, data
