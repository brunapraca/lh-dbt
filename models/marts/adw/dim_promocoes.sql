with 
    stg_promocao as (
        select *
        from {{ ref('stg_sap_adw__specialoffer') }}
    )
    , stg_promocao_produto as (
        select *
        from {{ ref('stg_sap_adw__specialofferproduct') }}
    )
    , stg_produto as (
        select *
        from {{ ref('stg_sap_adw__products') }}
    )
    , stg_pedidos as (
        select *
        from {{ ref('stg_sap_adw__salesorderdetails') }}
    )
    , stg_detalhe_pedido as (
        select * 
        from {{ ref('stg_sap_adw__salesorderheaders') }}
    )
    , transformacao_promocoes as (
        select 
            stg_promocao.pk_promocao
            , stg_produto.pk_produto
            , stg_promocao.descricao_promocao
            , stg_promocao.percentual_desconto
            , stg_promocao.tipo_promocao
            , stg_promocao.categoria_promocao
            , stg_promocao.data_inicio
            , stg_promocao.data_fim
            , stg_produto.nome_produto
            , date_diff(stg_promocao.data_fim, stg_promocao.data_inicio, day) as duracao_promocao
        from stg_promocao 
        left join stg_promocao_produto 
            on stg_promocao.pk_promocao = stg_promocao_produto.pk_promocao
        left join stg_produto 
            on stg_promocao_produto.fk_produto = stg_produto.pk_produto
    )
    , tabela_final as (
        select
            {{ dbt_utils.generate_surrogate_key(
                ['stg_pedidos.pk_pedido', 'transformacao_promocoes.pk_produto']
            ) }} as produto_pedido_sk
            , stg_pedidos.pk_pedido
            , stg_detalhe_pedido.data_pedido
            , stg_pedidos.fk_promocao
            , stg_pedidos.quantidade_comprada
            , stg_pedidos.preco_unitario
            , stg_pedidos.desconto_unitario
            , transformacao_promocoes.pk_produto
            , transformacao_promocoes.descricao_promocao
            , transformacao_promocoes.percentual_desconto
            , transformacao_promocoes.tipo_promocao
            , transformacao_promocoes.categoria_promocao
            , transformacao_promocoes.data_inicio
            , transformacao_promocoes.data_fim
            , transformacao_promocoes.nome_produto
            , transformacao_promocoes.duracao_promocao
        from stg_pedidos
        left join transformacao_promocoes 
            on stg_pedidos.fk_promocao = transformacao_promocoes.pk_promocao
            and stg_pedidos.fk_produto = transformacao_promocoes.pk_produto
        left join stg_detalhe_pedido
            on stg_pedidos.pk_pedido = stg_detalhe_pedido.pk_pedido
    )

select * 
from tabela_final
