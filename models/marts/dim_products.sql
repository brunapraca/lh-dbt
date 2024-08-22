with 
    stg_produto as (
        select *
        from {{ ref('stg_sap_adw__products') }}
    )

    , stg_detalhes_pedido as (
        select *
        from {{ ref('stg_sap_adw__salesorderdetails') }}
    )

    , stg_cabecalho_pedido as (
        select *
        from {{ ref('stg_sap_adw__salesorderheaders') }}
    ) 
        
    , stg_promocao_produto as (
        select *
        from {{ ref('stg_sap_adw__specialofferproduct') }}
    )

    , stg_ofertas as (
        select *
        from {{ ref('stg_sap_adw__specialoffer') }}
    )

    , transformacao as (
        select 
            row_number() over (order by stg_produto.pk_produto) as produto_sk --criação de uma chave surrogate 
            , stg_produto.pk_produto
            , stg_produto.nome_produto
            , stg_produto.produto_fabricado
            , stg_produto.produto_acabado
            , stg_produto.cor_produto
            , stg_produto.custo_padrao_produto
            , stg_produto.preco_venda_sugerido
            , stg_produto.dias_producao
            , stg_detalhes_pedido.pk_pedido
            , stg_detalhes_pedido.pk_detalhe_pedido
            , stg_detalhes_pedido.fk_promocao
            , stg_detalhes_pedido.quantidade_pedido
            , stg_detalhes_pedido.preco_unitario
            , stg_detalhes_pedido.desconto_unitario
            , stg_cabecalho_pedido.data_pedido
            , stg_cabecalho_pedido.status_pedido
            , stg_cabecalho_pedido.flag_pedido_online
            , stg_promocao_produto.pk_promocao
            , stg_ofertas.descricao_promocao
            , stg_ofertas.percentual_desconto
            , stg_ofertas.tipo_promocao
            , stg_ofertas.categoria_promocao
            , stg_ofertas.data_inicio
            , stg_ofertas.data_fim
        from stg_produto 
        left join stg_detalhes_pedido on stg_produto.pk_produto = stg_detalhes_pedido.fk_produto
        left join stg_cabecalho_pedido on stg_detalhes_pedido.pk_pedido = stg_cabecalho_pedido.pk_pedido
        left join stg_promocao_produto on stg_promocao_produto.fk_produto = stg_produto.pk_produto
        left join stg_ofertas on stg_ofertas.pk_promocao = stg_promocao_produto.pk_promocao 
    )
select * 
from transformacao
