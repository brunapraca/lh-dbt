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


    , transformacao as (
        select 
            row_number() over (order by stg_produto.pk_produto) as produto_sk --criação de uma chave surrogate  -- Criação de uma chave surrogate 
            , stg_produto.pk_produto as pk_produto
            , stg_produto.nome_produto as nome_produto
            , stg_produto.produto_fabricado as fabricado
            , stg_produto.produto_acabado as acabado
            , stg_produto.cor_produto as cor
            , stg_produto.custo_padrao_produto as custo_padrao
            , stg_produto.preco_venda_sugerido as preco_sugerido
            , stg_produto.dias_producao as dias_producao
        from stg_produto 
        left join stg_detalhes_pedido 
            on stg_produto.pk_produto = stg_detalhes_pedido.fk_produto
        left join stg_cabecalho_pedido 
            on stg_detalhes_pedido.pk_pedido = stg_cabecalho_pedido.pk_pedido
    )

select * 
from transformacao
