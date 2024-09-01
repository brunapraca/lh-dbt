WITH 
    stg_produto AS (
        SELECT *
        FROM {{ ref('stg_sap_adw__products') }}
    )
    
    , stg_detalhes_pedido AS (
        SELECT *
        FROM {{ ref('stg_sap_adw__salesorderdetails') }}
    )
    
    , stg_cabecalho_pedido AS (
        SELECT *
        FROM {{ ref('stg_sap_adw__salesorderheaders') }}
    )

    , transformacao AS (
        SELECT 
            {{ dbt_utils.generate_surrogate_key(
                    ['stg_produto.pk_produto']
                )
            }} as produto_sk
            , stg_produto.pk_produto AS pk_produto
            , MAX(stg_produto.nome_produto) AS nome_produto
            , MAX(stg_produto.produto_fabricado) AS fabricado
            , MAX(stg_produto.produto_acabado) AS acabado
            , MAX(stg_produto.cor_produto) AS cor
            , MAX(stg_produto.custo_padrao_produto) AS custo_padrao
            , MAX(stg_produto.preco_venda_sugerido) AS preco_sugerido
            , MAX(stg_produto.dias_producao) AS dias_producao
        FROM stg_produto 
        LEFT JOIN stg_detalhes_pedido 
            ON stg_produto.pk_produto = stg_detalhes_pedido.fk_produto
        LEFT JOIN stg_cabecalho_pedido 
            ON stg_detalhes_pedido.pk_pedido = stg_cabecalho_pedido.pk_pedido
        GROUP BY 
            stg_produto.pk_produto
    )

SELECT * 
FROM transformacao
