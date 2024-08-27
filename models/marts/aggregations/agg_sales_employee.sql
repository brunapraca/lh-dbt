with
    vendedores as (
        select
            pk_vendedor
            , fk_territorio
            , cota_vendas
            , vendas_ano_atual
        from {{ ref('stg_sap_adw__salesperson') }}
    )

    , metricas_vendas AS (
    SELECT
        vendas.fk_vendedor
        , SUM(vendas.quantidade_comprada) AS total_quantidade_comprada
        , SUM(vendas.valor_com_desconto) AS total_vendas
        , SUM(vendas.preco_unitario * vendas.quantidade_comprada) - SUM(vendas.valor_com_desconto) AS total_desconto
        , COUNT(DISTINCT vendas.pk_pedido) AS total_pedidos
        , COUNT(DISTINCT vendas.fk_cliente) AS total_clientes
        , AVG(vendas.valor_com_desconto) AS valor_medio_pedido
        , SUM(vendas.preco_unitario * vendas.quantidade_comprada) AS valor_bruto
    FROM {{ ref('int_sales') }} vendas
    GROUP BY vendas.fk_vendedor
)

SELECT
    vendedores.pk_vendedor
    , vendedores.fk_territorio
    , vendedores.cota_vendas
    , vendedores.vendas_ano_atual
    , metricas_vendas.total_vendas
    , metricas_vendas.total_quantidade_comprada
    , metricas_vendas.total_desconto
    , metricas_vendas.total_pedidos
    , metricas_vendas.total_clientes
    , metricas_vendas.media_tempo_frete
    , metricas_vendas.valor_medio_pedido
    , metricas_vendas.valor_bruto
    , CASE
        WHEN metricas_vendas.total_vendas >= vendedores.cota_vendas THEN 'Cota Atingida'
        ELSE 'Cota Não Atingida'
      END AS status_cota
FROM
    vendedores
LEFT JOIN
    metricas_vendas ON vendedores.pk_vendedor = metricas_vendas.fk_vendedor