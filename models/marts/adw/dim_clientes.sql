WITH 
    stg_cliente AS (
        SELECT *
        FROM {{ ref('stg_sap_adw__customers') }}
    )
    
    , stg_territorio AS (
        SELECT *
        FROM {{ ref('stg_sap_adw__salesterritory') }}
    )
    
    , stg_cadastro_pessoa AS (
        SELECT *
        FROM {{ ref('stg_sap_adw__person') }}
    )
    
    , stg_email AS (
        SELECT *
        FROM {{ ref('stg_sap_adw__emailaddress') }}
    )
    
    , stg_razao_compra AS (
        SELECT *
        FROM {{ ref('stg_sap_adw__salesreasons') }}
    )
    
    , stg_vendas AS (
        SELECT *
        FROM {{ ref('stg_sap_adw__salesorderheadersalesreason') }}
    )
    
    , stg_cartao_credito AS (
        SELECT *
        FROM {{ ref('stg_sap_adw__creditcard') }}
    )
    
    , stg_pedidos AS (
        SELECT * 
        FROM {{ ref('stg_sap_adw__salesorderheaders') }}
    )
    
    , stg_email_recent AS (
    SELECT 
        stg_email.pk_entidade
        , stg_email.email_cliente
        , stg_email.data_modificacao
        , ROW_NUMBER() OVER (
            PARTITION BY stg_email.pk_entidade 
            ORDER BY stg_email.data_modificacao DESC
        ) AS row_num
    FROM stg_email
    )
    
    , stg_razao_compra_unique AS (
        SELECT 
            stg_vendas.pk_pedido
            , stg_vendas.pk_motivo_venda
            , stg_razao_compra.nome_motivo_venda
            , stg_razao_compra.tipo_motivo_venda
            , stg_vendas.data_modificacao
            , ROW_NUMBER() OVER (
                PARTITION BY stg_vendas.pk_pedido 
                ORDER BY stg_vendas.data_modificacao DESC, stg_vendas.pk_motivo_venda DESC
            ) AS row_num
        FROM stg_vendas
        INNER JOIN stg_razao_compra
            ON stg_vendas.pk_motivo_venda = stg_razao_compra.pk_motivo_venda
    )
    
    , transformacao AS (
        SELECT 
            {{ dbt_utils.generate_surrogate_key(
                    ['stg_cliente.pk_cliente']
                )
            }} as cliente_sk
            , stg_cliente.pk_cliente
            , stg_territorio.nome_territorio
            , stg_cadastro_pessoa.nome_entidade
            , stg_email_recent.email_cliente
            , MAX(stg_razao_compra_unique.nome_motivo_venda) AS nome_motivo_venda
            , MAX(stg_razao_compra_unique.tipo_motivo_venda) AS tipo_motivo_venda
            , CASE 
                WHEN stg_cartao_credito.pk_entidade IS NOT NULL THEN TRUE   
                ELSE FALSE
            END AS usou_cartao_credito

        FROM stg_cliente
        LEFT JOIN stg_territorio 
            ON stg_cliente.fk_territorio = stg_territorio.pk_territorio
        LEFT JOIN stg_cadastro_pessoa 
            ON stg_cliente.fk_pessoa = stg_cadastro_pessoa.pk_entidade
        LEFT JOIN stg_email_recent 
            ON stg_cadastro_pessoa.pk_entidade = stg_email_recent.pk_entidade AND stg_email_recent.row_num = 1
        LEFT JOIN stg_pedidos 
            ON stg_cliente.pk_cliente = stg_pedidos.fk_cliente
        LEFT JOIN stg_razao_compra_unique 
            ON stg_pedidos.pk_pedido = stg_razao_compra_unique.pk_pedido AND stg_razao_compra_unique.row_num = 1
        LEFT JOIN stg_cartao_credito 
            ON stg_pedidos.fk_cartao_credito = stg_cartao_credito.pk_cartao_credito
        WHERE 
            stg_cadastro_pessoa.tipo_entidade = "IN"
        GROUP BY 
            stg_cliente.pk_cliente
            , stg_territorio.nome_territorio
            , stg_cadastro_pessoa.nome_entidade
            , stg_email_recent.email_cliente
            , stg_cartao_credito.pk_entidade
    )
SELECT * 
FROM transformacao
