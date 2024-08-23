with 
    stg_cliente as (
        select *
        from {{ ref('stg_sap_adw__customers') }}
    )

    , stg_territorio as (
        select *
        from {{ ref('stg_sap_adw__salesterritory') }}
    )

    , stg_cadastro_pessoa as (
        select *
        from {{ ref('stg_sap_adw__person') }}
    )

    , stg_email as (
        select *
        from {{ ref('stg_sap_adw__emailaddress') }}
    )

    , stg_cartao_credito as (
        select *
        from {{ ref('stg_sap_adw__creditcard') }}
    )

    , stg_razao_compra as (
        select * 
        from {{ ref('stg_sap_adw__salesreasons') }}
    )

    , stg_id_razaocompras as (
        select *
        from {{ ref('stg_sap_adw__salesorderheadersalesreason') }}
    )

    , stg_vendas as (
        select * 
        from {{ ref('stg_sap_adw__salesorderheaders') }}
    )


    , transformacao as (
       select 
            row_number() over (order by stg_cliente.pk_cliente) as cliente_sk  -- criação de uma chave surrogat
            , stg_cliente.pk_cliente
            , stg_territorio.nome_territorio
            , stg_territorio.codigo_pais
            , stg_territorio.total_vendas_territorio
            , stg_cadastro_pessoa.tipo_entidade
            , stg_cadastro_pessoa.nome_entidade
            , stg_email.email_cliente
            , case when stg_cartao_credito.pk_entidade is not null then 1 else 0 end as usou_cartao_credito -- Identifica se o cliente usou cartão de crédito
            , stg_razao_compra.nome_motivo_venda
            , stg_razao_compra.tipo_motivo_venda

        from stg_cliente
        left join stg_territorio on stg_cliente.fk_territorio = stg_territorio.pk_territorio
        left join stg_cadastro_pessoa on stg_cliente.fk_pessoa = stg_cadastro_pessoa.pk_entidade
        left join stg_email on stg_cadastro_pessoa.pk_entidade = stg_email.pk_entidade
        left join stg_vendas on stg_cliente.pk_cliente = stg_vendas.pk_cliente
        left join stg_id_razaocompras on stg_vendas.pk_pedido = stg_id_razaocompras.pk_pedido
        left join stg_razao_compra on stg_id_razaocompras.pk_motivo_venda = stg_razao_compra.pk_motivo_venda
        left join stg_cartao_credito on stg_vendas.pk_cartao_credito = stg_cartao_credito.pk_cartao_credito
    )
select * 
from transformacao
