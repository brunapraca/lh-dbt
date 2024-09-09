with
    vendas as (
        select *
        from {{ ref('stg_sap_adw__salesorderheaders') }}
    )

    , vendas_detalhes as (
        select *
        from {{ ref('stg_sap_adw__salesorderdetails') }}
    )

    , stg_territorio as (
        select *
        from {{ref('stg_sap_adw__salesterritory')}}
        )

    , stg_cidade as (
        select *
        from {{ref('stg_sap_adw__addresses')}}
    )

    , stg_provincia as (
        select *
        from {{ref('stg_sap_adw__stateprovince')}}
    )

    , stg_pais as (
        select *
        from {{ref('stg_sap_adw__countryregions')}}
        )

    , joined as (
        select 
            vendas.pk_pedido
            , vendas.fk_endereco
            , stg_territorio.pk_territorio
            , vendas_detalhes.fk_produto
            , vendas.status_pedido
            , stg_territorio.nome_territorio
            , stg_pais.nome_pais
            , stg_provincia.code_provincia
            , stg_cidade.cidade
            , vendas_detalhes.quantidade_comprada
            , vendas_detalhes.preco_unitario
            , stg_territorio.total_vendas_territorio
        from vendas
        inner join vendas_detalhes
            on vendas.pk_pedido = vendas_detalhes.pk_pedido
        inner join stg_territorio 
            on vendas.fk_territorio = stg_territorio.pk_territorio
        inner join stg_provincia 
            on stg_territorio.pk_territorio = stg_provincia.fk_territorio
        inner join stg_cidade 
            on stg_provincia.pk_provincia = stg_cidade.fk_provincia
        inner join stg_pais 
            on stg_provincia.fk_code_regiao = stg_pais.pk_pais
        /* Para filtrar somente as vendas desses pedidos. Campo condicional = "status_pedido", 
        se for igual a 2 ("Pedido Aprovado") e 5 ("Pedido Enviado"), então é uma venda. */
        where status_pedido in (2,5)
    )

    , metricas as (
        select
            fk_endereco
            , nome_territorio as nome_regiao
            , nome_pais as pais
            , cidade
            , COUNT(distinct pk_pedido) as qnt_pedido_cidade
            , SUM(quantidade_comprada) as total_vendas_cidade
            , SUM(preco_unitario) as valor_negociado_cidade
        from joined
        group by 
            fk_endereco
            , cidade
            , nome_territorio
            , nome_pais
    )
select * 
from metricas