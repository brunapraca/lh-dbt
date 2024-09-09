with
    vendedores as (
        select *
        from {{ ref('stg_sap_adw__salesperson') }}
    )

    , vendas as (
        select *
        from {{ ref('stg_sap_adw__salesorderheaders') }}
    )

    , vendas_detalhes as (
        select *
        from {{ ref('stg_sap_adw__salesorderdetails') }}
    )

    , lojas as (
        select *
        from {{ ref('stg_sap_adw__stores') }}
    )

    , joined as (
        select
            vendedores.pk_vendedor
            , vendedores.fk_territorio
            , vendedores.cota_vendas
            , vendedores.vendas_ano_atual
            , vendedores.vendas_ano_anterior
            , vendas.pk_pedido
            , vendas.data_pedido
            , vendas_detalhes.quantidade_comprada
            , lojas.pk_entidade
            , lojas.fk_vendedor_associado
            , lojas.nome_loja
        from vendedores
        left join vendas
            on vendedores.pk_vendedor = vendas.fk_vendedor
        left join vendas_detalhes
            on vendas.pk_pedido = vendas_detalhes.pk_pedido
        left join lojas
            on vendedores.pk_vendedor = lojas.fk_vendedor_associado
    )

select
    pk_vendedor
    , pk_pedido
    , sum(quantidade_comprada) as total_quantidade_comprada  
    , max(fk_territorio) as fk_territorio 
    , max(cota_vendas) as cota_vendas  
    , max(vendas_ano_atual) as vendas_ano_atual  
    , max(vendas_ano_anterior) as vendas_ano_anterior  
    , max(data_pedido) as data_pedido  
    , max(nome_loja) as nome_loja  
from joined
group by pk_vendedor
    , pk_pedido
