WITH
    vendas_detalhes as (
        select * 
        from {{ ref('stg_sap_adw__salesorderdetails') }}
    )

    , vendas as (
        select * 
        from {{ ref('stg_sap_adw__salesorderheaders') }}
    )

    , lojas as (
        select *
        from {{ ref('stg_sap_adw__stores') }}
    )

    , territorio as (
        select * 
        from {{ ref('dim_territorios') }}
    )

    , produto as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , joined as (
       select
            vendas.pk_pedido
            , vendas.data_pedido as orderdate     
            , vendas_detalhes.quantidade_comprada as orderqty
            , territorio.nome_cidade as City
            , territorio.nome_estado as StateProvinceName
            , territorio.nome_pais as CountryRegionName
            , produto.nome_produto as name
        from vendas
        left join vendas_detalhes
            on vendas.pk_pedido = vendas_detalhes.pk_pedido
        left join lojas
            on vendas.fk_vendedor = lojas.fk_vendedor_associado
        left join territorio
            on vendas.fk_endereco = territorio.fk_endereco
        left join produto
            on produto.pk_produto = vendas_detalhes.fk_produto
    )
select *
from joined