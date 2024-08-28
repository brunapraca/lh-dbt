with 
    vendas as (
        select *
        from {{ ref('stg_sap_adw__salesorderheaders') }}
    )

    , vendas_detalhes as (
        select *
        from {{ ref('stg_sap_adw__salesorderdetails') }}
    )

    ,  stg_territorio as (
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

    , status_pedido as (
        select
            pk_pedido
            , status_pedido
            , case 
                when status_pedido = 1 then 'Em processo'
                when status_pedido = 2 then 'Aprovado'
                when status_pedido = 3 then 'Em espera'
                when status_pedido = 4 then 'Rejeitado'
                when status_pedido = 5 then 'Enviado'
                when status_pedido = 6 then 'Cancelado'
                else 'sem_status'
            end as nome_status_pedido
        from vendas
    )

    , stg_vendas_cidade as (
        select 
            pk_territorio
            , pk_cidade
        from stg_territorio
        left join stg_provincia 
            on stg_territorio.pk_territorio = stg_provincia.fk_territorio
        left join stg_cidade 
            on stg_provincia.pk_provincia = stg_cidade.fk_provincia
    )

    , joined as (
        select 
            vendas.pk_pedido
            , stg_vendas_cidade.pk_cidade
            , stg_vendas_cidade.pk_territorio
            , vendas.fk_cliente
            , vendas.fk_vendedor
            , vendas.fk_territorio
            , vendas.fk_cartao_credito
            , vendas.data_pedido      
            , vendas.data_envio
            , vendas.status_pedido
            , vendas.flag_pedido_online
            , vendas_detalhes.pk_detalhe_pedido
            , vendas_detalhes.fk_promocao
            , vendas_detalhes.fk_produto
            , vendas_detalhes.quantidade_comprada
            , vendas_detalhes.preco_unitario
            , vendas_detalhes.desconto_unitario
            , status_pedido.nome_status_pedido
        from vendas
        left join vendas_detalhes
            on vendas.pk_pedido = vendas_detalhes.pk_pedido
        left join status_pedido
            on vendas.pk_pedido = status_pedido.pk_pedido
        left join stg_territorio
            on vendas.fk_territorio = stg_territorio.pk_territorio
        left join stg_vendas_cidade 
            on vendas.fk_territorio = stg_vendas_cidade.pk_territorio
    )

    , metricas as (
        select 
            pk_pedido
            , pk_detalhe_pedido
            , fk_promocao
            , fk_produto
            , fk_cliente
            , fk_vendedor
            , fk_territorio
            , fk_cartao_credito
            , data_pedido      
            , data_envio
            , status_pedido
            , nome_status_pedido
            , flag_pedido_online
            , quantidade_comprada
            , preco_unitario
            , (preco_unitario * (1 - desconto_unitario)) as valor_com_desconto
            , DATE_DIFF(data_envio, data_pedido, day) as tempo_frete
            , concat(cast(pk_pedido as string), '-', cast(pk_cidade as string)
                , '-' , cast(pk_territorio as string))
                as sk_regiao
            , MD5(
                concat(
                    cast(pk_pedido as string)
                    , cast(fk_cliente as string)
                    , cast(fk_promocao as string)
                    , cast(fk_produto as string)
                    , cast(fk_vendedor as string)
                    , cast(fk_territorio as string)
                    , cast(fk_cartao_credito as string)
                )
            ) AS sk_vendas
        from joined
    )
select *
from metricas
