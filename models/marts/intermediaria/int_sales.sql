with 
    vendas as (
        select *
        from {{ ref('stg_sap_adw__salesorderheaders') }}
    )

    , vendas_detalhes as (
        select *
        from {{ ref('stg_sap_adw__salesorderdetails') }}
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

    , joined as (
        select 
            {{ dbt_utils.generate_surrogate_key(
                ['vendas.pk_pedido', 'vendas_detalhes.fk_produto']
            ) }} as produto_pedido_sk 
            , vendas.pk_pedido
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
    )

    , metricas as (
        select 
            produto_pedido_sk 
            , pk_pedido
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
            , desconto_unitario
            , preco_unitario
            , (preco_unitario * (1 - desconto_unitario)) as valor_com_desconto
            , quantidade_comprada * (preco_unitario * (1 - desconto_unitario)) as valor_total_compra
            , DATE_DIFF(data_envio, data_pedido, day) as tempo_frete
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
            ) as sk_vendas
        from joined
    )
select *
from metricas
