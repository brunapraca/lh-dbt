with
renamed as (
    select 
        cast(salesorderid as int) as pk_pedido
        , cast(revisionnumber as int) as numero_revisao
        , cast(orderdate as timestamp) as data_pedido
        , cast(duedate as timestamp) as data_vencimento
        , cast(shipdate as timestamp) as data_envio
        , cast(status as string) as status_pedido
        , cast(onlineorderflag as boolean) as flag_pedido_online
        , cast(purchaseordernumber as string) as numero_pedido_compra
        , cast(accountnumber as string) as numero_conta
        , cast(customerid as int) as pk_cliente
        , cast(salespersonid as int) as pk_vendedor
        , cast(territoryid as int) as pk_territorio
        , cast(billtoaddressid as int) as pk_endereco_faturamento
        , cast(shiptoaddressid as int) as pk_endereco_entrega
        , cast(shipmethodid as int) as pk_metodo_envio
        , cast(creditcardid as int) as pk_cartao_credito
        , cast(creditcardapprovalcode as string) as codigo_aprovacao_cartao_credito
        , cast(currencyrateid as int) as pk_taxa_cambio
        , cast(subtotal as decimal) as subtotal
        , cast(taxamt as decimal) as valor_impostos
        , cast(freight as decimal) as custo_frete
        , cast(totaldue as decimal) as total_devido
        
        -- coment -- não será necessário nessa análise
        -- rowguid -- não será necessário nessa análise
        -- modifieddate -- não será necessário nessa análise
     
     
    from {{ source('sap_adw', 'salesorderheader') }}
)

select *
from renamed
