with
    renamed as (
        select 
            cast(salesorderid as int) as pk_pedido
            , cast(customerid as int) as fk_cliente
            , cast(salespersonid as int) as fk_vendedor
            , cast(territoryid as int) as fk_territorio
            , cast(creditcardid as int) as fk_cartao_credito
            , cast(orderdate as timestamp) as data_pedido
            , cast(duedate as timestamp) as data_vencimento
            , cast(shipdate as timestamp) as data_envio
            , cast(status as int) as status_pedido
            , cast(onlineorderflag as boolean) as flag_pedido_online
            -- taxamt -- não será necessário nessa análise
            -- freight -- não será necessário nessa análise
            -- totaldue -- não será necessário nessa análise
            -- subtotal-- não será necessário nessa análise
            -- currencyrateid-- não será necessário nessa análise
            -- creditcardapprovalcode-- não será necessário nessa análise
            -- shipmethodid-- não será necessário nessa análise
            -- shiptoaddressid -- não será necessário nessa análise
            -- billtoaddressid-- não será necessário nessa análise
            -- accountnumber-- não será necessário nessa análise
            -- purchaseordernumber-- não será necessário nessa análise
            -- revisionnumbe-- não será necessário nessa análise
            -- coment -- não será necessário nessa análise
            -- rowguid -- não será necessário nessa análise
            -- modifieddate -- não será necessário nessa análise     
        from {{ source('sap_adw', 'salesorderheader') }}
    )
select *
from renamed
