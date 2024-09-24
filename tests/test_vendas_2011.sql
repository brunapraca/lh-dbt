 /* Teste de vendas brutas no ano de 2011 - Auditado por Carlos 
 Total = $12.646.112,16 */
 
{{ config (
        severity = 'error'
    )
}}
 
 with


    vendas_2011 as (
        select 
            SUM(preco_unitario * quantidade_comprada) as vendas_brutas
        from {{ ref('int_sales') }}
        where data_pedido between '2011-01-01' and '2011-12-31'
        --$12.646.112,16 
    )
select *
from vendas_2011

