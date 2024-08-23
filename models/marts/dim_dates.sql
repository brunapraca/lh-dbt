with 
    date_range as (
        select distinct
            data_pedido as date_value
        from {{ ref('stg_sap_adw__salesorderheaders') }}
        where data_pedido is not null
    )

select
    date_value as date_key 
    , extract(year from date_value) as ano
    , extract(month from date_value) as mes
    , extract(day from date_value) as dia
    , extract(dayofweek from date_value) as dia_da_semana 
    , extract(quarter from date_value) as trimestre
    , format_date('%Y-%m-%d', date_value) as data_string
    , case
        when extract(dayofweek from date_value) in (1, 7) then 'weekend'
        else 'weekday'
    end as day_type

from date_range

