with 
    date_range as (
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
        from unnest(
            generate_date_array(
                date('2000-01-01'), 
                date('2030-12-31'), 
                interval 1 day
            )
        ) as date_value
    )
    
    , last_update as (
        select
            max(data_pedido) as ultima_atualizacao
        from {{ ref('stg_sap_adw__salesorderheaders') }}
        where data_pedido is not null
    )

    , enriched_date_range as (
        select
            date_key
            , ano
            , mes
            , dia
            , dia_da_semana
            , trimestre
            , data_string
            , day_type
            , (select ultima_atualizacao from last_update) as data_ultima_atualizacao
        from date_range
    )

select *
from enriched_date_range


