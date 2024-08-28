with 
    rennamed as (
        select 
            cast(salesreasonid as int) as pk_motivo_venda
            , cast(name as string) as nome_motivo_venda
            , cast(reasontype as string) as tipo_motivo_venda
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('sap_adw', 'salesreason') }}
    )
    select *
    from rennamed
