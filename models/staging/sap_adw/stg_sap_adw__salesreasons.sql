with 
    rennamed as (
        select 
            cast(salesreasonid as int) as pk_motivo_venda
            , cast(name as string) as nome_motivo_venda
            , cast(reasontype as string) as tipo_motivo_venda

            -- modifieddate -- este campo não será necessário para essa análise.
        from {{ source('sap_adw', 'salesreason') }}
    )

    select *
    from rennamed
