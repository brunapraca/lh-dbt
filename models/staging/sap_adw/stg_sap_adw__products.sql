with
    rennamed as (
        select 
            cast(productid as int) as pk_produto
            , cast(name as string) as nome_produto
            , cast(makeflag as boolean) as produto_fabricado
            , cast(finishedgoodsflag as boolean) as produto_acabado
            , cast(color as string) as cor_produto
            , cast(standardcost as decimal) as custo_padrao_produto
            , cast(listprice as decimal) as preco_venda_sugerido
            , cast(daystomanufacture as int) as dias_producao
            , cast(modifieddate as timestamp) as data_modificacao
            -- productline -- este campo não será necessário para essa análise.
            -- class -- este campo não será necessário para essa análise.
            -- style -- este campo não será necessário para essa análise.
            -- productsubcategoryid -- este campo não será necessário para essa análise.
            -- productmodelid -- este campo não será necessário para essa análise.
            -- productnumber -- este campo não será necessário para essa análise.
            -- eightunitmeasurecode -- este campo não será necessário para essa análise.
            -- size-- este campo não será necessário para essa análise.
            -- SizeUnitMeasureCode -- este campo não será necessário para essa análise.
            -- weight -- este campo não será necessário para essa análise.
            -- safetystocklevel -- este campo não será necessário para essa análise.
            -- reorderpoint-- este campo não será necessário para essa análise.
            -- sellstartdate -- este campo não será necessário para essa análise.
            -- sellenddate -- este campo não será necessário para essa análise.
            -- discontinueddate -- este campo não será necessário para essa análise.
            -- rowguid -- este campo não será necessário para essa análise.
        from {{ source('sap_adw', 'product') }}
    )
select *
from rennamed