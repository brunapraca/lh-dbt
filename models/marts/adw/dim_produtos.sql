with 
    
    produto as (
        select *
        from {{ ref('stg_sap_adw__products') }}
    )
    
    , subcategoria as (
        select *
        from {{ ref('stg_sap_adw__productsubcategory') }}
    )
    
    , categoria as (
        select *
        from {{ ref('stg_sap_adw__productcategory') }}
    )

    , estoque as (
        select 
            pk_produto
            , sum(quantidade_estoque) as quantidade_estoque
        from {{ ref('stg_sap_adw__productInventory') }}
        group by pk_produto
    )

    , joined as (
        select
            {{ dbt_utils.generate_surrogate_key(
                    ['produto.pk_produto']
                )
            }} as produto_sk
            , produto.pk_produto
            , produto.nome_produto
            , produto.produto_fabricado
            , produto.produto_acabado
            , produto.cor_produto
            , produto.custo_padrao_produto
            , produto.preco_venda_sugerido
            , produto.dias_producao
            , produto.data_diponivel_venda
            , produto.data_indisponivel_venda
            , categoria.nome_produto_categoria
            , subcategoria.nome_subcategoria
            , estoque.quantidade_estoque

        from produto 
        left join subcategoria
            on subcategoria.subcategoria_pk = produto.fk_subcategoria
        left join categoria
            on categoria.pk_produto_categoria = subcategoria.fk_produto_categoria
        left join estoque
            on produto.pk_produto = estoque.pk_produto    
    )

select * 
from joined


