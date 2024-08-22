with 
    stg_promocao as (
        select *
        from {{ ref('stg_sap_adw__specialoffer') }}
    )

    , stg_promocao_produto as (
        select *
        from {{ ref('stg_sap_adw__specialofferproduct') }}
    )

    , stg_produto as (
        select *
        from {{ ref('stg_sap_adw__products') }}
    )

    , transformacao as (
        select 
            row_number() over (order by stg_promocao.pk_promocao) as promocao_sk --criação de uma chave surrogate
            , stg_promocao.pk_promocao
            , stg_promocao.descricao_promocao
            , stg_promocao.percentual_desconto
            , stg_promocao.tipo_promocao
            , stg_promocao.categoria_promocao
            , stg_promocao.data_inicio
            , stg_promocao.data_fim
            , stg_produto.pk_produto
            , stg_produto.nome_produto
        from stg_promocao 
        left join stg_promocao_produto on stg_promocao.pk_promocao = stg_promocao_produto.pk_promocao
        left join stg_produto on stg_promocao_produto.fk_produto = stg_produto.pk_produto
    )
select * 
from transformacao
