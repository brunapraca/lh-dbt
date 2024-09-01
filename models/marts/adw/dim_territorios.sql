with 
    stg_vendas as (
        select 
            distinct(fk_endereco)
        from {{ ref('stg_sap_adw__salesorderheaders') }}
    )

    , stg_cidade as (
        select *
        from {{ref('stg_sap_adw__addresses')}}
    )

    , stg_estado as (
        select 
            pk_provincia
            , nome_estado
            , code_provincia
        from {{ref('stg_sap_adw__stateprovince')}}
    )

    , stg_pais as (
        select *
        from {{ref('stg_sap_adw__countryregions')}}
    )

    , transformacao as (
         select
             {{ dbt_utils.generate_surrogate_key(
            ['stg_vendas.fk_endereco']
            )
            }} as endereco_sk 
        , stg_vendas.fk_endereco 
        , stg_cidade.cidade as nome_cidade
        , stg_estado.nome_estado
        , stg_pais.nome_pais
        from stg_vendas
        left join stg_cidade
            on stg_vendas.fk_endereco = stg_cidade.pk_cidade
        left join 	stg_estado 
            on stg_cidade.fk_provincia = stg_estado.pk_provincia
        left join stg_pais
            on stg_estado.code_provincia = stg_pais.pk_pais
    )
select *
from transformacao
