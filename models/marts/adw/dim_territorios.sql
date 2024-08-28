with 
    stg_territorio as (
        select *
        from {{ref('stg_sap_adw__salesterritory')}}
    )

    , stg_cidade as (
        select *
        from {{ref('stg_sap_adw__addresses')}}
    )

    , stg_provincia as (
        select *
        from {{ref('stg_sap_adw__stateprovince')}}
    )

    , stg_pais as (
        select *
        from {{ref('stg_sap_adw__countryregions')}}
    )

    , stg_vendas as (
        select * 
        from {{ ref('stg_sap_adw__salesorderheaders') }}
    )

    , transformacao as (
        select
            concat(cast(pk_pedido as string), '-', cast(pk_cidade as string)
                , '-' , cast(pk_territorio as string))
                as sk_regiao
            , stg_vendas.pk_pedido
            , stg_territorio.pk_territorio
            , stg_territorio.nome_territorio
            , stg_territorio.total_vendas_territorio
            , stg_pais.nome_pais
            , stg_provincia.code_provincia
            , stg_cidade.cidade
        from stg_territorio
        left join stg_provincia 
            on stg_territorio.pk_territorio = stg_provincia.fk_territorio
        left join stg_vendas
            on stg_vendas.fk_territorio = stg_territorio.pk_territorio
        left join stg_cidade 
            on stg_provincia.pk_provincia = stg_cidade.fk_provincia
        left join stg_pais 
            on stg_provincia.fk_code_regiao = stg_pais.pk_pais

    )
select *
from transformacao
