with
    vendas as (
        select *
        from {{ ref('int_sales') }}
    )
    
    , dim_produtos as (
        select *
        from {{ ref('dim_products') }}
    )

    , dim_clientes as (
        select *
        from {{ ref('dim_customers') }}
    )

    , dim_promocoes as (
        select *
        from {{ ref('dim_promotions') }}
    )

    , dim_datas as (
        select *
        from {{ ref('dim_dates') }}
    )

    , dim_territorio as (
        select *
        from {{ ref('dim_territories') }}
    )

    , joined as (
        select 
        vendas.sk_vendas
        , pk_pedido
        , vendas.data_pedido
        , vendas.data_envio
        , vendas.status_pedido
        , vendas.nome_status_pedido
        , vendas.flag_pedido_online
        , vendas.quantidade_comprada 
        , vendas.preco_unitario
        , vendas.valor_com_desconto
        , vendas.tempo_frete
        , dim_datas.date_key
        , dim_datas.ano
        , dim_datas.mes
        , dim_datas.dia
        , dim_datas.dia_da_semana
        , dim_datas.trimestre
        , dim_datas.data_string
        , dim_datas.day_type
        , dim_produtos.pk_produto
        , dim_produtos.nome_produto
        , dim_produtos.fabricado as produto_fabricado
        , dim_produtos.acabado as produto_acabado
        , dim_produtos.cor as cor_produto
        , dim_produtos.custo_padrao as custo_padrao_produto
        , dim_produtos.preco_sugerido as preco_sugerido_venda
        , dim_produtos.dias_producao as dias_producao_produto
        , dim_promocoes.pk_promocao
        , dim_promocoes.descricao_promocao
        , dim_promocoes.percentual_desconto
        , dim_promocoes.tipo_promocao
        , dim_promocoes.categoria_promocao
        , dim_promocoes.data_inicio
        , dim_promocoes.data_fim
        , dim_promocoes.duracao_promocao
        , dim_promocoes.nome_produto as nome_produto_promocao
        , dim_territorio.pk_territorio
        , dim_territorio.nome_territorio
        , dim_territorio.total_vendas_territorio
        , dim_territorio.nome_pais
        , dim_territorio.code_provincia
        , dim_territorio.cidade as nome_cidade
        , dim_clientes.pk_cliente
        , dim_clientes.nome_territorio as endereco_cliente
        , dim_clientes.nome_entidade as nome_cliente
        , dim_clientes.email_cliente
        , dim_clientes.nome_motivo_venda
        , dim_clientes.tipo_motivo_venda
        , dim_clientes.usou_cartao_credito
        from vendas
        left join dim_clientes
            on vendas.fk_cliente = dim_clientes.pk_cliente
        left join dim_produtos
            on vendas.fk_produto = dim_produtos.pk_produto 
        left join dim_promocoes
            on vendas.fk_promocao = dim_promocoes.pk_promocao 
        left join dim_datas
            on vendas.data_pedido = dim_datas.date_key 
        left join dim_territorio
            on vendas.fk_territorio = dim_territorio.pk_territorio
    )
select * 
from joined