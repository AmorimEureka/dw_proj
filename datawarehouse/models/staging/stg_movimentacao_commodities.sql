

-- import [importar o schema.yml]

with source as (
    select
        "date", 
        "symbol",
        "action", 
        "quantity"
    from
        {{ source ('dw_proj', 'movimentacao_commodities') }}
),


-- renamed [renomear, definir os tipos]

renamed as (
    select
        cast(date as date) as data,
        "symbol" as simbolo,
        "action" as acao, 
        "quantity" as quantidade
    from
        source
)


-- select's final
    select 
        data,
        simbolo,
        acao,
        quantidade
    from 
        renamed