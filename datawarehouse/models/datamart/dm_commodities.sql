

-- import [importar o schema.yml]

with commodities as (
    select
        "data", 
        "abertura", 
        "alta", 
        "baixa", 
        "fechado", 
        "simbolo"
    from
        {{ ref ('stg_commodities') }}
),

movimentacao as (
    select
        data, 
        "simbolo",
        "acao",
        quantidade
    from
        {{ ref ('stg_movimentacao_commodities') }}
),

joined as (
    select
        c.data,
        c.simbolo,
        c.fechado as valor_fechamento,
        m.acao,
        m.quantidade,
        (m.quantidade * c.fechado) as valor,
        case
            when m.acao = 'sell' then (m.quantidade * c.fechado)
            else -(m.quantidade * c.fechado)
        end as ganho
    from
        commodities c
    inner join 
        movimentacao m
    on
        c.data = m.data
        and c.simbolo = m.simbolo
),

last_day as (
    select 
        max(data) as max_date
    from 
        joined
),

filtered as (
    select 
        *
    from
        joined
    where
        data = ( select max_date from last_day )
)

select
    data,
    simbolo,
    valor_fechamento,
    acao,
    quantidade,
    valor,
    ganho
from
    filtered
