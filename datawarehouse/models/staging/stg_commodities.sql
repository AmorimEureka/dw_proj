-- import [importar o schema.yml]

with source as (
    select
        "data", 
        "abertura", 
        "alta", 
        "baixa", 
        "fechado", 
        "simbolo"
    from
        {{ source ('dw_proj', 'commodities') }}
),


-- renamed [renomear, definir os tipos]

renamed as (
    select
        cast(data as date),
        "abertura", 
        "alta", 
        "baixa", 
        "fechado", 
        "simbolo"
    from
        source
)


-- select's final
    select * from renamed