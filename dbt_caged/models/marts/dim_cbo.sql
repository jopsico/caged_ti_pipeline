select
    cast(cbo_ocupacao as varchar)  as cbo_ocupacao,
    nome_ocupacao,
    cast(familia_cbo as varchar)   as familia_cbo,
    nome_familia,
    nivel_carreira
from {{ ref('seed_cbo_ti') }}