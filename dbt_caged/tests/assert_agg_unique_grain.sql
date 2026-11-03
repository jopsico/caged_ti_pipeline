{#
  Garante o grão do mart agregado: uma linha por (competencia_mov, recorte_geografico, nivel_carreira).
  O teste falha se retornar qualquer linha.
#}
select
    competencia_mov,
    recorte_geografico,
    nivel_carreira,
    count(*) as qtd_linhas
from {{ ref('agg_caged_ti_mensal') }}
group by
    competencia_mov,
    recorte_geografico,
    nivel_carreira
having count(*) > 1

