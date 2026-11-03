{#
  Reconciliação fato x agregado no grão (competencia_mov, recorte_geografico, nivel_carreira).
  FULL OUTER JOIN expõe grupos ausentes em qualquer lado, e a comparação por grupo impede que
  divergências regionais se compensem num total global. O teste falha se retornar qualquer linha.
#}
with fato as (
    select
        competencia_mov,
        recorte_geografico,
        nivel_carreira,
        sum(saldo_ajustado)                                                    as saldo,
        sum(is_admissao_ativa)                                                 as admissoes,
        sum(is_desligamento_ativo)                                             as desligamentos,
        sum(case when is_admissao_ativa = 1 and idade <= 24 then 1 else 0 end) as jovens_ate_24
    from {{ ref('fato_movimentacoes_ti') }}
    group by
        competencia_mov,
        recorte_geografico,
        nivel_carreira
),

agg as (
    select
        competencia_mov,
        recorte_geografico,
        nivel_carreira,
        saldo_liquido            as saldo,
        total_admissoes          as admissoes,
        total_desligamentos      as desligamentos,
        admissoes_jovens_ate_24  as jovens_ate_24
    from {{ ref('agg_caged_ti_mensal') }}
)

select
    coalesce(f.competencia_mov, a.competencia_mov)       as competencia_mov,
    coalesce(f.recorte_geografico, a.recorte_geografico) as recorte_geografico,
    coalesce(f.nivel_carreira, a.nivel_carreira)         as nivel_carreira,
    f.saldo           as saldo_fato,
    a.saldo           as saldo_agg,
    f.admissoes       as admissoes_fato,
    a.admissoes       as admissoes_agg,
    f.desligamentos   as desligamentos_fato,
    a.desligamentos   as desligamentos_agg,
    f.jovens_ate_24   as jovens_ate_24_fato,
    a.jovens_ate_24   as jovens_ate_24_agg
from fato f
full outer join agg a
    on  f.competencia_mov is not distinct from a.competencia_mov
    and f.recorte_geografico is not distinct from a.recorte_geografico
    and f.nivel_carreira is not distinct from a.nivel_carreira
where f.saldo is distinct from a.saldo
   or f.admissoes is distinct from a.admissoes
   or f.desligamentos is distinct from a.desligamentos
   or f.jovens_ate_24 is distinct from a.jovens_ate_24
