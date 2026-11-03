{#
  Reconciliação fato x agregado: os totais de saldo, admissões e desligamentos
  precisam ser idênticos nas duas tabelas. O teste falha se retornar qualquer linha.
#}
with fato as (
    select
        coalesce(sum(saldo_ajustado), 0)        as saldo,
        coalesce(sum(is_admissao_ativa), 0)     as admissoes,
        coalesce(sum(is_desligamento_ativo), 0) as desligamentos
    from {{ ref('fato_movimentacoes_ti') }}
),

agg as (
    select
        coalesce(sum(saldo_liquido), 0)        as saldo,
        coalesce(sum(total_admissoes), 0)      as admissoes,
        coalesce(sum(total_desligamentos), 0)  as desligamentos
    from {{ ref('agg_caged_ti_mensal') }}
)

select
    fato.saldo          as saldo_fato,
    agg.saldo           as saldo_agg,
    fato.admissoes      as admissoes_fato,
    agg.admissoes       as admissoes_agg,
    fato.desligamentos  as desligamentos_fato,
    agg.desligamentos   as desligamentos_agg
from fato
cross join agg
where fato.saldo != agg.saldo
   or fato.admissoes != agg.admissoes
   or fato.desligamentos != agg.desligamentos

