{#
  Mart Agregado Mensal (Camada Gold)
  Consolidado no grão exato das hipóteses (Competência x Recorte Geográfico x Nível de Carreira)
  para garantir cálculo exato das medianas salariais (que não são aditivas).
#}
select
    competencia_mov,
    recorte_geografico,
    nivel_carreira,

    -- Métricas de Volume e Saldo (H3)
    sum(is_admissao_ativa)                                                 as total_admissoes,
    sum(is_desligamento_ativo)                                             as total_desligamentos,
    sum(saldo_ajustado)                                                    as saldo_liquido,

    -- Métricas de Porta de Entrada e Júnior (H2)
    sum(case when is_admissao_ativa = 1 and idade <= 24 then 1 else 0 end) as admissoes_jovens_ate_24,
    round(
        100.0 * sum(case when is_admissao_ativa = 1 and idade <= 24 then 1 else 0 end)
        / nullif(sum(is_admissao_ativa), 0),
        2
    )                                                                      as pct_admissoes_jovens_ate_24,

    -- Métricas Salariais Exatas: Admitidos vs Desligados (H1)
    round(avg(case when is_admissao_ativa = 1 then salario_valido end), 2)          as media_salario_admitidos,
    round(avg(case when is_desligamento_ativo = 1 then salario_valido end), 2)      as media_salario_desligados,
    round(median(case when is_admissao_ativa = 1 then salario_valido end), 2)       as mediana_salario_admitidos,
    round(median(case when is_desligamento_ativo = 1 then salario_valido end), 2)   as mediana_salario_desligados,
    round(
        median(case when is_admissao_ativa = 1 then salario_valido end)
        - median(case when is_desligamento_ativo = 1 then salario_valido end),
        2
    )                                                                               as diff_mediana_admitido_desligado

from {{ ref('fato_movimentacoes_ti') }}
group by
    competencia_mov,
    recorte_geografico,
    nivel_carreira