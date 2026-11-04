{#
  Invariantes da regra de exclusão e do saldo (valem em qualquer extração):
    1. Exclusão nunca conta como admissão ou desligamento ativo.
    2. saldo_ajustado = saldo_movimentacao, com sinal invertido apenas em exclusões.
    3. Registros não excluídos são exatamente uma admissão ativa OU um desligamento ativo.
  O teste falha se retornar qualquer linha.
#}
select
    tipo_arquivo,
    competencia_mov,
    indicador_exclusao,
    saldo_movimentacao,
    saldo_ajustado,
    is_admissao_ativa,
    is_desligamento_ativo
from {{ ref('fato_movimentacoes_ti') }}
where (indicador_exclusao = 1 and (is_admissao_ativa = 1 or is_desligamento_ativo = 1))
   or saldo_ajustado != case when indicador_exclusao = 1 then -saldo_movimentacao else saldo_movimentacao end
   or (indicador_exclusao = 0 and is_admissao_ativa + is_desligamento_ativo != 1)

