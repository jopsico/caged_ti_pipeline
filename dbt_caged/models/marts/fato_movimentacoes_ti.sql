{#
  Tabela Fato de Movimentações em TI (Camada Gold).
  Consolida MOV + FOR + EXC filtrando apenas ocupações presentes na dim_cbo.
#}
with movimentacoes_unificadas as (
    select * from {{ ref('stg_caged_mov') }}
    union all
    select * from {{ ref('stg_caged_for') }}
    union all
    select * from {{ ref('stg_caged_exc') }}
)

select
    m.competencia_mov,
    m.competencia_dec,
    m.competencia_exc,
    m.regiao_codigo,
    m.uf_codigo,
    case
        when m.uf_codigo = '23' then 'Ceará'
        when m.regiao_codigo = '2' then 'Resto do Nordeste'
        else 'Resto do Brasil'
    end                                                                    as recorte_geografico,
    m.municipio_codigo,
    m.cnae_secao,
    m.cnae_subclasse,
    m.cbo_ocupacao,
    d.nome_ocupacao,
    d.familia_cbo,
    d.nome_familia,
    d.nivel_carreira,
    m.grau_instrucao_codigo,
    m.sexo_codigo,
    m.raca_cor_codigo,
    m.idade,
    case
        when m.idade <= 24 then 'Até 24 anos (Jovem)'
        when m.idade between 25 and 34 then '25 a 34 anos'
        when m.idade between 35 and 44 then '35 a 44 anos'
        when m.idade >= 45 then '45 anos ou mais'
        else 'Não informado'
    end                                                                    as faixa_etaria,
    m.horas_contratuais,
    m.unidade_salario_codigo,
    m.salario_bruto,
    m.salario_valido,
    m.valor_salario_fixo,
    m.tipo_movimentacao_codigo,
    case
        when m.indicador_exclusao = 0 and m.tipo_movimentacao_codigo in (10, 25) then 1
        else 0
    end                                                                    as is_primeiro_emprego,
    m.indicador_aprendiz,
    m.ind_trab_intermitente,
    m.ind_trab_parcial,
    m.tamanho_estabelecimento_jan,
    m.tipo_arquivo,
    m.indicador_fora_prazo,
    m.indicador_exclusao,
    m.saldo_movimentacao,
    m.saldo_ajustado,
    case
        when m.indicador_exclusao = 0 and m.saldo_movimentacao = 1 then 1
        else 0
    end                                                                    as is_admissao_ativa,
    case
        when m.indicador_exclusao = 0 and m.saldo_movimentacao = -1 then 1
        else 0
    end                                                                    as is_desligamento_ativo,
    m._ingested_at
from movimentacoes_unificadas m
inner join {{ ref('dim_cbo') }} d
    on m.cbo_ocupacao = d.cbo_ocupacao