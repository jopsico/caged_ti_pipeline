{% macro stg_caged_base(source_table_name, tipo_arquivo) %}

with raw_source as (
    select * from {{ source('bronze', source_table_name) }}
),

padronizado as (
    select
        cast("competênciamov" as varchar)                                      as competencia_mov,
        cast("competênciadec" as varchar)                                      as competencia_dec,
        {% if tipo_arquivo == 'EXC' %}
        cast("competênciaexc" as varchar)                                      as competencia_exc,
        coalesce(try_cast("indicadordeexclusão" as integer), 1)                as indicador_exclusao,
        {% else %}
        cast(null as varchar)                                                  as competencia_exc,
        0                                                                      as indicador_exclusao,
        {% endif %}
        cast("região" as varchar)                                              as regiao_codigo,
        cast("uf" as varchar)                                                  as uf_codigo,
        cast("município" as varchar)                                           as municipio_codigo,
        cast("seção" as varchar)                                               as cnae_secao,
        cast("subclasse" as varchar)                                           as cnae_subclasse,
        cast("cbo2002ocupação" as varchar)                                     as cbo_ocupacao,
        try_cast("categoria" as integer)                                       as categoria_codigo,
        try_cast("graudeinstrução" as integer)                                 as grau_instrucao_codigo,
        try_cast("idade" as integer)                                           as idade,
        try_cast(replace("horascontratuais", ',', '.') as numeric(6,2))        as horas_contratuais,
        try_cast("raçacor" as integer)                                         as raca_cor_codigo,
        try_cast("sexo" as integer)                                            as sexo_codigo,
        try_cast("tipoempregador" as integer)                                  as tipo_empregador_codigo,
        try_cast("tipoestabelecimento" as integer)                             as tipo_estabelecimento_codigo,
        try_cast("tipomovimentação" as integer)                                as tipo_movimentacao_codigo,
        try_cast("tipodedeficiência" as integer)                               as tipo_deficiencia_codigo,
        try_cast("indtrabintermitente" as integer)                             as ind_trab_intermitente,
        try_cast("indtrabparcial" as integer)                                  as ind_trab_parcial,
        try_cast("tamestabjan" as integer)                                     as tamanho_estabelecimento_jan,
        try_cast("indicadoraprendiz" as integer)                               as indicador_aprendiz,
        try_cast("origemdainformação" as integer)                              as origem_informacao_codigo,
        try_cast("indicadordeforadoprazo" as integer)                          as indicador_fora_prazo,
        try_cast("unidadesaláriocódigo" as integer)                            as unidade_salario_codigo,
        try_cast(replace("salário", ',', '.') as numeric(14,2))                as salario_bruto,
        try_cast(replace("valorsaláriofixo", ',', '.') as numeric(14,2))       as valor_salario_fixo,
        try_cast("saldomovimentação" as integer)                               as saldo_movimentacao,
        '{{ tipo_arquivo }}'                                                   as tipo_arquivo,
        _competencia_arquivo,
        _arquivo_origem,
        _ingested_at
    from raw_source
)

select
    *,
    -- Regra oficial do Novo CAGED: exclusões invertem o sinal do saldo
    case
        when indicador_exclusao = 1 then saldo_movimentacao * -1
        else saldo_movimentacao
    end as saldo_ajustado,
    -- Saneamento de outliers salariais (mantém a linha para o saldo, mas anula salários irreais)
    case
        when salario_bruto between 700.00 and 100000.00 then salario_bruto
        else null
    end as salario_valido
from padronizado

{% endmacro %}