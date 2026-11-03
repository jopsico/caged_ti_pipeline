# Pipeline de Dados e Dinâmica do Emprego Formal de TI no Brasil (Novo CAGED)

![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)
![dbt-duckdb](https://img.shields.io/badge/dbt--duckdb-FF694B?logo=dbt&logoColor=white)
![Parquet](https://img.shields.io/badge/Apache%20Parquet-ZSTD-50ABF1?logo=apacheparquet&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI-2088FF?logo=githubactions&logoColor=white)

## Resumo Executivo (TL;DR)

- **Problema:** o Novo CAGED publica microdados brutos, em `.7z`, com schemas divergentes entre arquivos e variáveis degradadas pós-eSocial, o que dificulta medir a dinâmica salarial e de entrada no emprego formal de TI.
- **Solução:** pipeline *Medallion* local (FTP → Parquet → dbt/DuckDB → Streamlit), com ingestão idempotente e atômica, regras contábeis de exclusão, saneamento de outliers e testes de grão, reconciliação e `pytest` com dados sintéticos.
- **Principal achado:** em Técnico e Superior / Analista, quem entra tem mediana salarial inferior à de quem sai em todos os recortes, e a base Técnica concentra a absorção de jovens (≤ 24 anos: 42,7% das admissões no Ceará e 49,6% no Resto do Nordeste).

![Dashboard](docs/dashboard.png)

Pipeline de Engenharia de Dados (*Modern Data Stack in a Box*) para extração, tratamento, modelagem dimensional e análise das movimentações formais (CLT) em ocupações de Tecnologia da Informação no Brasil, com recorte comparativo para o **Ceará e Região Nordeste**, a partir dos microdados brutos do **Novo CAGED (MTE)**.

## Sumário

1. [Arquitetura (Medallion)](#arquitetura-medallion)
2. [Decisões de Engenharia e Tratamento de Dados](#decisões-de-engenharia-e-tratamento-de-dados-data-quality)
3. [Modelagem Dimensional e Testes de Qualidade (dbt)](#modelagem-dimensional-e-testes-de-qualidade-dbt)
4. [Limitações Conhecidas dos Dados](#limitações-conhecidas-dos-dados)
5. [Perguntas de Negócio e Hipóteses Testáveis](#perguntas-de-negócio-e-hipóteses-testáveis)
6. [Resultados e Conclusões Analíticas](#resultados-e-conclusões-analíticas-safra-2025-a-2026)
7. [Estrutura do Repositório](#estrutura-do-repositório)
8. [Como Executar](#como-executar)
9. [Stack Tecnológica](#stack-tecnológica)

---

## Arquitetura (Medallion)

O pipeline foi desenhado para operar localmente com baixo consumo de disco e alta performance colunar, eliminando arquivos brutos intermediários após a conversão atômica para Parquet:

```text
FTP MTE (.7z) ──► data/tmp (.txt) ──► Bronze (Parquet ZSTD) ──► Silver (dbt views) ──► Gold (dbt tables) ──► Data Serving (Streamlit / src/app.py)
              └─── extract_ftp.py ───┘                      └─── stg_caged_* ───┘   └─── marts (dim/fct) ───┘
```

| Camada | Ferramenta | Armazenamento | Responsabilidade Técnica |
|---|---|---|---|
| **Ingestão & Bronze** | Python (`ftplib`, `py7zr`, `duckdb`) | `data/bronze/caged_<tipo>/competencia=<AAAAMM>/data.parquet` | Download idempotente em modo passivo, descompactação em diretório temporário (`data/tmp`), leitura *schema-on-read* (`all_varchar=True`, UTF-8), adição de metadados de linhagem e escrita atômica em Parquet (ZSTD). |
| **Silver (Staging)** | dbt + DuckDB | Schema `silver` (`data/caged.duckdb`) | Padronização de nomenclatura (snake_case sem acentos), unificação de schemas divergentes (MOV/FOR vs. EXC), tipagem explícita, tratamento de outliers salariais e inversão de sinal de exclusões. |
| **Gold (Marts)** | dbt + DuckDB | Schema `gold` (`data/caged.duckdb`) | Modelagem dimensional (Tabelas Fato e Dimensões), filtro de domínio via seed oficial da CBO 2002 e agregações (contagens, saldo e medianas salariais) no grão **Competência × Recorte Geográfico (Ceará / Resto do Nordeste / Resto do Brasil) × Nível de Carreira (Gestão / Superior-Analista / Técnico)**. |
| **Data Serving / BI** | Streamlit + Plotly | Leitura *read-only* (`data/caged.duckdb`) | Dashboard interativo ([`src/app.py`](src/app.py)) que consome as tabelas do schema `gold` em modo `read_only=True`, permitindo visualização sem risco de *lock* de escrita no banco. |

---

## Decisões de Engenharia e Tratamento de Dados (Data Quality)

A inspeção exploratória dos microdados do Novo CAGED e da documentação técnica do PDET/MTE determinou as seguintes regras implementadas no pipeline:

### 1. Divergência de Schema (Schema Drift entre Arquivos)
Os arquivos `CAGEDMOV` (dentro do prazo) e `CAGEDFOR` (fora do prazo) possuem **28 colunas**, enquanto `CAGEDEXC` (exclusões) possui **30 colunas** (adicionando `competênciaexc` e `indicadordeexclusão`). A unificação na camada Silver utiliza padronização explícita de colunas para evitar quebra posicional.

- **Escrita Atômica, Resiliência e Linhagem na Bronze** ([`extract_ftp.py`](src/extract_ftp.py)):
  - **Escrita atômica:** o Parquet é gravado primeiro como `data.parquet.part` e só então renomeado atomicamente para `data.parquet`, evitando arquivos corrompidos caso o script seja interrompido no meio.
  - **Resiliência de rede:** até 3 tentativas por arquivo com *backoff* progressivo (5 s, 10 s) e reconexão automática ao FTP do MTE; modo binário garantido (`TYPE I`) e validação do tamanho baixado contra o `SIZE` do servidor.
  - **Linhagem:** injeção das colunas de auditoria `_competencia_arquivo`, `_tipo_arquivo`, `_arquivo_origem` e `_ingested_at` em cada registro.

### 2. Regra Contábil de Exclusões e Competência do Evento
- Conforme documentação oficial do Novo CAGED (`Leia-me.txt`), registros do arquivo EXC mantêm o sinal original do evento cancelado (`1` para admissão cancelada, `-1` para desligamento cancelado). Na camada Silver, o saldo efetivo é ajustado multiplicando `saldomovimentação` por `-1` quando `indicadordeexclusão = 1`.
- Todas as séries históricas são consolidadas pela **competência da movimentação** (`competênciamov`), e não pela competência da declaração (`competênciadec`), alocando declarações atrasadas (FOR) e exclusões (EXC) no mês em que o vínculo realmente ocorreu.
- **Isolamento de Eventos Ativos vs. Exclusões:** uma exclusão de desligamento gera saldo `+1`, mas **não é uma contratação nova**. Por isso, a fato expõe as flags `is_admissao_ativa` e `is_desligamento_ativo` (válidas apenas quando `indicador_exclusao = 0`), garantindo que cancelamentos afetem somente o saldo e não distorçam contagens nem o cálculo salarial de quem foi de fato admitido ou desligado.

### 3. Saneamento de Salários e Outliers do eSocial
- O campo `valorsaláriofixo` reflete a unidade de pagamento (`unidadesaláriocódigo`), trazendo valores por hora para horistas (ex.: R$ 10,49/h), enquanto a coluna `salário` traz o valor mensalizado.
- Devido a erros de preenchimento na origem (foram identificados registros de TI com `salário = 0` e valores de até R$ 1.382.700,00/mês), o pipeline **não descarta a linha** (preservando a contagem oficial de admissões/desligamentos), mas cria a métrica tratada `salario_valido`, anulando (`NULL`) valores abaixo de R$ 700,00 ou acima de R$ 100.000,00 para não distorcer médias e medianas na camada Gold.

### 4. Curadoria do Escopo de TI ([`seed_cbo_ti.csv`](dbt_caged/seeds/seed_cbo_ti.csv))
- O filtro de TI baseia-se nas famílias oficiais da CBO 2002: **1425** (Gerentes de TI), **2122** (Engenheiros em Computação), **2123** (Administradores de TI), **2124** (Analistas de TI), **3171** (Técnicos de Desenvolvimento) e **3172** (Técnicos de Operação/Suporte).
- **Expurgo de Falso Positivo:** o código `317115` ("Programador de máquinas - ferramenta com comando numérico"), embora pertença à família 3171, refere-se à operação de tornos CNC na indústria mecânica e foi removido da seed para não contaminar os indicadores de TI.

### 5. Ausência de Deduplicação Cega
Como os microdados não são identificados (sem CPF/CNPJ), múltiplas contratações idênticas na mesma empresa, cargo, município e faixa salarial geram linhas idênticas legítimas. O pipeline **não aplica `DISTINCT`** sobre os microdados.

### 6. Não-Aditividade da Mediana
A mediana **não é uma métrica aditiva**: calcular a média de medianas (ou reagregar medianas de UFs para regiões) gera distorção. Por isso, o mart [`agg_caged_ti_mensal`](dbt_caged/models/marts/agg_caged_ti_mensal.sql) calcula as medianas diretamente a partir do grão individual de `fato_movimentacoes_ti`, já no recorte final de consumo.

---

## Modelagem Dimensional e Testes de Qualidade (dbt)

Objetos materializados no banco `data/caged.duckdb` e testes de qualidade executados via `dbt test` / `dbt build` (**21 testes de dados**, incluindo os testes singulares de grão e reconciliação):

| Objeto | Materialização | Descrição | Testes de Qualidade |
| :--- | :---: | :--- | :--- |
| `silver.stg_caged_mov` | View | Leitura dos Parquets da Bronze (MOV), tipagem e limpeza. | — |
| `silver.stg_caged_for` | View | Leitura dos Parquets da Bronze (FOR), tipagem e limpeza. | — |
| `silver.stg_caged_exc` | View | Leitura dos Parquets da Bronze (EXC), tipagem, limpeza e inversão de sinal. | — |
| `gold.dim_cbo` | Table | 25 ocupações de TI classificadas em 3 níveis de carreira (seed CBO 2002). | `unique` em `cbo_ocupacao`; `not_null` em `cbo_ocupacao`, `nome_ocupacao` e `familia_cbo` |
| `gold.fato_movimentacoes_ti` | Table | Movimentações consolidadas (MOV + FOR + EXC) no grão individual. | `not_null` em `cbo_ocupacao`, `competencia_mov`, `uf_codigo` e `saldo_ajustado`; `relationships` (`cbo_ocupacao` → `dim_cbo`); `accepted_values` `[-1, 1]` em `saldo_ajustado` |
| `gold.agg_caged_ti_mensal` | Table | Mart pré-agregado mensal (Competência × Recorte × Carreira) para consumo no Streamlit. | `not_null` nas chaves (`competencia_mov`, `recorte_geografico`, `nivel_carreira`) e nas métricas aditivas (`total_admissoes`, `total_desligamentos`, `saldo_liquido`, `admissoes_primeiro_emprego`, `admissoes_jovens_ate_24`); `accepted_values` em `recorte_geografico` |
| `assert_agg_unique_grain` | Teste singular | Garante uma única linha por (`competencia_mov`, `recorte_geografico`, `nivel_carreira`) no agregado. | Falha se retornar qualquer linha |
| `assert_reconciliacao_saldo_fato_vs_agg` | Teste singular | Reconcilia a soma de saldo, admissões e desligamentos entre a fato e o agregado. | Falha se qualquer total divergir |
| `tests/test_pipeline.py` | Suíte `pytest` | Gera Bronze sintética (MOV/FOR/EXC) e executa `dbt build` isolado. Valida inversão de sinal da exclusão, `salario_valido` nulo em outlier (mantendo o saldo), classificação geográfica, reconciliação fato × agregado, escrita atômica e idempotência de `extract_ftp.py`. | 7 testes; executados no CI (`python -m pytest -q`) |

---

## Limitações Conhecidas dos Dados

- **Recorte apenas CLT:** o Novo CAGED capta exclusivamente vínculos celetistas formais; contratações via PJ, cooperativas, estágios ou trabalho informal não constam na base.
- **Fluxo de Contratações vs. Saturação:** os dados medem vínculos efetivados e encerrados, não o número de vagas abertas nem a quantidade de candidatos por vaga.
- **Qualidade de Variáveis Desagregadas:** conforme a Nota Técnica MTPS 088/12, variáveis como Subclasse CNAE, Ocupação (CBO) e Raça/Cor podem apresentar inconsistências em níveis muito desagregados, sendo mais robustas em análises agregadas.

---

## Perguntas de Negócio e Hipóteses Testáveis

Antes da análise na camada Gold, foram registradas as seguintes hipóteses com seus critérios objetivos de refutação:

**H1 — Substituição / Barateamento da Mão de Obra**
As empresas estão demitindo profissionais com salários mais altos e contratando substitutos com salários menores nas mesmas ocupações de TI.
- *Métrica:* razão entre a mediana do `salario_valido` dos admitidos vs. desligados por competência, recorte geográfico e nível de carreira.
- *Critério de refutação:* se a mediana salarial dos admitidos for igual ou superior à dos desligados (descontada a inflação/tendência) na maioria das competências.

**H2 — Porta de Entrada e Primeiro Emprego**
A absorção de profissionais iniciantes (≤ 24 anos) no mercado formal de TI concentra-se majoritariamente na base técnica, enquanto o indicador oficial de 1º emprego (`tipomovimentação = 10`) sofreu degradação estrutural pós-eSocial.
- *Métrica:* participação percentual (%) de admitidos com ≤ 24 anos no total de admissões de cada nível de carreira (Gestão, Superior/Analista, Técnico) e recorte geográfico; proporção de admissões com código `10` frente aos códigos `20` e `97`.
- *Critério de refutação:* se a participação de jovens no nível Técnico for igual ou inferior à dos níveis Superior/Analista e Gestão, ou se o código `10` apresentar volume compatível com a proporção de jovens admitidos.

**H3 — Dinâmica Regional (Ceará e Nordeste vs. Brasil)**
O mercado formal de TI no Ceará apresenta menor volatilidade no saldo relativo, porém com gap salarial persistente em relação ao eixo Sul-Sudeste.
- *Métrica:* saldo líquido acumulado e mediana salarial de admissão em `UF = 23` (Ceará) comparados à média do Nordeste e do Brasil.

---

## Resultados e Conclusões Analíticas (Safra 2025 a 2026)

Após o processamento de mais de **410 mil movimentações formais de TI** na camada Gold (`gold.fato_movimentacoes_ti`), a consolidação estatística por recorte geográfico e nível de carreira revelou o seguinte cenário:

| Recorte Geográfico | Nível de Carreira | Admissões | Desligamentos | Saldo Líquido | % Jovens (≤ 24 anos) | Mediana Salarial (Entrou) | Mediana Salarial (Saiu) | Gap Entrada vs. Saída |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Ceará** | Gestão | 465 | 555 | -90 | 9,2% | R$ 8.000,00 | R$ 7.361,20 | +8,7% |
| **Ceará** | Superior / Analista | 4.071 | 3.868 | +208 | 24,9% | R$ 4.086,17 | R$ 4.102,26 | -0,4% |
| **Ceará** | Técnico | 4.916 | 4.368 | +563 | 42,7% | R$ 2.000,00 | R$ 2.094,30 | -4,5% |
| **Resto do Nordeste** | Gestão | 1.270 | 1.587 | -320 | 10,2% | R$ 6.491,70 | R$ 5.873,19 | +10,5% |
| **Resto do Nordeste** | Superior / Analista | 13.371 | 12.384 | +995 | 27,3% | R$ 3.600,00 | R$ 3.993,37 | -9,8% |
| **Resto do Nordeste** | Técnico | 14.635 | 12.018 | +2.614 | 49,6% | R$ 1.847,88 | R$ 1.918,00 | -3,7% |
| **Resto do Brasil** | Gestão | 22.482 | 29.812 | -7.319 | 5,2% | R$ 12.612,71 | R$ 13.000,00 | -3,0% |
| **Resto do Brasil** | Superior / Analista | 228.733 | 216.631 | +12.034 | 24,5% | R$ 5.680,00 | R$ 5.871,77 | -3,3% |
| **Resto do Brasil** | Técnico | 122.323 | 109.215 | +12.975 | 47,0% | R$ 2.412,98 | R$ 2.600,00 | -7,2% |

> **Nota sobre o Saldo Líquido:** as colunas *Admissões* e *Desligamentos* contam apenas **eventos ativos** (MOV + FOR), enquanto o *Saldo Líquido* também desconta as **exclusões** (MOV + FOR − EXC). Por isso, em algumas linhas *Admissões − Desligamentos* difere do saldo (ex.: Ceará / Superior-Analista: 4.071 − 3.868 = 203, mas o saldo oficial é **+208**).

### Visualização Interativa (Data Serving)

O dashboard em Streamlit ([`src/app.py`](src/app.py)) consome diretamente o mart `gold.agg_caged_ti_mensal` e permite filtrar os indicadores acima por competência, recorte geográfico e nível de carreira.

![Dashboard](docs/dashboard.png)

**H1 · Barateamento da mão de obra**

![H1](docs/h1.png)

**H2 · Porta de entrada**

![H2](docs/h2.png)

**H3 · Dinâmica regional**

![H3](docs/h3.png)

![H3 - Consolidado](docs/h3_2.png)

**Data Quality**

![Data Quality](docs/data_quality.png)

### Veredito das Hipóteses

1. **H1 (Substituição e Barateamento da Mão de Obra) — EVIDÊNCIA CONSISTENTE COM A HIPÓTESE:**
   - Em todas as regiões do país, profissionais contratados nos níveis **Técnico** e **Superior / Analista** entram ganhando uma mediana salarial inferior à dos profissionais desligados. O maior deságio na reposição de Analistas ocorre no **Resto do Nordeste (-9,8%)**, onde a mediana cai de R$ 3.993,37 nos desligados para R$ 3.600,00 nos admitidos, seguido pelo **Resto do Brasil (-3,3%)**.
   - **Enxugamento severo de Gestão (`CBO 1425`):** Houve retração líquida estrutural em cargos de gerência e coordenação de TI em todo o território nacional (**-7.319 vagas no Resto do Brasil**, **-320 no Resto do Nordeste** e **-90 no Ceará**). No Ceará e no Nordeste, o aumento na mediana de entrada de Gestão reflete o corte da média gerência mais barata e a retenção/contratação restrita a posições sêniores pontuais.
   - **Nota metodológica:** os microdados mostram um deságio salarial líquido entre o fluxo de entrada e o de saída nos mesmos grupos ocupacionais. Por serem dados anonimizados, sem identificador de empresa ou indivíduo, eles não medem substituição direta 1-para-1 no mesmo CNPJ, e a associação não é causal sem controle de variáveis adicionais (porte da empresa, senioridade, inflação e composição dos cargos).

2. **H2 (Porta de Entrada e Primeiro Emprego) — EVIDÊNCIA CONSISTENTE COM A HIPÓTESE (+ ACHADO TÉCNICO DE DATA QUALITY):**
   - **Descoberta de Engenharia (Degradação de Variável Pós-eSocial):** A auditoria na camada Gold revelou que o código oficial `tipomovimentação = 10` (*Primeiro Emprego*) encontra-se severamente subnotificado nos microdados pós-migração para o eSocial (com quase a totalidade das admissões classificadas nos códigos `20 - Reemprego` ou `97 - Admissão de tipo ignorado`).
   - Utilizando a **faixa etária (≤ 24 anos)** como proxy confiável de entrada no mercado formal, evidencia-se que a base **Técnica** é a verdadeira locomotiva de absorção de iniciantes (representando **42,7% das contratações no Ceará** e **49,6% no Resto do Nordeste**), além de responder por **24,9%** das contratações de nível Superior/Analista no estado.

3. **H3 (Dinâmica Regional: Ceará vs. Nordeste vs. Centro-Sul) — EVIDÊNCIA CONSISTENTE COM A HIPÓTESE:**
   - O **Ceará** posiciona-se como polo remuneratório superior à média do Nordeste em todas as faixas da carreira de TI: paga **+13,5% a mais** na mediana de entrada para Analistas/Devs (**R$ 4.086,17** vs. **R$ 3.600,00**) e **+23,2% a mais** para Gestão (**R$ 8.000,00** vs. **R$ 6.491,70**).
   - Em relação ao eixo Sul-Sudeste/Centro-Oeste (**R$ 5.680,00** para Analistas), o Ceará mantém um *gap* salarial de **-28,1%**, mas apresenta maior estabilidade na reposição salarial de analistas (diferença de apenas **-0,4%** entre admitidos e desligados no estado).

---

## Estrutura do Repositório

```text
caged_ti_pipeline/
├── .github/workflows/
│   └── ci.yml                         # CI (lint + pytest + validação dbt)
├── data/                              # Dados locais ignorados no Git (bronze/, tmp/, caged.duckdb)
├── dbt_caged/                         # Projeto dbt
│   ├── macros/
│   │   ├── generate_schema_name.sql   # Schemas limpos (silver / gold) sem prefixo do target
│   │   └── stg_caged_base.sql         # Lógica de staging compartilhada entre MOV / FOR / EXC
│   ├── models/
│   │   ├── staging/                   # stg_caged_mov, stg_caged_for, stg_caged_exc, _sources.yml
│   │   └── marts/
│   │       ├── dim_cbo.sql
│   │       ├── fato_movimentacoes_ti.sql
│   │       ├── agg_caged_ti_mensal.sql
│   │       └── _marts.yml             # Documentação e testes da Gold
│   └── seeds/
│       └── seed_cbo_ti.csv            # Escopo oficial de ocupações de TI (CBO 2002)
├── src/
│   ├── extract_ftp.py                 # CLI de ingestão FTP → Bronze (Parquet)
│   └── app.py                         # Dashboard Streamlit
├── tests/
│   └── test_pipeline.py               # Testes com dados sintéticos (pytest)
├── Makefile                           # Atalhos de execução
└── requirements.txt                   # Dependências do ambiente Python
```

---

## Como Executar

### 1. Ambiente

```powershell
# Criar e ativar o ambiente virtual (Windows PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Ingestão Bronze (CLI `src/extract_ftp.py`)

```powershell
# Uma competência
python src/extract_ftp.py --competencia 202501

# Múltiplas competências
python src/extract_ftp.py --competencia 202501 202502

# Ano inteiro (backfill)
python src/extract_ftp.py --ano 2025

# Apenas um tipo de arquivo (teste rápido)
python src/extract_ftp.py --competencia 202601 --tipos EXC

# Forçar reprocessamento, ignorando a trava de idempotência
python src/extract_ftp.py --competencia 202601 --force
```

### 3. Transformação e Testes (dbt)

```powershell
# Executar a partir da pasta dbt_caged (onde está o dbt_project.yml)
cd dbt_caged

# Execução completa: seeds + modelos Silver/Gold + testes
python -m dbt.cli.main build --profiles-dir .

# Documentação e grafo de linhagem
python -m dbt.cli.main docs generate --profiles-dir .
python -m dbt.cli.main docs serve --profiles-dir .
```

### 4. Dashboard (Streamlit)

```powershell
# Na raiz do repositório
cd ..
python -m streamlit run src/app.py
```

### Atalhos via Makefile

```powershell
make setup                # instala dependências
make extract COMP=202601  # ingestão Bronze de uma competência
make build                # dbt seed + run + test
make pytest               # testes automatizados com dados sintéticos
make docs                 # gera e serve a documentação do dbt
make pipeline COMP=202601 # extract + build
```

> [!WARNING]
> **Nota sobre Concorrência no DuckDB:** o DuckDB é um banco *in-process* em arquivo único (`data/caged.duckdb`). Conexões de escrita (`dbt build`) exigem que clientes externos conectados em modo de escrita (ex.: DBeaver) estejam **desconectados**, caso contrário ocorre erro de *lock*. O [`src/app.py`](src/app.py) já opera com `read_only=True`.

---

## Stack Tecnológica

Python · DuckDB · dbt-duckdb · Apache Parquet (ZSTD) · Streamlit · GitHub Actions
