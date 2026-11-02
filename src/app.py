"""Dashboard Streamlit — Camada de Data Serving do pipeline CAGED TI.

Consome as tabelas do schema `gold` em `data/caged.duckdb` em modo read-only
(sem risco de lock com o `dbt build`).

Uso (na raiz do repositório):
    python -m streamlit run src/app.py
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="CAGED TI - Analytics", page_icon="📊", layout="wide")

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "caged.duckdb"
RECORTES = ["Ceará", "Resto do Nordeste", "Resto do Brasil"]
NIVEIS = ["Técnico", "Superior / Analista", "Gestão"]
CORES_RECORTE = {
    "Ceará": "#00CC96",
    "Resto do Nordeste": "#FFA15A",
    "Resto do Brasil": "#636EFA",
}
CORES_MOV = {"Admitidos (Entrou)": "#00CC96", "Desligados (Saiu)": "#EF553B"}


def _mtime() -> float:
    """Versão do banco: invalida o cache automaticamente após um novo `dbt build`."""
    return DB_PATH.stat().st_mtime


@st.cache_data(show_spinner="Consultando a camada Gold...")
def consultar(query: str, params: tuple = (), versao: float = 0.0) -> pd.DataFrame:
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        df = con.execute(query, list(params)).df()
    # DECIMAL/HUGEINT chegam como object -> converte para float ou int64. Colunas de texto não são tocadas.
    for col in df.columns:
        if df[col].dtype == object:
            primeiro = df[col].dropna()
            if not primeiro.empty and isinstance(
                primeiro.iloc[0], (int, float, Decimal)
            ):
                dtype = (
                    "int64"
                    if isinstance(primeiro.iloc[0], int) and not df[col].isna().any()
                    else float
                )
                df[col] = df[col].astype(dtype)
    return df


def brl(valor: float | None) -> str:
    if valor is None or pd.isna(valor):
        return "—"
    return "R$ " + f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def inteiro(valor: float, sinal: bool = False) -> str:
    return (f"{int(valor):+,}" if sinal else f"{int(valor):,}").replace(",", ".")


def fmt_comp(c: str) -> str:
    return f"{c[4:]}/{c[:4]}"


if not DB_PATH.exists():
    st.error(
        f"Banco não encontrado em `{DB_PATH}`.\n\n"
        "Execute a ingestão (`python src/extract_ftp.py ...`) e o "
        "`python -m dbt.cli.main build --profiles-dir .` dentro de `dbt_caged/` antes de abrir o dashboard."
    )
    st.stop()

try:
    versao = _mtime()
    competencias = (
        consultar(
            "SELECT DISTINCT competencia_mov FROM gold.agg_caged_ti_mensal ORDER BY 1",
            versao=versao,
        )["competencia_mov"]
        .astype(str)
        .tolist()
    )
except duckdb.Error as e:
    st.error(f"Não foi possível ler a camada Gold: {e}")
    st.stop()

if not competencias:
    st.warning("Tabela Gold vazia.")
    st.stop()

st.sidebar.header("Filtros")
# Declarações fora do prazo (FOR) e exclusões (EXC) referenciam competências antigas;
# por padrão o painel foca na safra analisada (2025 em diante).
padrao_ini = next((c for c in competencias if c >= "202501"), competencias[0])
if len(competencias) > 1:
    comp_ini, comp_fim = st.sidebar.select_slider(
        "Período (competência da movimentação)",
        options=competencias,
        value=(padrao_ini, competencias[-1]),
        format_func=fmt_comp,
    )
else:
    comp_ini = comp_fim = competencias[0]
    st.sidebar.info(f"Apenas uma competência disponível: {fmt_comp(comp_ini)}")
recortes_sel = st.sidebar.multiselect("Recorte geográfico", RECORTES, default=RECORTES)
niveis_sel = st.sidebar.multiselect("Nível de carreira", NIVEIS, default=NIVEIS)

if not recortes_sel or not niveis_sel:
    st.warning("Selecione ao menos um recorte geográfico e um nível de carreira.")
    st.stop()

st.sidebar.caption(
    "Fonte: Novo CAGED (MTE) · Camada Gold (`dbt` + `DuckDB`).\n\n"
    "Medianas calculadas sobre o grão individual (`fato_movimentacoes_ti`), "
    "pois mediana não é aditiva."
)

# Filtro SQL parametrizado (sem interpolação de valores do usuário)
ph_rec = ", ".join("?" * len(recortes_sel))
ph_niv = ", ".join("?" * len(niveis_sel))
FILTRO = (
    f"competencia_mov BETWEEN ? AND ? "
    f"AND recorte_geografico IN ({ph_rec}) AND nivel_carreira IN ({ph_niv})"
)
PARAMS = (comp_ini, comp_fim, *recortes_sel, *niveis_sel)

st.title("📊 Dinâmica do Emprego Formal de TI no Brasil")
st.markdown(
    f"Novo CAGED · **{fmt_comp(comp_ini)} a {fmt_comp(comp_fim)}** · "
    "Camada **Gold** (`DuckDB` + `dbt`) comparando **Ceará, Resto do Nordeste e Resto do Brasil**."
)

kpi = consultar(
    f"""
    SELECT
        SUM(is_admissao_ativa)                                                    AS admissoes,
        SUM(is_desligamento_ativo)                                                AS desligamentos,
        SUM(saldo_ajustado)                                                       AS saldo,
        MEDIAN(CASE WHEN is_admissao_ativa = 1 THEN salario_valido END)           AS med_entrou,
        MEDIAN(CASE WHEN is_desligamento_ativo = 1 THEN salario_valido END)       AS med_saiu,
        100.0 * SUM(CASE WHEN is_admissao_ativa = 1 AND idade <= 24 THEN 1 ELSE 0 END)
              / NULLIF(SUM(is_admissao_ativa), 0)                                 AS pct_jovens
    FROM gold.fato_movimentacoes_ti
    WHERE {FILTRO}
    """,
    PARAMS,
    versao=versao,
).iloc[0]

if pd.isna(kpi["admissoes"]) or kpi["admissoes"] == 0:
    st.info("Nenhuma movimentação encontrada para os filtros selecionados.")
    st.stop()

gap = (kpi["med_entrou"] / kpi["med_saiu"] - 1) * 100 if kpi["med_saiu"] else None
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Admissões", inteiro(kpi["admissoes"]))
c2.metric("Desligamentos", inteiro(kpi["desligamentos"]))
c3.metric("Saldo Líquido", inteiro(kpi["saldo"], sinal=True), help="MOV + FOR − EXC")
c4.metric(
    "Mediana Admitidos",
    brl(kpi["med_entrou"]),
    delta=f"{gap:+.1f}% vs. desligados" if gap is not None else None,
)
c5.metric("Mediana Desligados", brl(kpi["med_saiu"]))
c6.metric(
    "% Jovens (≤ 24) nas Admissões", f"{kpi['pct_jovens']:.1f}%".replace(".", ",")
)

st.divider()

aba_h1, aba_h2, aba_h3, aba_dq = st.tabs(
    [
        "H1 · Barateamento da Mão de Obra",
        "H2 · Porta de Entrada",
        "H3 · Dinâmica Regional",
        "Data Quality",
    ]
)

with aba_h1:
    st.subheader("Mediana salarial: admitidos vs. desligados")
    st.caption(
        "Gap negativo = quem entra ganha menos do que quem sai (substituição por mão de obra mais barata)."
    )
    sal = consultar(
        f"""
        SELECT
            recorte_geografico, nivel_carreira,
            MEDIAN(CASE WHEN is_admissao_ativa = 1 THEN salario_valido END)     AS "Admitidos (Entrou)",
            MEDIAN(CASE WHEN is_desligamento_ativo = 1 THEN salario_valido END) AS "Desligados (Saiu)"
        FROM gold.fato_movimentacoes_ti
        WHERE {FILTRO}
        GROUP BY 1, 2
        """,
        PARAMS,
        versao=versao,
    )
    sal_long = sal.melt(
        id_vars=["recorte_geografico", "nivel_carreira"],
        value_vars=list(CORES_MOV),
        var_name="Movimento",
        value_name="Mediana Salarial (R$)",
    )
    fig = px.bar(
        sal_long,
        x="recorte_geografico",
        y="Mediana Salarial (R$)",
        color="Movimento",
        barmode="group",
        facet_col="nivel_carreira",
        text_auto=",.0f",
        color_discrete_map=CORES_MOV,
        category_orders={"recorte_geografico": RECORTES, "nivel_carreira": NIVEIS},
        labels={"recorte_geografico": "", "nivel_carreira": "Nível"},
    )
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.update_layout(legend={"orientation": "h", "y": 1.15, "x": 0}, separators=",.")
    st.plotly_chart(fig, use_container_width=True)

    sal["Gap Entrada vs. Saída (%)"] = (
        sal["Admitidos (Entrou)"] / sal["Desligados (Saiu)"] - 1
    ) * 100
    tabela = sal.pivot(
        index="nivel_carreira",
        columns="recorte_geografico",
        values="Gap Entrada vs. Saída (%)",
    )
    tabela = tabela.reindex(
        index=[n for n in NIVEIS if n in tabela.index],
        columns=[r for r in RECORTES if r in tabela.columns],
    )
    st.markdown("**Gap Entrada vs. Saída (%)**")
    st.dataframe(
        tabela.style.format("{:+.1f}%", na_rep="—").map(
            lambda v: (
                ""
                if pd.isna(v)
                else f"color: {'#EF553B' if v < 0 else '#00CC96'}; font-weight: 600"
            )
        ),
        use_container_width=True,
    )

with aba_h2:
    st.subheader("Participação de jovens (≤ 24 anos) nas admissões")
    st.caption(
        "Proxy de entrada no mercado formal, já que o código oficial de 1º emprego está degradado pós-eSocial."
    )
    jov = consultar(
        f"""
        SELECT
            recorte_geografico, nivel_carreira,
            100.0 * SUM(admissoes_jovens_ate_24) / NULLIF(SUM(total_admissoes), 0) AS pct_jovens,
            SUM(admissoes_jovens_ate_24) AS jovens,
            SUM(total_admissoes)         AS admissoes
        FROM gold.agg_caged_ti_mensal
        WHERE {FILTRO}
        GROUP BY 1, 2
        """,
        PARAMS,
        versao=versao,
    )
    col_a, col_b = st.columns([3, 2])
    with col_a:
        fig = px.bar(
            jov,
            x="nivel_carreira",
            y="pct_jovens",
            color="recorte_geografico",
            barmode="group",
            text_auto=".1f",
            color_discrete_map=CORES_RECORTE,
            category_orders={"nivel_carreira": NIVEIS, "recorte_geografico": RECORTES},
            labels={
                "nivel_carreira": "",
                "pct_jovens": "% das admissões",
                "recorte_geografico": "Recorte",
            },
            hover_data={"jovens": ":,", "admissoes": ":,"},
        )
        fig.update_layout(
            legend={"orientation": "h", "y": 1.12, "x": 0}, separators=",."
        )
        st.plotly_chart(fig, use_container_width=True)
    with col_b:
        st.markdown("**Faixa etária dos admitidos**")
        faixa = consultar(
            f"""
            SELECT faixa_etaria, COUNT(*) AS admissoes
            FROM gold.fato_movimentacoes_ti
            WHERE is_admissao_ativa = 1 AND {FILTRO}
            GROUP BY 1
            """,
            PARAMS,
            versao=versao,
        )
        fig = px.pie(
            faixa,
            names="faixa_etaria",
            values="admissoes",
            hole=0.5,
            category_orders={
                "faixa_etaria": [
                    "Até 24 anos (Jovem)",
                    "25 a 34 anos",
                    "35 a 44 anos",
                    "45 anos ou mais",
                    "Não informado",
                ]
            },
        )
        fig.update_layout(legend={"orientation": "h", "y": -0.1}, separators=",.")
        st.plotly_chart(fig, use_container_width=True)

with aba_h3:
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Evolução mensal do saldo líquido")
        recorte_evol = st.selectbox(
            "Recorte geográfico", recortes_sel, key="recorte_evol"
        )
        evol = consultar(
            f"""
            SELECT competencia_mov, nivel_carreira, saldo_liquido
            FROM gold.agg_caged_ti_mensal
            WHERE {FILTRO} AND recorte_geografico = ?
            ORDER BY competencia_mov
            """,
            (*PARAMS, recorte_evol),
            versao=versao,
        )
        evol["Competência"] = pd.to_datetime(
            evol["competencia_mov"].astype(str), format="%Y%m"
        )
        fig = px.line(
            evol,
            x="Competência",
            y="saldo_liquido",
            color="nivel_carreira",
            markers=True,
            category_orders={"nivel_carreira": NIVEIS},
            labels={"saldo_liquido": "Saldo Líquido", "nivel_carreira": "Nível"},
        )
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        fig.update_layout(
            legend={"orientation": "h", "y": 1.12, "x": 0}, separators=",."
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Mediana salarial de admissão por recorte")
        med = consultar(
            f"""
            SELECT recorte_geografico, nivel_carreira,
                   MEDIAN(salario_valido) AS mediana_admissao
            FROM gold.fato_movimentacoes_ti
            WHERE is_admissao_ativa = 1 AND {FILTRO}
            GROUP BY 1, 2
            """,
            PARAMS,
            versao=versao,
        )
        fig = px.bar(
            med,
            x="nivel_carreira",
            y="mediana_admissao",
            color="recorte_geografico",
            barmode="group",
            text_auto=",.0f",
            color_discrete_map=CORES_RECORTE,
            category_orders={"nivel_carreira": NIVEIS, "recorte_geografico": RECORTES},
            labels={
                "nivel_carreira": "",
                "mediana_admissao": "Mediana (R$)",
                "recorte_geografico": "Recorte",
            },
        )
        fig.update_layout(
            legend={"orientation": "h", "y": 1.12, "x": 0}, separators=",."
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Consolidado do período**")
    cons = consultar(
        f"""
        SELECT
            recorte_geografico AS "Recorte", nivel_carreira AS "Nível",
            SUM(total_admissoes) AS "Admissões", SUM(total_desligamentos) AS "Desligamentos",
            SUM(saldo_liquido) AS "Saldo Líquido"
        FROM gold.agg_caged_ti_mensal
        WHERE {FILTRO}
        GROUP BY 1, 2
        """,
        PARAMS,
        versao=versao,
    )
    cons["Recorte"] = pd.Categorical(cons["Recorte"], RECORTES, ordered=True)
    cons["Nível"] = pd.Categorical(cons["Nível"], NIVEIS, ordered=True)
    cons = cons.sort_values(["Recorte", "Nível"])
    st.dataframe(
        cons.style.format(
            {
                "Admissões": "{:,.0f}",
                "Desligamentos": "{:,.0f}",
                "Saldo Líquido": "{:+,.0f}",
            },
            thousands=".",
        ),
        hide_index=True,
        use_container_width=True,
    )

with aba_dq:
    st.subheader("Degradação do código de 1º emprego pós-eSocial")
    st.caption(
        "Distribuição de `tipo_movimentacao_codigo` nas admissões ativas. "
        "O código 10 (Primeiro Emprego) praticamente desaparece, justificando o uso da idade como proxy na H2."
    )
    tipos = consultar(
        f"""
        SELECT CAST(tipo_movimentacao_codigo AS VARCHAR) AS codigo, COUNT(*) AS admissoes
        FROM gold.fato_movimentacoes_ti
        WHERE is_admissao_ativa = 1 AND {FILTRO}
        GROUP BY 1 ORDER BY 2 DESC
        """,
        PARAMS,
        versao=versao,
    )
    rotulos = {
        "10": "10 · Primeiro emprego",
        "20": "20 · Reemprego",
        "25": "25 · Contrato prazo determinado",
        "35": "35 · Reintegração",
        "97": "97 · Admissão de tipo ignorado",
    }
    tipos["Tipo"] = tipos["codigo"].map(lambda c: rotulos.get(c, c))
    tipos["%"] = 100 * tipos["admissoes"] / tipos["admissoes"].sum()
    col_a, col_b = st.columns([3, 2])
    with col_a:
        fig = px.bar(
            tipos,
            x="admissoes",
            y="Tipo",
            orientation="h",
            text=tipos["%"].map("{:.2f}%".format),
            labels={"admissoes": "Admissões", "Tipo": ""},
        )
        fig.update_layout(separators=",.", yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
    with col_b:
        cod10 = int(tipos.loc[tipos["codigo"] == "10", "admissoes"].sum())
        st.metric("Admissões com código 10", inteiro(cod10))
        st.metric(
            "Admissões de jovens ≤ 24 (proxy)",
            inteiro(jov["jovens"].sum()) if not jov.empty else "—",
        )
        st.info(
            "Uma diferença dessa magnitude indica subnotificação estrutural da variável na origem, "
            "não ausência real de primeiros empregos."
        )

    st.divider()
    st.subheader("Outliers salariais neutralizados na camada Silver")
    st.caption(
        "Registros com `salario_valido IS NULL` e salário bruto informado (fora da faixa válida)."
    )
    out = consultar(
        f"""
        SELECT
            CAST(COUNT(*) FILTER (WHERE salario_valido IS NULL AND salario_bruto IS NOT NULL) AS BIGINT)
                AS neutralizados,
            CAST(COUNT(*) AS BIGINT) AS total,
            CAST(MAX(salario_bruto) AS DOUBLE) AS max_bruto
        FROM gold.fato_movimentacoes_ti
        WHERE {FILTRO}
        """,
        PARAMS,
        versao=versao,
    ).iloc[0]
    o1, o2, o3 = st.columns(3)
    o1.metric(
        "Salários inválidos/outliers neutralizados", inteiro(out["neutralizados"])
    )
    o2.metric(
        "% do total de registros",
        f"{100 * out['neutralizados'] / out['total']:.2f}%".replace(".", ",")
        if out["total"]
        else "—",
    )
    o3.metric("Maior salário bruto no período", brl(out["max_bruto"]))
