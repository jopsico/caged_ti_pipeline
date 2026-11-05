"""Métricas consolidadas da camada Gold, derivadas diretamente do DuckDB.

Fonte única para conferir o README contra o dashboard: ambos devem refletir
exatamente estas definições.

Uso (na raiz do repositório):
    python src/metricas.py                       # imprime em Markdown
    python src/metricas.py --out docs/metricas.md
    python src/metricas.py --ini 202501 --fim 202608
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import duckdb
import pandas as pd

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "caged.duckdb"
RECORTES = ["Ceará", "Resto do Nordeste", "Resto do Brasil"]
NIVEIS = ["Técnico", "Superior / Analista", "Gestão"]
ROTULOS_TIPO_MOV = {
    10: "Primeiro emprego",
    20: "Reemprego",
    25: "Contrato por prazo determinado",
    35: "Reintegração",
    97: "Admissão de tipo ignorado",
}

# Janela de análise = competências cobertas pelos arquivos MOV (declaração no prazo).
# FOR e EXC também referenciam competências anteriores, mas só como retificações parciais:
# sem o MOV dessas competências, qualquer indicador delas ficaria incompleto.
SQL_JANELA_ANALISE = """
    SELECT MIN(competencia_mov) AS ini, MAX(competencia_mov) AS fim
    FROM gold.fato_movimentacoes_ti
    WHERE tipo_arquivo = 'MOV'
"""

_FILTRO_PERIODO = "competencia_mov BETWEEN ? AND ?"


def gap_percentual(entrada: float | None, saida: float | None) -> float | None:
    """Variação percentual da mediana de admissão frente à de desligamento."""
    if entrada is None or saida is None or pd.isna(entrada) or pd.isna(saida):
        return None
    e, s = float(entrada), float(saida)
    if not math.isfinite(e) or not math.isfinite(s) or s == 0:
        return None
    resultado = (e / s - 1) * 100
    return resultado if math.isfinite(resultado) else None


def janela_analise(con: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    ini, fim = con.execute(SQL_JANELA_ANALISE).fetchone()
    if ini is None:
        raise ValueError("Nenhuma movimentação MOV na camada Gold.")
    return str(ini), str(fim)


def volume(con: duckdb.DuckDBPyConnection, ini: str, fim: str) -> dict[str, int]:
    """Contagens por critério, sem somar grãos distintos entre si."""
    r = con.execute(
        f"""
        SELECT
            COUNT(*)                                                       AS registros,
            CAST(SUM(is_admissao_ativa) AS BIGINT)                         AS admissoes_ativas,
            CAST(SUM(is_desligamento_ativo) AS BIGINT)                     AS desligamentos_ativos,
            CAST(COUNT(*) FILTER (WHERE indicador_exclusao = 1) AS BIGINT) AS exclusoes,
            CAST(SUM(saldo_ajustado) AS BIGINT)                            AS saldo_liquido,
            CAST(COUNT(*) FILTER (
                WHERE salario_valido IS NULL AND salario_bruto IS NOT NULL
            ) AS BIGINT)                                                   AS salarios_neutralizados
        FROM gold.fato_movimentacoes_ti
        WHERE {_FILTRO_PERIODO}
        """,
        [ini, fim],
    ).df()
    return {k: int(v) for k, v in r.iloc[0].items()}


def consolidado(con: duckdb.DuckDBPyConnection, ini: str, fim: str) -> pd.DataFrame:
    """Nove combinações recorte x nível: volumes (mart agregado) e medianas (grão individual)."""
    vol = con.execute(
        f"""
        SELECT recorte_geografico, nivel_carreira,
               CAST(SUM(total_admissoes) AS BIGINT)     AS admissoes,
               CAST(SUM(total_desligamentos) AS BIGINT) AS desligamentos,
               CAST(SUM(saldo_liquido) AS BIGINT)       AS saldo,
               100.0 * SUM(admissoes_jovens_ate_24) / NULLIF(SUM(total_admissoes), 0)
                                                        AS pct_jovens
        FROM gold.agg_caged_ti_mensal
        WHERE {_FILTRO_PERIODO}
        GROUP BY 1, 2
        """,
        [ini, fim],
    ).df()
    med = con.execute(
        f"""
        SELECT recorte_geografico, nivel_carreira,
               CAST(MEDIAN(CASE WHEN is_admissao_ativa = 1 THEN salario_valido END)
                    AS DOUBLE) AS med_entrou,
               CAST(MEDIAN(CASE WHEN is_desligamento_ativo = 1 THEN salario_valido END)
                    AS DOUBLE) AS med_saiu
        FROM gold.fato_movimentacoes_ti
        WHERE {_FILTRO_PERIODO}
        GROUP BY 1, 2
        """,
        [ini, fim],
    ).df()
    df = vol.merge(med, on=["recorte_geografico", "nivel_carreira"], how="outer")
    df["gap_pct"] = [
        gap_percentual(e, s)
        for e, s in zip(df["med_entrou"], df["med_saiu"], strict=True)
    ]
    df["recorte_geografico"] = pd.Categorical(
        df["recorte_geografico"], RECORTES, ordered=True
    )
    df["nivel_carreira"] = pd.Categorical(df["nivel_carreira"], NIVEIS, ordered=True)
    return df.sort_values(["recorte_geografico", "nivel_carreira"]).reset_index(
        drop=True
    )


def gaps_mensais_h1(con: duckdb.DuckDBPyConnection, ini: str, fim: str) -> pd.DataFrame:
    """Contagem mensal de competências por direção do gap salarial (H1) nas 9 combinações.

    Calcula as medianas de admissão e desligamento diretamente sobre o grão individual
    de `fato_movimentacoes_ti` em cada competência da janela e aplica `gap_percentual`.
    Competências sem mediana válida de entrada ou de saída ficam como `gap_indefinido`
    e nunca são contabilizadas como gap negativo.
    """
    competencias = [
        str(r[0])
        for r in con.execute(
            f"""
            SELECT DISTINCT competencia_mov
            FROM gold.fato_movimentacoes_ti
            WHERE {_FILTRO_PERIODO}
            ORDER BY 1
            """,
            [ini, fim],
        ).fetchall()
    ]
    mensal = con.execute(
        f"""
        SELECT competencia_mov,
               recorte_geografico,
               nivel_carreira,
               CAST(MEDIAN(CASE WHEN is_admissao_ativa = 1 THEN salario_valido END)
                    AS DOUBLE) AS med_entrou,
               CAST(MEDIAN(CASE WHEN is_desligamento_ativo = 1 THEN salario_valido END)
                    AS DOUBLE) AS med_saiu
        FROM gold.fato_movimentacoes_ti
        WHERE {_FILTRO_PERIODO}
        GROUP BY 1, 2, 3
        """,
        [ini, fim],
    ).df()

    grade = (
        pd.MultiIndex.from_product(
            [competencias, RECORTES, NIVEIS],
            names=["competencia_mov", "recorte_geografico", "nivel_carreira"],
        )
        .to_frame()
        .reset_index(drop=True)
    )

    df = grade.merge(
        mensal,
        on=["competencia_mov", "recorte_geografico", "nivel_carreira"],
        how="left",
    )
    df["gap_pct"] = [
        gap_percentual(e, s)
        for e, s in zip(df["med_entrou"], df["med_saiu"], strict=True)
    ]

    registros: list[dict[str, object]] = []
    total_janela = len(competencias)
    for recorte in RECORTES:
        for nivel in NIVEIS:
            sub = df[
                (df["recorte_geografico"] == recorte) & (df["nivel_carreira"] == nivel)
            ]["gap_pct"]
            validos = sub.dropna()
            neg = int((validos < 0).sum())
            zero = int((validos == 0).sum())
            pos = int((validos > 0).sum())
            n_validos = len(validos)
            indef = total_janela - n_validos
            registros.append(
                {
                    "recorte_geografico": recorte,
                    "nivel_carreira": nivel,
                    "competencias_janela": total_janela,
                    "competencias_validas": n_validos,
                    "gap_negativo": neg,
                    "gap_zero": zero,
                    "gap_positivo": pos,
                    "gap_indefinido": indef,
                }
            )
    return pd.DataFrame(registros)


def tipos_movimentacao(
    con: duckdb.DuckDBPyConnection, ini: str, fim: str
) -> pd.DataFrame:
    """Distribuição de `tipo_movimentacao_codigo` nas admissões ativas (incluindo códigos 10, 20, 35 e 97)."""
    return con.execute(
        f"""
        WITH codigos_ref(codigo) AS (
            VALUES (10), (20), (35), (97)
        ),
        contagem AS (
            SELECT tipo_movimentacao_codigo AS codigo,
                   CAST(COUNT(*) AS BIGINT) AS admissoes
            FROM gold.fato_movimentacoes_ti
            WHERE is_admissao_ativa = 1 AND {_FILTRO_PERIODO}
            GROUP BY 1
        ),
        unificado AS (
            SELECT COALESCE(c.codigo, r.codigo) AS codigo,
                   COALESCE(c.admissoes, 0)     AS admissoes
            FROM contagem c
            FULL OUTER JOIN codigos_ref r ON c.codigo = r.codigo
        )
        SELECT codigo,
               admissoes,
               COALESCE(100.0 * admissoes / NULLIF(SUM(admissoes) OVER (), 0), 0.0) AS pct
        FROM unificado
        ORDER BY admissoes DESC, codigo ASC
        """,
        [ini, fim],
    ).df()


def diferencial_regional_h3(
    con: duckdb.DuckDBPyConnection,
    ini: str,
    fim: str,
    cons: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Comparativo da mediana salarial de admissão do Ceará frente ao Resto do Nordeste e ao Resto do Brasil (H3)."""
    if cons is None:
        cons = consolidado(con, ini, fim)
    por_chave = {
        (str(r.recorte_geografico), str(r.nivel_carreira)): r for r in cons.itertuples()
    }
    registros: list[dict[str, object]] = []
    for nivel in NIVEIS:
        ce = por_chave.get(("Ceará", nivel))
        ne = por_chave.get(("Resto do Nordeste", nivel))
        br = por_chave.get(("Resto do Brasil", nivel))
        med_ce = ce.med_entrou if ce is not None else None
        med_ne = ne.med_entrou if ne is not None else None
        med_br = br.med_entrou if br is not None else None
        registros.append(
            {
                "nivel_carreira": nivel,
                "med_ceara": med_ce,
                "med_resto_ne": med_ne,
                "gap_ce_vs_ne_pct": gap_percentual(med_ce, med_ne),
                "med_resto_br": med_br,
                "gap_ce_vs_br_pct": gap_percentual(med_ce, med_br),
                "saldo_ceara": int(ce.saldo)
                if ce is not None and not pd.isna(ce.saldo)
                else 0,
                "saldo_resto_ne": int(ne.saldo)
                if ne is not None and not pd.isna(ne.saldo)
                else 0,
                "saldo_resto_br": int(br.saldo)
                if br is not None and not pd.isna(br.saldo)
                else 0,
            }
        )
    return pd.DataFrame(registros)


# ------------------------------------------------------------------ formatação pt-BR
def _int(v: float, sinal: bool = False) -> str:
    return (f"{int(v):+,}" if sinal else f"{int(v):,}").replace(",", ".")


def _dec(v: float, casas: int = 1, sinal: bool = False) -> str:
    texto = f"{v:+,.{casas}f}" if sinal else f"{v:,.{casas}f}"
    return texto.replace(",", "X").replace(".", ",").replace("X", ".")


def _brl(v: float | None) -> str:
    return "—" if v is None or pd.isna(v) else "R$ " + _dec(v, 2)


def _pct_gap(v: float | None) -> str:
    return "—" if v is None or pd.isna(v) else _dec(v, 1, True) + "%"


def _comp(c: str) -> str:
    return f"{c[4:]}/{c[:4]}"


def _resumo_competencias_h1(neg: int, validas: int, janela: int) -> str:
    if validas == 0:
        return f"0 de 0 válidos ({janela} na janela)"
    if validas < janela:
        return f"{neg} de {validas} válidos ({janela} na janela)"
    return f"{neg} de {validas}"


def para_markdown(con: duckdb.DuckDBPyConnection, ini: str, fim: str) -> str:
    vol = volume(con, ini, fim)
    cons = consolidado(con, ini, fim)
    h1_mensal = gaps_mensais_h1(con, ini, fim)
    tipos = tipos_movimentacao(con, ini, fim)
    h3_reg = diferencial_regional_h3(con, ini, fim, cons=cons)

    linhas = [
        f"**Período analisado:** {_comp(ini)} a {_comp(fim)} (competência da movimentação)",
        "",
        "### Volume de Dados Processados",
        "",
        "| Grão / critério | Quantidade |",
        "| :--- | ---: |",
        f"| Registros na fato (`fato_movimentacoes_ti`) | {_int(vol['registros'])} |",
        f"| Admissões ativas (MOV + FOR) | {_int(vol['admissoes_ativas'])} |",
        f"| Desligamentos ativos (MOV + FOR) | {_int(vol['desligamentos_ativos'])} |",
        f"| Registros de exclusão (EXC) | {_int(vol['exclusoes'])} |",
        f"| Saldo líquido (MOV + FOR − EXC) | {_int(vol['saldo_liquido'], sinal=True)} |",
        f"| Salários neutralizados (`salario_valido` nulo) | {_int(vol['salarios_neutralizados'])} |",
        "",
        "### Consolidado por Recorte Geográfico e Nível de Carreira",
        "",
        (
            "| Recorte Geográfico | Nível de Carreira | Admissões | Desligamentos | Saldo Líquido "
            "| % Jovens (≤ 24 anos) | Mediana Salarial (Entrou) | Mediana Salarial (Saiu) "
            "| Gap Entrada vs. Saída |"
        ),
        "| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]
    for r in cons.itertuples():
        linhas.append(
            f"| **{r.recorte_geografico}** | {r.nivel_carreira} | {_int(r.admissoes)} "
            f"| {_int(r.desligamentos)} | {_int(r.saldo, True)} | {_dec(r.pct_jovens)}% "
            f"| {_brl(r.med_entrou)} | {_brl(r.med_saiu)} | {_pct_gap(r.gap_pct)} |"
        )

    linhas += [
        "",
        "### H1 — Frequência Mensal de Gaps Salariais por Competência",
        "",
        (
            "> Denominador: `competencias_validas` (competências da janela com mediana salarial válida "
            "tanto em admissões ativas quanto em desligamentos ativos). Meses sem par válido ficam "
            "em `Indefinido` e não entram como gap negativo."
        ),
        "",
        (
            "| Recorte Geográfico | Nível de Carreira | Gap < 0 (Negativo) | Gap = 0 | Gap > 0 (Positivo) "
            "| Indefinido | Competências com Gap Negativo |"
        ),
        "| :--- | :--- | :---: | :---: | :---: | :---: | :---: |",
    ]
    for r in h1_mensal.itertuples():
        resumo = _resumo_competencias_h1(
            int(r.gap_negativo), int(r.competencias_validas), int(r.competencias_janela)
        )
        linhas.append(
            f"| **{r.recorte_geografico}** | {r.nivel_carreira} | {r.gap_negativo} "
            f"| {r.gap_zero} | {r.gap_positivo} | {r.gap_indefinido} | {resumo} |"
        )

    linhas += [
        "",
        "### H2 — Distribuição de `tipomovimentação` nas Admissões Ativas",
        "",
        "| `tipomovimentação` (admissões ativas) | Descrição | Admissões | % |",
        "| :--- | :--- | ---: | ---: |",
    ]
    for r in tipos.itertuples():
        desc = ROTULOS_TIPO_MOV.get(int(r.codigo), "Outros")
        linhas.append(
            f"| {r.codigo} | {desc} | {_int(r.admissoes)} | {_dec(r.pct, 2)}% |"
        )

    linhas += [
        "",
        "### H3 — Diferencial Salarial Regional na Mediana de Admissão",
        "",
        (
            "| Nível de Carreira | Mediana Admissão (Ceará) | Mediana Admissão (Resto do Nordeste) "
            "| Ceará vs. Resto do Nordeste | Mediana Admissão (Resto do Brasil) "
            "| Ceará vs. Resto do Brasil | Saldo Líquido (Ceará) |"
        ),
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]
    for r in h3_reg.itertuples():
        linhas.append(
            f"| {r.nivel_carreira} | {_brl(r.med_ceara)} | {_brl(r.med_resto_ne)} "
            f"| {_pct_gap(r.gap_ce_vs_ne_pct)} | {_brl(r.med_resto_br)} "
            f"| {_pct_gap(r.gap_ce_vs_br_pct)} | {_int(r.saldo_ceara, True)} |"
        )
    return "\n".join(linhas) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--ini", help="Competência inicial AAAAMM (padrão: início da janela MOV)"
    )
    parser.add_argument(
        "--fim", help="Competência final AAAAMM (padrão: fim da janela MOV)"
    )
    parser.add_argument(
        "--out", type=Path, help="Grava o Markdown neste arquivo em vez de imprimir"
    )
    args = parser.parse_args()

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        padrao_ini, padrao_fim = janela_analise(con)
        texto = para_markdown(con, args.ini or padrao_ini, args.fim or padrao_fim)
    if args.out:
        args.out.write_text(texto, encoding="utf-8")
    else:
        sys.stdout.reconfigure(encoding="utf-8")
        print(texto)


if __name__ == "__main__":
    main()
