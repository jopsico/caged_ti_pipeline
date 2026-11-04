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
    if entrada is None or saida is None or math.isnan(entrada) or math.isnan(saida):
        return None
    if saida == 0:
        return None
    return (entrada / saida - 1) * 100


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


def tipos_movimentacao(
    con: duckdb.DuckDBPyConnection, ini: str, fim: str
) -> pd.DataFrame:
    """Distribuição de `tipo_movimentacao_codigo` nas admissões ativas."""
    return con.execute(
        f"""
        SELECT tipo_movimentacao_codigo AS codigo,
               CAST(COUNT(*) AS BIGINT) AS admissoes,
               100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS pct
        FROM gold.fato_movimentacoes_ti
        WHERE is_admissao_ativa = 1 AND {_FILTRO_PERIODO}
        GROUP BY 1 ORDER BY 2 DESC
        """,
        [ini, fim],
    ).df()


# ------------------------------------------------------------------ formatação pt-BR
def _int(v: float, sinal: bool = False) -> str:
    return (f"{int(v):+,}" if sinal else f"{int(v):,}").replace(",", ".")


def _dec(v: float, casas: int = 1, sinal: bool = False) -> str:
    texto = f"{v:+,.{casas}f}" if sinal else f"{v:,.{casas}f}"
    return texto.replace(",", "X").replace(".", ",").replace("X", ".")


def _brl(v: float | None) -> str:
    return "—" if v is None or pd.isna(v) else "R$ " + _dec(v, 2)


def _comp(c: str) -> str:
    return f"{c[4:]}/{c[:4]}"


def para_markdown(con: duckdb.DuckDBPyConnection, ini: str, fim: str) -> str:
    vol = volume(con, ini, fim)
    cons = consolidado(con, ini, fim)
    tipos = tipos_movimentacao(con, ini, fim)

    linhas = [
        f"**Período analisado:** {_comp(ini)} a {_comp(fim)} (competência da movimentação)",
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
        (
            "| Recorte Geográfico | Nível de Carreira | Admissões | Desligamentos | Saldo Líquido "
            "| % Jovens (≤ 24 anos) | Mediana Salarial (Entrou) | Mediana Salarial (Saiu) "
            "| Gap Entrada vs. Saída |"
        ),
        "| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]
    for r in cons.itertuples():
        gap = (
            "—"
            if r.gap_pct is None or pd.isna(r.gap_pct)
            else _dec(r.gap_pct, 1, True) + "%"
        )
        linhas.append(
            f"| **{r.recorte_geografico}** | {r.nivel_carreira} | {_int(r.admissoes)} "
            f"| {_int(r.desligamentos)} | {_int(r.saldo, True)} | {_dec(r.pct_jovens)}% "
            f"| {_brl(r.med_entrou)} | {_brl(r.med_saiu)} | {gap} |"
        )
    linhas += [
        "",
        "| `tipomovimentação` (admissões ativas) | Admissões | % |",
        "| :--- | ---: | ---: |",
    ]
    for r in tipos.itertuples():
        linhas.append(f"| {r.codigo} | {_int(r.admissoes)} | {_dec(r.pct, 2)}% |")
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
