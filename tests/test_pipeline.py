"""Testes do pipeline CAGED TI com dados sintéticos (Bronze -> dbt Silver/Gold)."""

from __future__ import annotations

import math
import os
import re
import shutil
import statistics
import subprocess
import sys
from datetime import datetime
from decimal import Decimal
from ftplib import error_perm
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pandas as pd
import py7zr
import pytest

import extract_ftp
import metricas

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt_caged"
COMPETENCIA = "202501"

CBO_GESTAO = "142505"
CBO_ANALISTA = "212205"
CBO_TECNICO = "317105"

CE, PE, SP = ("23", "2"), ("26", "2"), ("35", "3")  # (uf, região)

COLUNAS_BASE = [
    "competênciamov",
    "competênciadec",
    "região",
    "uf",
    "município",
    "seção",
    "subclasse",
    "cbo2002ocupação",
    "categoria",
    "graudeinstrução",
    "idade",
    "horascontratuais",
    "raçacor",
    "sexo",
    "tipoempregador",
    "tipoestabelecimento",
    "tipomovimentação",
    "tipodedeficiência",
    "indtrabintermitente",
    "indtrabparcial",
    "tamestabjan",
    "indicadoraprendiz",
    "origemdainformação",
    "indicadordeforadoprazo",
    "unidadesaláriocódigo",
    "salário",
    "valorsaláriofixo",
    "saldomovimentação",
]
COLUNAS_EXC = ["competênciaexc", "indicadordeexclusão"]


def _registro(
    uf_regiao: tuple[str, str],
    cbo: str,
    salario: str,
    saldo: int,
    idade: int = 30,
    tipo_mov: str = "20",
    competencia: str = COMPETENCIA,
) -> dict[str, str]:
    uf, regiao = uf_regiao
    return {
        "competênciamov": competencia,
        "competênciadec": COMPETENCIA,
        "região": regiao,
        "uf": uf,
        "município": "000000",
        "seção": "J",
        "subclasse": "6201501",
        "cbo2002ocupação": cbo,
        "categoria": "101",
        "graudeinstrução": "9",
        "idade": str(idade),
        "horascontratuais": "40,00",
        "raçacor": "1",
        "sexo": "1",
        "tipoempregador": "0",
        "tipoestabelecimento": "1",
        "tipomovimentação": tipo_mov,
        "tipodedeficiência": "0",
        "indtrabintermitente": "0",
        "indtrabparcial": "0",
        "tamestabjan": "5",
        "indicadoraprendiz": "0",
        "origemdainformação": "1",
        "indicadordeforadoprazo": "0",
        "unidadesaláriocódigo": "5",
        "salário": salario,
        "valorsaláriofixo": salario,
        "saldomovimentação": str(saldo),
    }


def _gravar_bronze(bronze: Path, tipo: str, registros: list[dict[str, str]]) -> None:
    destino = bronze / f"caged_{tipo}" / f"competencia={COMPETENCIA}" / "data.parquet"
    destino.parent.mkdir(parents=True, exist_ok=True)
    colunas = COLUNAS_BASE + (COLUNAS_EXC if tipo == "exc" else [])
    df = pd.DataFrame(registros, columns=colunas)
    df["_competencia_arquivo"] = COMPETENCIA
    df["_tipo_arquivo"] = tipo.upper()
    df["_arquivo_origem"] = f"CAGED{tipo.upper()}{COMPETENCIA}.txt"
    df["_ingested_at"] = datetime(2025, 1, 31, 12, 0, 0)
    df.to_parquet(destino, index=False)


@pytest.fixture(scope="session")
def bronze_sintetica(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Gera Parquets sintéticos MOV/FOR/EXC no layout Hive da Bronze."""
    bronze = tmp_path_factory.mktemp("data") / "bronze"

    mov = [
        _registro(CE, CBO_GESTAO, "5000,00", 1, idade=22, tipo_mov="10"),
        _registro(PE, CBO_GESTAO, "4000,00", 1),
        _registro(SP, CBO_GESTAO, "999999,00", 1),  # outlier salarial
        _registro(CE, CBO_ANALISTA, "3000,00", -1),
        # Fronteiras do saneamento salarial (válido: 700,00 a 100.000,00)
        _registro(CE, CBO_ANALISTA, "700,00", 1),
        _registro(CE, CBO_ANALISTA, "100000,00", 1),
        _registro(CE, CBO_ANALISTA, "699,99", 1),
        # Duas linhas 100% idênticas (sem deduplicação cega)
        _registro(PE, CBO_ANALISTA, "4500,00", 1),
        _registro(PE, CBO_ANALISTA, "4500,00", 1),
        # Mediana ignorando NULL: válidos 2000/3000/4000/5000 -> 3500; 100,00 e 100001,00 viram NULL
        _registro(SP, CBO_TECNICO, "2000,00", 1),
        _registro(SP, CBO_TECNICO, "3000,00", 1),
        _registro(SP, CBO_TECNICO, "4000,00", 1),
        _registro(SP, CBO_TECNICO, "5000,00", 1),
        _registro(SP, CBO_TECNICO, "100,00", 1),
        _registro(SP, CBO_TECNICO, "100001,00", 1),
    ]
    fora_prazo = [
        _registro(SP, CBO_ANALISTA, "6000,00", 1),
        # Retificação de competência anterior à cobertura MOV: fica na Gold, fora da janela
        _registro(CE, CBO_ANALISTA, "3300,00", 1, competencia="202412"),
    ]
    exclusoes = [_registro(CE, CBO_ANALISTA, "3500,00", 1, tipo_mov="20")]
    exclusoes[0]["indicadordeexclusão"] = "1"
    exclusoes[0]["competênciaexc"] = COMPETENCIA
    exclusoes[0]["origemdainformação"] = "2"
    exclusoes[0]["indicadordeforadoprazo"] = "0"
    # Exclusão de um desligamento (saldo -1 no evento original -> +1 ajustado)
    exclusao_deslig = _registro(CE, CBO_ANALISTA, "3200,00", -1, tipo_mov="31")
    exclusao_deslig["indicadordeexclusão"] = "1"
    exclusao_deslig["competênciaexc"] = COMPETENCIA
    exclusoes.append(exclusao_deslig)

    _gravar_bronze(bronze, "mov", mov)
    _gravar_bronze(bronze, "for", fora_prazo)
    _gravar_bronze(bronze, "exc", exclusoes)
    return bronze


@pytest.fixture(scope="session")
def gold_db(bronze_sintetica: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Executa `dbt build` (seed + modelos + testes) contra a Bronze sintética."""
    pytest.importorskip("dbt.cli.main")
    workdir = tmp_path_factory.mktemp("dbt")
    db_path = workdir / "caged_test.duckdb"
    (workdir / "profiles.yml").write_text(
        "dbt_caged:\n"
        "  target: test\n"
        "  outputs:\n"
        "    test:\n"
        "      type: duckdb\n"
        f"      path: '{db_path.as_posix()}'\n"
        "      threads: 1\n",
        encoding="utf-8",
    )
    env = {
        **os.environ,
        "DBT_TARGET_PATH": str(workdir / "target"),
        "DBT_LOG_PATH": str(workdir / "logs"),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    resultado = subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "build",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(workdir),
            "--vars",
            f"{{bronze_path: '{bronze_sintetica.as_posix()}'}}",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        cwd=ROOT,
    )
    assert resultado.returncode == 0, resultado.stdout + resultado.stderr
    return db_path


def _consulta(db_path: Path, sql: str) -> list[tuple]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.execute(sql).fetchall()


# ------------------------------------------------------------------ dbt (Silver/Gold)
def test_exclusao_inverte_sinal(gold_db: Path) -> None:
    linhas = _consulta(
        gold_db,
        "SELECT saldo_movimentacao, saldo_ajustado, is_admissao_ativa, is_desligamento_ativo "
        "FROM gold.fato_movimentacoes_ti "
        "WHERE tipo_arquivo = 'EXC' AND saldo_movimentacao = 1",
    )
    assert linhas == [(1, -1, 0, 0)]


def test_exclusao_de_desligamento_gera_saldo_positivo_sem_contar_admissao(
    gold_db: Path,
) -> None:
    linhas = _consulta(
        gold_db,
        "SELECT saldo_movimentacao, saldo_ajustado, is_admissao_ativa, is_desligamento_ativo "
        "FROM gold.fato_movimentacoes_ti "
        "WHERE tipo_arquivo = 'EXC' AND saldo_movimentacao = -1",
    )
    assert linhas == [(-1, 1, 0, 0)]


def test_outlier_vira_null_mas_conta_no_saldo(gold_db: Path) -> None:
    salario_valido, salario_bruto, saldo, admissao = _consulta(
        gold_db,
        "SELECT salario_valido, salario_bruto, saldo_ajustado, is_admissao_ativa "
        "FROM gold.fato_movimentacoes_ti WHERE salario_bruto = 999999.00",
    )[0]
    assert salario_valido is None
    assert float(salario_bruto) == 999999.00
    assert saldo == 1
    assert admissao == 1

    (saldo_total,) = _consulta(
        gold_db,
        "SELECT SUM(saldo_liquido) FROM gold.agg_caged_ti_mensal "
        "WHERE recorte_geografico = 'Resto do Brasil' AND nivel_carreira = 'Gestão'",
    )[0]
    assert saldo_total == 1  # a linha outlier continua contando no saldo


def test_classificacao_geografica(gold_db: Path) -> None:
    recortes = dict(
        _consulta(
            gold_db,
            "SELECT uf_codigo, recorte_geografico FROM gold.fato_movimentacoes_ti "
            "GROUP BY 1, 2",
        )
    )
    assert recortes == {
        "23": "Ceará",
        "26": "Resto do Nordeste",
        "35": "Resto do Brasil",
    }


def test_agregado_reconcilia_com_fato(gold_db: Path) -> None:
    fato = _consulta(
        gold_db, "SELECT SUM(saldo_ajustado) FROM gold.fato_movimentacoes_ti"
    )[0][0]
    agg = _consulta(gold_db, "SELECT SUM(saldo_liquido) FROM gold.agg_caged_ti_mensal")[
        0
    ][0]
    assert fato == agg


def test_fronteiras_salariais_exatas(gold_db: Path) -> None:
    validos = dict(
        _consulta(
            gold_db,
            "SELECT CAST(salario_bruto AS DOUBLE), CAST(salario_valido AS DOUBLE) "
            "FROM gold.fato_movimentacoes_ti "
            "WHERE salario_bruto IN (700.00, 100000.00, 699.99)",
        )
    )
    assert validos == {700.00: 700.00, 100000.00: 100000.00, 699.99: None}


def test_linhas_identicas_nao_sao_deduplicadas(gold_db: Path) -> None:
    linhas, saldo = _consulta(
        gold_db,
        "SELECT COUNT(*), SUM(saldo_ajustado) FROM gold.fato_movimentacoes_ti "
        "WHERE uf_codigo = '26' AND salario_bruto = 4500.00",
    )[0]
    assert (linhas, saldo) == (2, 2)


def test_mediana_ignora_nulls_de_salario_valido(gold_db: Path) -> None:
    grupo = "uf_codigo = '35' AND cbo_ocupacao = '317105' AND is_admissao_ativa = 1"
    total, validos, mediana = _consulta(
        gold_db,
        "SELECT COUNT(*), COUNT(salario_valido), MEDIAN(salario_valido) "
        f"FROM gold.fato_movimentacoes_ti WHERE {grupo}",
    )[0]
    assert (total, validos) == (6, 4)
    assert float(mediana) == 3500.00

    (mediana_agg,) = _consulta(
        gold_db,
        "SELECT CAST(mediana_salario_admitidos AS DOUBLE) FROM gold.agg_caged_ti_mensal "
        "WHERE recorte_geografico = 'Resto do Brasil' AND nivel_carreira = 'Técnico'",
    )[0]
    assert mediana_agg == 3500.00


def _sql_reconciliacao() -> str:
    """Renderiza o teste singular do dbt trocando `ref()` pelos objetos do schema gold."""
    caminho = DBT_DIR / "tests" / "assert_reconciliacao_saldo_fato_vs_agg.sql"
    sql = re.sub(r"\{#.*?#\}", "", caminho.read_text(encoding="utf-8"), flags=re.DOTALL)
    return re.sub(r"\{\{\s*ref\('(\w+)'\)\s*\}\}", r"gold.\1", sql)


def test_reconciliacao_granular_sem_divergencias(gold_db: Path) -> None:
    assert _consulta(gold_db, _sql_reconciliacao()) == []


@pytest.mark.parametrize(
    "ajuste",
    [
        # divergências regionais que se compensam no total global
        (
            "UPDATE gold.agg_caged_ti_mensal SET saldo_liquido = saldo_liquido + 1 "
            "WHERE recorte_geografico = 'Ceará' AND nivel_carreira = 'Superior / Analista';"
            "UPDATE gold.agg_caged_ti_mensal SET saldo_liquido = saldo_liquido - 1 "
            "WHERE recorte_geografico = 'Resto do Brasil' AND nivel_carreira = 'Técnico'"
        ),
        # grupo ausente no agregado
        (
            "DELETE FROM gold.agg_caged_ti_mensal "
            "WHERE recorte_geografico = 'Resto do Nordeste'"
        ),
    ],
    ids=["divergencia_compensada", "grupo_ausente"],
)
def test_reconciliacao_granular_detecta_divergencias(
    gold_db: Path, tmp_path: Path, ajuste: str
) -> None:
    copia = tmp_path / "copia.duckdb"
    shutil.copy(gold_db, copia)
    with duckdb.connect(str(copia)) as con:
        con.execute(ajuste)
    assert _consulta(copia, _sql_reconciliacao()) != []


# ------------------------------------------------------------------ métricas e invariantes
def _janela_e_con(db_path: Path) -> tuple[duckdb.DuckDBPyConnection, tuple[str, str]]:
    con = duckdb.connect(str(db_path), read_only=True)
    return con, metricas.janela_analise(con)


def test_janela_de_analise_vem_da_cobertura_mov(gold_db: Path) -> None:
    con, (ini, fim) = _janela_e_con(gold_db)
    with con:
        competencias_gold = [
            r[0]
            for r in con.execute(
                "SELECT DISTINCT competencia_mov FROM gold.agg_caged_ti_mensal ORDER BY 1"
            ).fetchall()
        ]
    assert (ini, fim) == (COMPETENCIA, COMPETENCIA)
    # a retificação de 202412 existe na Gold, mas não pertence à janela analisada
    assert "202412" in competencias_gold
    assert all(c >= ini for c in competencias_gold if c != "202412")


def test_consolidado_ignora_registros_fora_da_janela(gold_db: Path) -> None:
    con, (ini, fim) = _janela_e_con(gold_db)
    with con:
        cons = metricas.consolidado(con, ini, fim)
        vol = metricas.volume(con, ini, fim)
        (adm_gold_total,) = con.execute(
            "SELECT SUM(is_admissao_ativa) FROM gold.fato_movimentacoes_ti"
        ).fetchone()
    assert cons["admissoes"].sum() == vol["admissoes_ativas"]
    assert (
        vol["admissoes_ativas"] == adm_gold_total - 1
    )  # exclui a retificação de 202412


def test_volume_nao_mistura_graos(gold_db: Path) -> None:
    """Registros = admissões ativas + desligamentos ativos + exclusões (critérios disjuntos)."""
    con, (ini, fim) = _janela_e_con(gold_db)
    with con:
        vol = metricas.volume(con, ini, fim)
    assert (
        vol["registros"]
        == vol["admissoes_ativas"] + vol["desligamentos_ativos"] + vol["exclusoes"]
    )
    assert vol["exclusoes"] == 2


def test_consolidado_reconcilia_com_a_fato_por_grupo(gold_db: Path) -> None:
    con, (ini, fim) = _janela_e_con(gold_db)
    with con:
        cons = metricas.consolidado(con, ini, fim)
        fato = {
            (r[0], r[1]): r[2:]
            for r in con.execute(
                "SELECT recorte_geografico, nivel_carreira, SUM(is_admissao_ativa), "
                "SUM(is_desligamento_ativo), SUM(saldo_ajustado) "
                "FROM gold.fato_movimentacoes_ti WHERE competencia_mov BETWEEN ? AND ? "
                "GROUP BY 1, 2",
                [ini, fim],
            ).fetchall()
        }
    assert len(cons) == len(fato)
    for r in cons.itertuples():
        assert fato[(r.recorte_geografico, r.nivel_carreira)] == (
            r.admissoes,
            r.desligamentos,
            r.saldo,
        )


def test_medianas_do_consolidado_conferem_com_calculo_independente(
    gold_db: Path,
) -> None:
    con, (ini, fim) = _janela_e_con(gold_db)
    with con:
        cons = metricas.consolidado(con, ini, fim)
        for r in cons.itertuples():
            for flag, valor in (
                ("is_admissao_ativa", r.med_entrou),
                ("is_desligamento_ativo", r.med_saiu),
            ):
                salarios = [
                    float(s[0])
                    for s in con.execute(
                        f"SELECT salario_valido FROM gold.fato_movimentacoes_ti "
                        f"WHERE {flag} = 1 AND salario_valido IS NOT NULL "
                        "AND competencia_mov BETWEEN ? AND ? "
                        "AND recorte_geografico = ? AND nivel_carreira = ?",
                        [ini, fim, r.recorte_geografico, r.nivel_carreira],
                    ).fetchall()
                ]
                esperado = statistics.median(salarios) if salarios else None
                assert (
                    (valor is None or math.isnan(valor))
                    if esperado is None
                    else valor == pytest.approx(esperado)
                )


def test_gap_do_consolidado_segue_a_formula(gold_db: Path) -> None:
    con, (ini, fim) = _janela_e_con(gold_db)
    with con:
        cons = metricas.consolidado(con, ini, fim)
    com_gap = cons.dropna(subset=["med_entrou", "med_saiu"])
    assert not com_gap.empty
    for r in com_gap.itertuples():
        assert r.gap_pct == pytest.approx((r.med_entrou / r.med_saiu - 1) * 100)


@pytest.mark.parametrize(
    ("entrada", "saida", "esperado"),
    [
        (90.0, 100.0, -10.0),
        (110.0, 100.0, 10.0),
        (Decimal("4500.00"), Decimal("4000.00"), 12.5),
        (100.0, 100.0, 0.0),
        (100.0, 0.0, None),
        (0.0, 0.0, None),
        (None, 100.0, None),
        (100.0, None, None),
        (None, None, None),
        (float("nan"), 100.0, None),
        (100.0, float("nan"), None),
        (float("nan"), float("nan"), None),
        (float("inf"), 100.0, None),
        (100.0, float("inf"), None),
        (float("-inf"), 100.0, None),
        (100.0, float("-inf"), None),
        (float("inf"), float("inf"), None),
    ],
)
def test_gap_percentual(
    entrada: float | None, saida: float | None, esperado: float | None
) -> None:
    resultado = metricas.gap_percentual(entrada, saida)
    assert resultado == (pytest.approx(esperado) if esperado is not None else None)


def test_dashboard_filtro_sem_desligamento_nao_exibe_nan(gold_db: Path) -> None:
    """Reproduz o filtro Ceará + Gestão em competência única sem desligamentos ativos."""
    from streamlit.testing.v1 import AppTest

    codigo = (ROOT / "src" / "app.py").read_text(encoding="utf-8")
    codigo = codigo.replace(
        'Path(__file__).resolve().parents[1] / "data" / "caged.duckdb"',
        f"Path({gold_db.as_posix()!r})",
    )
    at = AppTest.from_string(codigo, default_timeout=60).run()
    assert not at.exception

    # Filtra Ceará + Gestão (1 admissão de R$ 5.000,00 e 0 desligamentos -> med_saiu nula)
    at.sidebar.multiselect[0].set_value(["Ceará"])
    at.sidebar.multiselect[1].set_value(["Gestão"])
    at.run()

    assert not at.exception
    metricas_ui = {m.label: (m.value, m.delta) for m in at.metric}
    assert metricas_ui["Mediana Admitidos"] == ("R$ 5.000,00", "")
    assert metricas_ui["Mediana Desligados"] == ("—", "")
    for valor, delta in metricas_ui.values():
        texto = f"{valor} {delta or ''}".lower()
        assert "nan" not in texto
        assert "inf" not in texto


def test_markdown_de_metricas_usa_periodo_e_nomenclatura_precisa(gold_db: Path) -> None:
    con, (ini, fim) = _janela_e_con(gold_db)
    with con:
        texto = metricas.para_markdown(con, ini, fim)
    assert "01/2025 a 01/2025" in texto
    for rotulo in (
        "Registros na fato",
        "Admissões ativas",
        "Desligamentos ativos",
        "Registros de exclusão",
        "H1 — Frequência Mensal de Gaps Salariais por Competência",
        "H2 — Distribuição de `tipomovimentação` nas Admissões Ativas",
        "H3 — Diferencial Salarial Regional na Mediana de Admissão",
    ):
        assert rotulo in texto


def test_colunas_obsoletas_de_primeiro_emprego_foram_removidas(gold_db: Path) -> None:
    colunas_fato = {
        r[0]
        for r in _consulta(
            gold_db,
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'gold' AND table_name = 'fato_movimentacoes_ti'",
        )
    }
    colunas_agg = {
        r[0]
        for r in _consulta(
            gold_db,
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'gold' AND table_name = 'agg_caged_ti_mensal'",
        )
    }
    assert "is_primeiro_emprego" not in colunas_fato
    assert "admissoes_primeiro_emprego" not in colunas_agg
    assert {
        "tipo_movimentacao_codigo",
        "is_admissao_ativa",
        "is_desligamento_ativo",
        "indicador_exclusao",
        "saldo_ajustado",
    }.issubset(colunas_fato)
    assert {"admissoes_jovens_ate_24", "pct_admissoes_jovens_ate_24"}.issubset(
        colunas_agg
    )


def test_gaps_mensais_h1_classifica_indefinidos_sem_contar_como_negativos(
    gold_db: Path,
) -> None:
    con, (ini, fim) = _janela_e_con(gold_db)
    with con:
        h1 = metricas.gaps_mensais_h1(con, ini, fim)
        texto = metricas.para_markdown(con, ini, fim)
    assert len(h1) == 9
    for r in h1.itertuples():
        assert r.competencias_validas == r.gap_negativo + r.gap_zero + r.gap_positivo
        assert r.competencias_janela == r.competencias_validas + r.gap_indefinido

    # Ceará + Gestão tem apenas 1 admissão e 0 desligamentos em 202501 -> gap indefinido (nunca negativo)
    ce_gestao = h1[
        (h1["recorte_geografico"] == "Ceará") & (h1["nivel_carreira"] == "Gestão")
    ].iloc[0]
    assert int(ce_gestao["competencias_janela"]) == 1
    assert int(ce_gestao["competencias_validas"]) == 0
    assert int(ce_gestao["gap_indefinido"]) == 1
    assert int(ce_gestao["gap_negativo"]) == 0
    assert "0 de 0 válidos (1 na janela)" in texto


def test_tipos_movimentacao_inclui_codigos_de_referencia_10_20_35_97(
    gold_db: Path,
) -> None:
    con, (ini, fim) = _janela_e_con(gold_db)
    with con:
        tipos = metricas.tipos_movimentacao(con, ini, fim)
        vol = metricas.volume(con, ini, fim)
    assert {10, 20, 35, 97}.issubset(set(tipos["codigo"]))
    assert int(tipos["admissoes"].sum()) == vol["admissoes_ativas"]
    assert float(tipos["pct"].sum()) == pytest.approx(100.0)


def test_diferencial_regional_h3_segue_gap_percentual(gold_db: Path) -> None:
    con, (ini, fim) = _janela_e_con(gold_db)
    with con:
        h3 = metricas.diferencial_regional_h3(con, ini, fim)
    assert list(h3["nivel_carreira"]) == metricas.NIVEIS
    for r in h3.itertuples():
        esperado_ne = metricas.gap_percentual(r.med_ceara, r.med_resto_ne)
        esperado_br = metricas.gap_percentual(r.med_ceara, r.med_resto_br)
        assert (
            (r.gap_ce_vs_ne_pct is None or math.isnan(r.gap_ce_vs_ne_pct))
            if esperado_ne is None
            else r.gap_ce_vs_ne_pct == pytest.approx(esperado_ne)
        )
        assert (
            (r.gap_ce_vs_br_pct is None or math.isnan(r.gap_ce_vs_br_pct))
            if esperado_br is None
            else r.gap_ce_vs_br_pct == pytest.approx(esperado_br)
        )


# ------------------------------------------------------------------ extract_ftp.py
@pytest.fixture
def bronze_isolada(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(extract_ftp, "ROOT", tmp_path)
    monkeypatch.setattr(extract_ftp, "BRONZE_DIR", tmp_path / "data" / "bronze")
    monkeypatch.setattr(extract_ftp, "TMP_DIR", tmp_path / "data" / "tmp")
    return tmp_path


def _txt_valido(
    caminho: Path, tipo: str = "MOV", colunas_ausentes: tuple[str, ...] = ()
) -> Path:
    colunas = list(extract_ftp.COLUNAS_OBRIGATORIAS_BASE)
    if tipo == "EXC":
        colunas += list(extract_ftp.COLUNAS_OBRIGATORIAS_EXC)
    colunas = [c for c in colunas if c not in colunas_ausentes]
    linha = ";".join("1" for _ in colunas)
    caminho.write_text(f"{';'.join(colunas)}\n{linha}\n{linha}\n", encoding="utf-8")
    return caminho


def test_txt_para_parquet_escrita_atomica(tmp_path: Path) -> None:
    txt = _txt_valido(tmp_path / "CAGEDMOV202501.txt")
    saida = tmp_path / "out" / "competencia=202501" / "data.parquet"

    extract_ftp.txt_para_parquet(txt, saida, "202501", "MOV")

    assert saida.exists()
    assert not saida.with_suffix(".parquet.part").exists()
    with duckdb.connect() as con:
        linhas, comp, tipo = con.execute(
            "SELECT COUNT(*), MIN(_competencia_arquivo), MIN(_tipo_arquivo) "
            f"FROM read_parquet('{saida.as_posix()}')"
        ).fetchone()
    assert (linhas, comp, tipo) == (2, "202501", "MOV")


def test_falha_na_conversao_preserva_arquivo_existente(tmp_path: Path) -> None:
    saida = tmp_path / "out" / "data.parquet"
    saida.parent.mkdir(parents=True)
    saida.write_bytes(b"conteudo-original")
    txt = _txt_valido(tmp_path / "invalido.txt")
    with txt.open("ab") as f:
        f.write(b"\xff\xfe;2\n")  # bytes inválidos em UTF-8

    with pytest.raises(duckdb.Error):
        extract_ftp.txt_para_parquet(txt, saida, "202501", "MOV")

    assert saida.read_bytes() == b"conteudo-original"
    assert not saida.with_suffix(".parquet.part").exists()


def test_falha_apos_criar_parcial_remove_o_part(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    saida = tmp_path / "out" / "data.parquet"
    saida.parent.mkdir(parents=True)
    saida.write_bytes(b"conteudo-original")
    txt = _txt_valido(tmp_path / "valido.txt")
    parcial = saida.with_suffix(".parquet.part")

    class ConexaoQueFalha:
        def execute(self, *_args: object) -> None:
            parcial.write_bytes(b"lixo-parcial")
            raise RuntimeError("falha simulada no meio da escrita")

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        extract_ftp.duckdb, "connect", lambda *_a, **_k: ConexaoQueFalha()
    )

    with pytest.raises(RuntimeError, match="falha simulada"):
        extract_ftp.txt_para_parquet(txt, saida, "202501", "MOV")

    assert not parcial.exists()
    assert saida.read_bytes() == b"conteudo-original"


def test_schema_invalido_levanta_value_error(tmp_path: Path) -> None:
    txt = _txt_valido(tmp_path / "sem_salario.txt", colunas_ausentes=("salário",))
    saida = tmp_path / "out" / "data.parquet"

    with pytest.raises(ValueError, match="salário"):
        extract_ftp.txt_para_parquet(txt, saida, "202501", "MOV")

    assert not saida.exists()
    assert not saida.with_suffix(".parquet.part").exists()


def test_exc_exige_colunas_adicionais(tmp_path: Path) -> None:
    txt = _txt_valido(
        tmp_path / "exc.txt",
        tipo="EXC",
        colunas_ausentes=("indicadordeexclusão", "competênciaexc"),
    )

    with pytest.raises(ValueError, match="indicadordeexclusão.*competênciaexc"):
        extract_ftp.validar_colunas_txt(txt, "EXC")
    extract_ftp.validar_colunas_txt(txt, "MOV")  # MOV não exige as colunas de EXC


@pytest.mark.parametrize("competencia", ["202513", "202500", "2025", "20251", "abcdef"])
def test_validar_competencia_rejeita_formatos_invalidos(competencia: str) -> None:
    with pytest.raises(ValueError, match="Competência inválida"):
        extract_ftp.validar_competencia(competencia)


@pytest.mark.parametrize("competencia", ["202501", "202512"])
def test_validar_competencia_aceita_formato_valido(competencia: str) -> None:
    assert extract_ftp.validar_competencia(competencia) == competencia


def test_cli_rejeita_competencia_invalida_antes_de_conectar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def nao_deve_conectar() -> None:
        raise AssertionError("não deveria conectar ao FTP com argumentos inválidos")

    monkeypatch.setattr(extract_ftp, "conectar", nao_deve_conectar)
    monkeypatch.setattr(sys, "argv", ["extract_ftp.py", "--competencia", "202513"])

    with pytest.raises(SystemExit) as saida:
        extract_ftp.main()

    assert saida.value.code == 2


def test_baixar_arquivo_ausente_no_ftp_retorna_false(tmp_path: Path) -> None:
    ftp = MagicMock()
    ftp.nlst.return_value = ["CAGEDFOR202501.7z"]
    destino = tmp_path / "CAGEDMOV202501.7z"

    assert extract_ftp.baixar(ftp, "202501", "CAGEDMOV202501.7z", destino) is False
    ftp.retrbinary.assert_not_called()
    assert not destino.exists()


def test_baixar_pasta_ausente_no_ftp_retorna_false(tmp_path: Path) -> None:
    ftp = MagicMock()
    ftp.cwd.side_effect = error_perm("550 pasta inexistente")

    assert extract_ftp.baixar(ftp, "202501", "x.7z", tmp_path / "x.7z") is False
    ftp.nlst.assert_not_called()


def test_processar_com_arquivo_ausente_nao_grava_bronze(bronze_isolada: Path) -> None:
    ftp = MagicMock()
    ftp.nlst.return_value = []

    retorno = extract_ftp.processar(ftp, "202501", "MOV", force=False)

    assert retorno is ftp
    assert not any(extract_ftp.BRONZE_DIR.rglob("*.parquet*"))
    assert list(extract_ftp.TMP_DIR.iterdir()) == []


def test_processar_e_idempotente(bronze_isolada: Path) -> None:
    saida = extract_ftp.BRONZE_DIR / "caged_mov" / "competencia=202501" / "data.parquet"
    saida.parent.mkdir(parents=True)
    saida.write_bytes(b"ja-existe")
    ftp = MagicMock()

    retorno = extract_ftp.processar(ftp, "202501", "MOV", force=False)

    assert retorno is ftp
    ftp.nlst.assert_not_called()
    ftp.retrbinary.assert_not_called()
    assert saida.read_bytes() == b"ja-existe"


def _gerar_7z_em(destino: Path, pasta_aux: Path) -> None:
    txt = _txt_valido(pasta_aux / "CAGEDMOV202501.txt")
    with py7zr.SevenZipFile(destino, "w") as z:
        z.write(txt, txt.name)


def test_retry_ftp_cenario_a_falha_inicial_e_recuperacao(
    bronze_isolada: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cenário A: 1ª tentativa falha no download; 2ª reconecta e conclui."""
    ftp_1 = MagicMock(name="ftp_1")
    ftp_2 = MagicMock(name="ftp_2")
    pausas: list[int] = []
    chamadas_baixar = 0

    def baixar_falso(ftp: object, _comp: str, _nome: str, destino: Path) -> bool:
        nonlocal chamadas_baixar
        chamadas_baixar += 1
        if chamadas_baixar == 1:
            assert ftp is ftp_1
            raise OSError("queda de rede na 1ª tentativa")
        assert ftp is ftp_2
        _gerar_7z_em(destino, tmp_path)
        return True

    monkeypatch.setattr(extract_ftp, "baixar", baixar_falso)
    monkeypatch.setattr(extract_ftp, "conectar", lambda: ftp_2)
    monkeypatch.setattr(extract_ftp.time, "sleep", pausas.append)

    retorno = extract_ftp.processar(ftp_1, "202501", "MOV", force=False)

    assert retorno is ftp_2
    ftp_1.close.assert_called_once()
    assert pausas == [5]
    saida = extract_ftp.BRONZE_DIR / "caged_mov" / "competencia=202501" / "data.parquet"
    assert saida.exists()


def test_retry_ftp_cenario_b_reconexao_falha_e_terceira_funciona(
    bronze_isolada: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cenário B: 1ª falha no download; 2ª falha ao reconectar; 3ª reconecta e conclui."""
    ftp_1 = MagicMock(name="ftp_1")
    ftp_3 = MagicMock(name="ftp_3")
    pausas: list[int] = []
    tentativas_conexao = 0

    def conectar_falso() -> MagicMock:
        nonlocal tentativas_conexao
        tentativas_conexao += 1
        if tentativas_conexao == 1:
            raise OSError("servidor FTP indisponível na reconexão")
        return ftp_3

    def baixar_falso(ftp: object, _comp: str, _nome: str, destino: Path) -> bool:
        if ftp is ftp_1:
            raise OSError("queda no download inicial")
        assert ftp is ftp_3
        _gerar_7z_em(destino, tmp_path)
        return True

    monkeypatch.setattr(extract_ftp, "conectar", conectar_falso)
    monkeypatch.setattr(extract_ftp, "baixar", baixar_falso)
    monkeypatch.setattr(extract_ftp.time, "sleep", pausas.append)

    retorno = extract_ftp.processar(ftp_1, "202501", "MOV", force=False)

    assert retorno is ftp_3
    assert tentativas_conexao == 2
    ftp_1.close.assert_called_once()
    assert pausas == [5, 10]
    saida = extract_ftp.BRONZE_DIR / "caged_mov" / "competencia=202501" / "data.parquet"
    assert saida.exists()


def test_retry_ftp_cenario_c_tres_falhas_consecutivas_propaga_erro(
    bronze_isolada: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cenário C: 3 falhas consecutivas encerram na 3ª tentativa e propagam o erro."""
    ftp_1 = MagicMock(name="ftp_1")
    ftp_2 = MagicMock(name="ftp_2")
    pausas: list[int] = []
    conexoes = iter([ftp_2, OSError("reconexão falhou na 3ª tentativa")])

    def conectar_falso() -> MagicMock:
        item = next(conexoes)
        if isinstance(item, Exception):
            raise item
        return item

    def baixar_sempre_falha(_ftp: object, _c: str, _n: str, _d: Path) -> bool:
        raise OSError("timeout durante RETR")

    monkeypatch.setattr(extract_ftp, "conectar", conectar_falso)
    monkeypatch.setattr(extract_ftp, "baixar", baixar_sempre_falha)
    monkeypatch.setattr(extract_ftp.time, "sleep", pausas.append)

    with pytest.raises(OSError, match="reconexão falhou na 3ª tentativa") as exc_info:
        extract_ftp.processar(ftp_1, "202501", "MOV", force=False)

    assert any("Esgotadas 3 tentativas" in n for n in exc_info.value.__notes__)
    ftp_1.close.assert_called_once()
    ftp_2.close.assert_called_once()
    assert pausas == [5, 10]
    assert not any(extract_ftp.BRONZE_DIR.rglob("*.parquet*"))
    assert list(extract_ftp.TMP_DIR.iterdir()) == []


def test_retry_ftp_cenario_d_limpeza_de_part_e_conexao_em_download_incompleto(
    tmp_path: Path,
) -> None:
    """Cenário D: falha ou truncamento no download remove .part e não deixa .7z inválido."""
    ftp = MagicMock()
    ftp.nlst.return_value = ["CAGEDMOV202501.7z"]
    ftp.size.return_value = 100
    ftp.retrbinary.side_effect = lambda _cmd, callback, **_kw: callback(b"incompleto")
    destino = tmp_path / "CAGEDMOV202501.7z"

    with pytest.raises(OSError, match="Download incompleto"):
        extract_ftp.baixar(ftp, "202501", "CAGEDMOV202501.7z", destino)

    assert not destino.exists()
    assert not destino.with_suffix(".7z.part").exists()


def test_retry_ftp_cenario_e_erro_permanente_nao_provoca_retry(
    bronze_isolada: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cenário E: erro permanente (5xx error_perm) falha imediatamente sem novas tentativas."""
    ftp = MagicMock()
    pausas: list[int] = []
    reconectou = False

    def nao_reconectar() -> None:
        nonlocal reconectou
        reconectou = True
        raise AssertionError("não deveria tentar reconectar em erro permanente")

    def baixar_erro_permanente(_ftp: object, _c: str, _n: str, _d: Path) -> bool:
        raise error_perm("550 Permissão negada no arquivo")

    monkeypatch.setattr(extract_ftp, "conectar", nao_reconectar)
    monkeypatch.setattr(extract_ftp, "baixar", baixar_erro_permanente)
    monkeypatch.setattr(extract_ftp.time, "sleep", pausas.append)

    with pytest.raises(error_perm, match="550"):
        extract_ftp.processar(ftp, "202501", "MOV", force=False)

    assert not reconectou
    assert pausas == []
    assert list(extract_ftp.TMP_DIR.iterdir()) == []


@pytest.mark.parametrize(
    ("falhas_iniciais", "pausas_esperadas"),
    [
        ([], []),  # Cenário A: sucesso na 1ª tentativa
        ([OSError("timeout na 1ª conexão")], [5]),  # Cenário B: 1 falha + sucesso na 2ª
        (
            [OSError("queda 1"), EOFError("queda 2")],
            [5, 10],
        ),  # Cenário C: 2 falhas + sucesso na 3ª
    ],
    ids=["cenario_a_sucesso_imediato", "cenario_b_uma_falha", "cenario_c_duas_falhas"],
)
def test_conexao_inicial_ftp_cenarios_a_b_c_recuperacao(
    bronze_isolada: Path,
    monkeypatch: pytest.MonkeyPatch,
    falhas_iniciais: list[Exception],
    pausas_esperadas: list[int],
) -> None:
    """Cenários A, B e C: conexão inicial em main() aplica retry com backoff 5s/10s e fecha sockets falhos."""
    pausas: list[int] = []
    instancias_falhas: list[MagicMock] = []
    ftp_ok = MagicMock(name="ftp_ok")
    eventos = [*falhas_iniciais, ftp_ok]
    chamadas_processar: list[tuple[object, str, str, bool]] = []

    def criar_ftp(*_args: object, **_kwargs: object) -> MagicMock:
        item = eventos.pop(0)
        if isinstance(item, Exception):
            inst = MagicMock(name="ftp_falho")
            inst.login.side_effect = item
            instancias_falhas.append(inst)
            return inst
        return item

    def processar_stub(ftp: object, comp: str, tipo: str, force: bool) -> object:
        chamadas_processar.append((ftp, comp, tipo, force))
        return ftp

    monkeypatch.setattr(extract_ftp, "FTP", criar_ftp)
    monkeypatch.setattr(extract_ftp, "processar", processar_stub)
    monkeypatch.setattr(extract_ftp.time, "sleep", pausas.append)
    monkeypatch.setattr(
        sys, "argv", ["extract_ftp.py", "--competencia", "202501", "--tipos", "MOV"]
    )

    extract_ftp.main()

    assert eventos == []
    assert pausas == pausas_esperadas
    for inst in instancias_falhas:
        inst.close.assert_called_once()
    assert chamadas_processar == [(ftp_ok, "202501", "MOV", False)]
    ftp_ok.quit.assert_called_once()


def test_conexao_inicial_ftp_cenario_d_tres_falhas_consecutivas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cenário D: 3 falhas consecutivas na conexão inicial encerram sem 4ª tentativa e propagam erro com nota."""
    pausas: list[int] = []
    instancias: list[MagicMock] = []

    def criar_ftp(*_args: object, **_kwargs: object) -> MagicMock:
        inst = MagicMock(name=f"ftp_{len(instancias) + 1}")
        inst.login.side_effect = OSError(
            f"indisponível na tentativa {len(instancias) + 1}"
        )
        instancias.append(inst)
        return inst

    monkeypatch.setattr(extract_ftp, "FTP", criar_ftp)
    monkeypatch.setattr(extract_ftp.time, "sleep", pausas.append)
    monkeypatch.setattr(
        sys, "argv", ["extract_ftp.py", "--competencia", "202501", "--tipos", "MOV"]
    )

    with pytest.raises(OSError, match="indisponível na tentativa 3") as exc_info:
        extract_ftp.main()

    assert len(instancias) == 3
    assert pausas == [5, 10]
    for inst in instancias:
        inst.close.assert_called_once()
    assert any("Esgotadas 3 tentativas" in n for n in exc_info.value.__notes__)


def test_conexao_inicial_ftp_cenario_e_erro_permanente_sem_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cenário E: erro permanente (error_perm 5xx) na conexão inicial falha imediatamente sem retry."""
    pausas: list[int] = []
    instancias: list[MagicMock] = []

    def criar_ftp(*_args: object, **_kwargs: object) -> MagicMock:
        inst = MagicMock(name="ftp_perm")
        inst.login.side_effect = error_perm("530 Login incorrect")
        instancias.append(inst)
        return inst

    monkeypatch.setattr(extract_ftp, "FTP", criar_ftp)
    monkeypatch.setattr(extract_ftp.time, "sleep", pausas.append)
    monkeypatch.setattr(
        sys, "argv", ["extract_ftp.py", "--competencia", "202501", "--tipos", "MOV"]
    )

    with pytest.raises(error_perm, match="530"):
        extract_ftp.main()

    assert len(instancias) == 1
    assert pausas == []
    instancias[0].close.assert_called_once()


def test_baixar_cenario_f_falha_sem_arquivo_anterior_remove_part(
    tmp_path: Path,
) -> None:
    """Cenário F: falha durante download sem arquivo final anterior remove .part e não cria .7z."""
    ftp = MagicMock()
    ftp.nlst.return_value = ["CAGEDMOV202501.7z"]
    ftp.size.return_value = 100

    def retr_falho(_cmd: str, callback: object, **_kw: object) -> None:
        callback(b"bytes-parciais")  # type: ignore[operator]
        raise OSError("queda de conexão durante RETR")

    ftp.retrbinary.side_effect = retr_falho
    destino = tmp_path / "CAGEDMOV202501.7z"

    with pytest.raises(OSError, match="queda de conexão"):
        extract_ftp.baixar(ftp, "202501", "CAGEDMOV202501.7z", destino)

    assert not destino.exists()
    assert not destino.with_suffix(".7z.part").exists()


def test_baixar_cenario_g_falha_preserva_arquivo_final_preexistente(
    tmp_path: Path,
) -> None:
    """Cenário G: falha no download preserva integralmente o arquivo .7z válido preexistente e remove .part."""
    ftp = MagicMock()
    ftp.nlst.return_value = ["CAGEDMOV202501.7z"]
    ftp.size.return_value = 100

    def retr_falho(_cmd: str, callback: object, **_kw: object) -> None:
        callback(b"lixo-parcial-interrompido")  # type: ignore[operator]
        raise OSError("conexão perdida no meio do download")

    ftp.retrbinary.side_effect = retr_falho
    destino = tmp_path / "CAGEDMOV202501.7z"
    destino.write_bytes(b"7z-valido-original")

    with pytest.raises(OSError, match="conexão perdida"):
        extract_ftp.baixar(ftp, "202501", "CAGEDMOV202501.7z", destino)

    assert destino.exists()
    assert destino.read_bytes() == b"7z-valido-original"
    assert not destino.with_suffix(".7z.part").exists()


def test_baixar_cenario_h_download_completo_substitui_arquivo_anterior(
    tmp_path: Path,
) -> None:
    """Cenário H: download bem-sucedido substitui atomicamente o arquivo anterior e não deixa .part."""
    conteudo_novo = b"7z-novo-completo-verificado"
    ftp = MagicMock()
    ftp.nlst.return_value = ["CAGEDMOV202501.7z"]
    ftp.size.return_value = len(conteudo_novo)
    ftp.retrbinary.side_effect = lambda _cmd, callback, **_kw: callback(conteudo_novo)

    destino = tmp_path / "CAGEDMOV202501.7z"
    destino.write_bytes(b"7z-versao-anterior")

    assert extract_ftp.baixar(ftp, "202501", "CAGEDMOV202501.7z", destino) is True
    assert destino.exists()
    assert destino.read_bytes() == conteudo_novo
    assert not destino.with_suffix(".7z.part").exists()


def test_baixar_cenario_i_download_incompleto_preserva_arquivo_anterior(
    tmp_path: Path,
) -> None:
    """Cenário I: download com tamanho menor que SIZE lança OSError, remove .part e preserva .7z anterior."""
    ftp = MagicMock()
    ftp.nlst.return_value = ["CAGEDMOV202501.7z"]
    ftp.size.return_value = 200
    ftp.retrbinary.side_effect = lambda _cmd, callback, **_kw: callback(b"truncado")

    destino = tmp_path / "CAGEDMOV202501.7z"
    destino.write_bytes(b"7z-intacto-anterior")

    with pytest.raises(OSError, match="Download incompleto"):
        extract_ftp.baixar(ftp, "202501", "CAGEDMOV202501.7z", destino)

    assert destino.exists()
    assert destino.read_bytes() == b"7z-intacto-anterior"
    assert not destino.with_suffix(".7z.part").exists()
