"""Testes do pipeline CAGED TI com dados sintéticos (Bronze -> dbt Silver/Gold)."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pandas as pd
import pytest

import extract_ftp

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt_caged"
COMPETENCIA = "202501"

CBO_GESTAO = "142505"
CBO_ANALISTA = "212205"

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
) -> dict[str, str]:
    uf, regiao = uf_regiao
    return {
        "competênciamov": COMPETENCIA,
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
    ]
    fora_prazo = [_registro(SP, CBO_ANALISTA, "6000,00", 1)]
    exclusoes = [_registro(CE, CBO_ANALISTA, "3500,00", 1, tipo_mov="20")]
    exclusoes[0]["indicadordeexclusão"] = "1"
    exclusoes[0]["competênciaexc"] = COMPETENCIA
    exclusoes[0]["origemdainformação"] = "2"
    exclusoes[0]["indicadordeforadoprazo"] = "0"

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
        "FROM gold.fato_movimentacoes_ti WHERE tipo_arquivo = 'EXC'",
    )
    assert linhas == [(1, -1, 0, 0)]


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
        "WHERE recorte_geografico = 'Resto do Brasil'",
    )[0]
    assert saldo_total == 2  # outlier (MOV) + admissão FOR


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


# ------------------------------------------------------------------ extract_ftp.py
@pytest.fixture
def bronze_isolada(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(extract_ftp, "ROOT", tmp_path)
    monkeypatch.setattr(extract_ftp, "BRONZE_DIR", tmp_path / "data" / "bronze")
    monkeypatch.setattr(extract_ftp, "TMP_DIR", tmp_path / "data" / "tmp")
    return tmp_path


def test_txt_para_parquet_escrita_atomica(tmp_path: Path) -> None:
    txt = tmp_path / "CAGEDMOV202501.txt"
    txt.write_text("uf;salário\n23;5000,00\n26;4000,00\n", encoding="utf-8")
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
    txt = tmp_path / "invalido.txt"
    txt.write_bytes(b"a;b\n\xff\xfe;2\n")  # bytes inválidos em UTF-8

    with pytest.raises(duckdb.Error):
        extract_ftp.txt_para_parquet(txt, saida, "202501", "MOV")

    assert saida.read_bytes() == b"conteudo-original"


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
