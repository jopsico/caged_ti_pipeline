"""Testes do pipeline CAGED TI com dados sintéticos (Bronze -> dbt Silver/Gold)."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from ftplib import error_perm
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
    fora_prazo = [_registro(SP, CBO_ANALISTA, "6000,00", 1)]
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
