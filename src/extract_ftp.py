"""Ingestão Bronze do Novo CAGED (FTP MTE -> .7z -> .txt -> Parquet).

Fluxo por competência (AAAAMM) e por tipo (MOV, FOR, EXC):
  1. Conecta ao FTP ftp.mtps.gov.br em modo passivo.
  2. Navega até /pdet/microdados/NOVO CAGED/<ANO>/<AAAAMM>/.
  3. Baixa CAGED<TIPO><AAAAMM>.7z para uma pasta temporária.
  4. Extrai o .txt com py7zr.
  5. Lê o .txt com DuckDB (all_varchar=True, utf-8) e grava Parquet em
     data/bronze/caged_<tipo>/competencia=<AAAAMM>/data.parquet
  6. Remove .7z e .txt temporários.

Uso:
  python src/extract_ftp.py --competencia 202501
  python src/extract_ftp.py --ano 2025
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import shutil
import tempfile
import time
from ftplib import FTP, Error, error_perm
from pathlib import Path

import duckdb
import py7zr

FTP_HOST = "ftp.mtps.gov.br"
FTP_BASE = "/pdet/microdados/NOVO CAGED"
TIPOS = ("MOV", "FOR", "EXC")
ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = ROOT / "data" / "bronze"
TMP_DIR = ROOT / "data" / "tmp"
MAX_TENTATIVAS = 3

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("extract_ftp")


def conectar() -> FTP:
    ftp = FTP(FTP_HOST, encoding="utf-8", timeout=120)
    ftp.login()  # login anônimo
    ftp.set_pasv(True)
    return ftp


def listar_competencias(ftp: FTP, ano: str) -> list[str]:
    ftp.cwd(f"{FTP_BASE}/{ano}")
    itens = [n.split("/")[-1] for n in ftp.nlst()]
    return sorted(c for c in itens if c.startswith(ano) and len(c) == 6 and c.isdigit())


def baixar(ftp: FTP, competencia: str, nome: str, destino: Path) -> bool:
    """Baixa `nome` para `destino`. Retorna False se o arquivo não existir no FTP."""
    caminho_remoto = f"{FTP_BASE}/{competencia[:4]}/{competencia}"
    try:
        ftp.cwd(caminho_remoto)
    except error_perm:
        log.warning("Pasta da competência não encontrada no FTP: %s", caminho_remoto)
        return False

    arquivos_na_pasta = [a.split("/")[-1] for a in ftp.nlst()]
    if nome not in arquivos_na_pasta:
        log.warning("Arquivo não encontrado no FTP: %s/%s", competencia, nome)
        return False

    tamanho = None
    try:
        ftp.voidcmd("TYPE I")  # Garante modo binário antes do SIZE
        tamanho = ftp.size(nome)
    except (Error, OSError) as e:
        # Se o servidor recusar SIZE, baixa mesmo assim sem validar tamanho prévio
        log.debug("SIZE indisponível para %s: %s", nome, e)

    if tamanho:
        log.info("Baixando %s (%.2f MB)...", nome, tamanho / (1024 * 1024))
    else:
        log.info("Baixando %s...", nome)

    with destino.open("wb") as f:
        ftp.retrbinary(f"RETR {nome}", f.write, blocksize=1024 * 1024)

    if tamanho and destino.stat().st_size != tamanho:
        raise OSError(
            f"Download incompleto de {nome}: esperado {tamanho}, obtido {destino.stat().st_size}"
        )
    return True


def txt_para_parquet(txt: Path, saida: Path, competencia: str, tipo: str) -> None:
    saida.parent.mkdir(parents=True, exist_ok=True)
    parcial = saida.with_suffix(".parquet.part")
    con = duckdb.connect()
    try:
        con.execute(f"""
            COPY (
                SELECT *,
                       '{competencia}'   AS _competencia_arquivo,
                       '{tipo}'          AS _tipo_arquivo,
                       '{txt.name}'      AS _arquivo_origem,
                       current_timestamp AS _ingested_at
                FROM read_csv(
                    '{txt.as_posix()}',
                    delim=';',
                    header=true,
                    all_varchar=true,
                    encoding='utf-8',
                    ignore_errors=false
                )
            ) TO '{parcial.as_posix()}' (FORMAT parquet, COMPRESSION zstd)
        """)
    finally:
        con.close()
    parcial.replace(saida)  # escrita atômica


def processar(ftp: FTP, competencia: str, tipo: str, force: bool) -> FTP:
    nome = f"CAGED{tipo}{competencia}.7z"
    saida = (
        BRONZE_DIR
        / f"caged_{tipo.lower()}"
        / f"competencia={competencia}"
        / "data.parquet"
    )

    if saida.exists() and not force:
        log.info("Já existe na Bronze, pulando: %s", saida.relative_to(ROOT))
        return ftp

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix=f"{tipo}{competencia}_", dir=TMP_DIR))
    try:
        arquivo_7z = tmp / nome
        for tentativa in range(1, MAX_TENTATIVAS + 1):
            try:
                if not baixar(ftp, competencia, nome, arquivo_7z):
                    return ftp
                break
            except Exception as e:
                log.warning(
                    "Falha (%s/%s) em %s: %s", tentativa, MAX_TENTATIVAS, nome, e
                )
                if tentativa == MAX_TENTATIVAS:
                    raise
                time.sleep(5 * tentativa)
                with contextlib.suppress(Error, OSError):
                    ftp.close()
                ftp = conectar()

        log.info("Extraindo %s...", nome)
        with py7zr.SevenZipFile(arquivo_7z, "r") as z:
            z.extractall(tmp)
        arquivo_7z.unlink()  # libera espaço em disco antes da conversão

        txts = [p for p in tmp.rglob("*") if p.suffix.lower() == ".txt"]
        if not txts:
            raise FileNotFoundError(f"Nenhum arquivo .txt encontrado dentro de {nome}")
        txt = txts[0]

        log.info("Convertendo %s -> %s", txt.name, saida.relative_to(ROOT))
        txt_para_parquet(txt, saida, competencia, tipo)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return ftp


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    grupo = parser.add_mutually_exclusive_group(required=True)
    grupo.add_argument(
        "--competencia", nargs="+", help="Uma ou mais competências AAAAMM (ex: 202501)"
    )
    grupo.add_argument(
        "--ano", help="Processa todas as competências disponíveis do ano"
    )
    parser.add_argument("--tipos", nargs="+", default=list(TIPOS), choices=TIPOS)
    parser.add_argument(
        "--force", action="store_true", help="Reprocessa mesmo se já existir"
    )
    args = parser.parse_args()

    ftp = conectar()
    try:
        competencias = args.competencia or listar_competencias(ftp, args.ano)
        for comp in competencias:
            if len(comp) != 6 or not comp.isdigit():
                parser.error(f"Competência inválida: {comp} (use AAAAMM)")
            for tipo in args.tipos:
                ftp = processar(ftp, comp, tipo, args.force)
    finally:
        try:
            ftp.quit()
        except (Error, OSError):
            ftp.close()
    log.info("Pipeline de ingestão Bronze concluído com sucesso.")


if __name__ == "__main__":
    main()
