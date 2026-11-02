PYTHON ?= py
DBT_DIR := dbt_caged
DBT := cd $(DBT_DIR) && $(PYTHON) -m dbt.cli.main
COMP ?= 202601

.PHONY: help setup extract seed run test build docs pipeline clean

help:
	@echo "setup    - instala dependencias do requirements.txt"
	@echo "extract  - baixa Novo CAGED do FTP e grava Parquet na Bronze (COMP=AAAAMM)"
	@echo "seed     - carrega seed_cbo_ti.csv no DuckDB"
	@echo "run      - executa modelos Silver e Gold no dbt"
	@echo "test     - executa testes de qualidade de dados no dbt"
	@echo "build    - executa seed + run + test no dbt"
	@echo "pipeline - executa extract + build completo"

setup:
	$(PYTHON) -m pip install -r requirements.txt

extract:
	$(PYTHON) src/extract_ftp.py --competencia $(COMP)

seed:
	$(DBT) seed --profiles-dir .

run:
	$(DBT) run --profiles-dir .

test:
	$(DBT) test --profiles-dir .

build:
	$(DBT) build --profiles-dir .

docs:
	$(DBT) docs generate --profiles-dir . && $(DBT) docs serve --profiles-dir .

pipeline: extract build

clean:
	$(DBT) clean --profiles-dir .