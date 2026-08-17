SHELL   := /bin/bash
VENV    := .venv

ifeq ($(OS),Windows_NT)
    BIN := Scripts
    PYTHON_CMD := python
else
    BIN := bin
    PYTHON_CMD := python3
endif

PY      := $(VENV)/$(BIN)/python
PIP     := $(VENV)/$(BIN)/pip
DBT     := $(VENV)/$(BIN)/dbt


export LAB17_DB := $(CURDIR)/warehouse.duckdb
export DBT_PROFILES_DIR := $(CURDIR)/dbt
export PYTHONUTF8 := 1
export PYTHONIOENCODING := utf-8

.DEFAULT_GOAL := help
.PHONY: help setup seed seed-extra pipeline verify quick explain plan dbt-test \
        dbt-docs crash-test compact reset clean

help:  ## danh sách lệnh
	@echo ""
	@echo "  LAB 17 — Data Pipeline Engineering"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "    \033[36m%-14s\033[0m %s\n", $$1, $$2}'
	@echo ""

setup:  ## venv + thư viện + sinh dữ liệu (chạy một lần)
	@test -d $(VENV) || $(PYTHON_CMD) -m venv $(VENV)
	@$(PY) -m pip install -q --upgrade pip
	@$(PIP) install -q -r requirements.txt
	@$(PY) seed/generate.py
	@echo ""
	@echo "  xong. Bước tiếp theo:  make pipeline  rồi  make verify"

seed:  ## sinh lại dữ liệu seed
	@$(PY) seed/generate.py

seed-extra:  ## sinh thêm dữ liệu cho bài mở rộng trong EXTRA.md (~30 giây)
	@$(PY) seed/generate.py --extra
	@$(PY) tools/explain.py --save-baseline

pipeline:  ## chạy đường ống một lượt (14 ngày vận hành)
	@$(PY) tools/run_pipeline.py

verify:  ## ⭐ xoá kho, chạy 3 lượt, in bảng chấm — dùng lệnh này liên tục
	@$(PY) tools/verify.py

quick:  ## như verify nhưng chỉ 1 lượt (nhanh, không kiểm tra tính ổn định)
	@$(PY) tools/verify.py --runs 1

explain:  ## [mở rộng] đo rows scanned của queries/dashboard.sql
	@$(PY) tools/explain.py

plan:  ## [mở rộng] explain + in cây EXPLAIN ANALYZE
	@$(PY) tools/explain.py --plan

compact:  ## [mở rộng] chạy tools/compact.py
	@$(PY) tools/compact.py

dbt-test:  ## chạy dbt test
	@cd dbt && ../$(DBT) test --profiles-dir . --target-path target --log-path logs

dbt-docs:  ## dựng và mở tài liệu dbt (tuỳ chọn)
	@cd dbt && ../$(DBT) docs generate --profiles-dir . --target-path target --log-path logs \
	  && ../$(DBT) docs serve --profiles-dir . --target-path target

crash-test:  ## [mở rộng] kịch bản consumer bị giết giữa batch
	@$(PY) tools/crash_test.py

reset:  ## xoá kho DuckDB (giữ nguyên seed và data/)
	@rm -f warehouse.duckdb warehouse.duckdb.wal
	@echo "  kho đã xoá."

clean:  ## xoá kho + target dbt + thư mục làm việc của crash-test
	@rm -rf warehouse.duckdb warehouse.duckdb.wal dbt/target dbt/logs data/crash
	@echo "  đã dọn."
