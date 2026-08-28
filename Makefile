
.DEFAULT_GOAL := help
## help: Вывести список доступных команд
.PHONY: help
help:
	@echo "Используйте: make <цель>"
	@echo ""
	@echo "Доступные цели:"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""

# Директория, в которой хранятся исполняемые файлы проекта и зависимости, необходимые для сборки.
LOCAL_BIN := $(CURDIR)/bin

export PATH:=$(LOCAL_BIN):$(PATH)

.PHONY: install-task

install-task:  ## Установка утилиты Task
	@mkdir -p ./bin
	@GOBIN=$(PWD)/bin go install github.com/go-task/task/v3/cmd/task@latest

