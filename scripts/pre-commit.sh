#!/bin/sh
# Этот скрипт устанавливает pre-commit хук в локальный .git репозиторий.
# Он создает символическую ссылку, чтобы хук всегда был актуальным.

# Путь к нашему скрипту
HOOK_SOURCE="scripts/pre-commit.sh"
# Путь к хуку в .git директории
HOOK_DEST=".git/hooks/pre-commit"

echo "Installing Git pre-commit hook..."
# Создаем символическую ссылку. Ключ -f перезаписывает существующую ссылку, если она есть.
ln -s -f ../../$HOOK_SOURCE $HOOK_DEST

echo "Git pre-commit hook installed successfully."