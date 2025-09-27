#!/bin/sh
# Этот скрипт будет запускаться перед каждым коммитом.
# Если он завершится с ошибкой, коммит будет отменен.

echo "Running pre-commit hook..."

# 1. Проверка форматирования
echo "Checking formatting with 'cargo fmt'..."
cargo fmt --check
if [ $? -ne 0 ]; then
    echo "Error: Code is not formatted. Please run 'cargo fmt'."
    exit 1
fi

# 2. Проверка линтером
echo "Running linter with 'cargo clippy'..."
cargo clippy -- -D warnings
if [ $? -ne 0 ]; then
    echo "Error: Clippy found issues. Please fix them."
    exit 1
fi

echo "Pre-commit checks passed."
exit 0```

**3. Создайте файл `scripts/setup-hooks.sh`:**
```bash
touch scripts/setup-hooks.sh