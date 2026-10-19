# Разработка и тестирование

## Структура

- `crates/qdynn-proto`: wire frames, кодек и capability.
- `crates/qdynn-core`: сессии, потоки, очереди, backend TCP и метрики.
- `crates/qdynn-server`: HTTP/WS, JS bridge, конфигурация и диагностика.
- `tests/`: локальные сценарии JS, бинарника, TLS и установки.
- `deploy/`: минимальная конфигурация и эксплуатационные шаблоны.

## Один запуск проверок

```sh
bash scripts/check.sh
```

Нужны Rust с rustfmt/Clippy, Node.js 22+, Python 3 и OpenSSL. `Cargo.lock` хранится в репозитории, все Cargo-команды используют `--locked`. Полная Rust suite включает примерно 25 секунд ожидания idle poll. Smoke tests открывают только временные loopback listeners, используют синтетические credentials и удаляют временные файлы/процессы. TLS smoke создаёт одноразовый сертификат в temp directory.

CI использует ту же команду на Linux. Node harness моделирует WebView/native adapter; он не доказывает совместимость конкретной мобильной сборки. Deployment smoke работает с временным деревом файлов и не запускает systemd. Серверная приёмка и Android-проверка выполняются отдельно.

Для release-сборки:

```sh
cargo build --release --locked -p qdynn-server
python3 tests/smoke-binary.py target/release/qdynn-bridge
node tests/bridge-recovery-interop.cjs target/release/qdynn-bridge --runtime-fallback
python3 tests/doctor-smoke.py target/release/qdynn-bridge
```

## Изменения

Сохраняйте порядок кадров и idempotency при retry. Проверяйте ограниченность очередей, освобождение backend-сокетов и отсутствие повторного старого DATA при recovery. Новые transport modes не включайте по умолчанию без клиентской проверки. При изменении wire contract обновляйте [protocol.md](protocol.md).

Измерения производительности должны сравнивать одинаковую нагрузку и настройки. Ускорение отдельной функции или уменьшение копирования не является доказательством роста Telegram throughput либо превосходства над другой реализацией.

## Что не коммитить

Реальные конфиги, `.env`, ключи/сертификаты, логи, дампы, бинарники и частные серверные заметки исключены через `.gitignore`. `.local/` предназначен для личных материалов. `Cargo.lock`, тесты и placeholder-конфиги должны оставаться в Git. `.gitignore` не удаляет уже закоммиченные данные из истории: перед первой публикацией проверяйте весь набор отправляемых commits, а не только HEAD.
