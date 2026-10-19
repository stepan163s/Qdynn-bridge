# Qdynn-Bridge

**Telegram WEB Proxy на Rust для одного сервера: HTTPS/WebSocket, восстановление соединений и простая эксплуатация.**

Проект основан на архитектуре и протоколе [telegramdesktop/tproxy-server](https://github.com/telegramdesktop/tproxy-server). Qdynn-Bridge принимает трафик из WebView клиента и передаёт его локальному официальному MTProxy. TLS обслуживает Nginx или Caddy.

```text
Telegram с поддержкой WEB Proxy
        │ HTTPS / WSS
        ▼
  Nginx или Caddy :443
        │ loopback
        ▼
     Qdynn-Bridge
        │ loopback TCP
        ▼
  Официальный MTProxy → Telegram
```

> Нужна сборка Telegram с поддержкой **WEB Proxy**. Обычной поддержки SOCKS5 или MTProxy недостаточно. Клиентские приложения не входят в этот репозиторий. См. [совместимость клиентов](docs/clients.md).

## Возможности

- Четыре транспорта: WebSocket, HTTPS и их варианты с отдельными полосами для потоков.
- Опциональный `auto`: WS → HTTPS при неудачном первоначальном открытии WS или открытии WS во время восстановления.
- Ограниченные по времени startup/recovery, отмена зависших запросов и защита от старых callbacks.
- Восстановление после рестарта relay без повторной передачи старых DATA и второго WELCOME.
- Ограниченные очереди, управление окном, справедливое обслуживание потоков и отдельный резерв для управляющих кадров.
- Свой статический публичный сайт, приватные health/metrics endpoints, диагностика `doctor`.
- Минимальный конфиг, systemd unit, шаблон Nginx, обновление и откат бинарника.

Режим по умолчанию — `websocket`. Recovery и `auto` проверены локальными тестами; поведение конкретной Android/WebView сборки нужно проверить отдельно. Сравнительная скорость с официальным сервером и устойчивость к конкретному DPI не измерены. Полная блокировка единственного IP/domain не устраняется сменой транспорта на том же адресе.

## Что потребуется

- Linux-сервер и домен с действующим TLS-сертификатом.
- Nginx или Caddy на внешнем HTTPS-порту; relay и MTProxy доступны только по loopback.
- Работающий официальный MTProxy и его секрет.
- Rust/Cargo для сборки. В Dockerfile используется Rust 1.94; минимальная поддерживаемая версия отдельно не зафиксирована.
- Python 3 только для `doctor`. Для работы самого relay Python не нужен.

## Быстрый запуск

Соберите бинарник на целевой ОС и архитектуре:

```sh
git clone https://github.com/stepan163s/Qdynn-bridge.git
cd Qdynn-bridge
cargo build --release --locked -p qdynn-server

cp deploy/config.minimal.toml config.toml
chmod 600 config.toml
```

В `config.toml` укажите свой домен, **существующий секрет локального MTProxy**, его loopback-адрес и каталог собственного сайта с `index.html`:

```toml
hostname = "proxy.example.com"
secret = "REPLACE_WITH_YOUR_MTPROXY_SECRET"
backend = "127.0.0.1:2398"
carrier_mode = "websocket"
public_dir = "/var/www/qdynn"
```

Секрет — 16 байт либо `dd` + 16 байт в hex/base64url. Тестовые значения из исходников для сервера не используйте. `config.toml` исключён из Git.

```sh
./target/release/qdynn-bridge check-config --config-file config.toml
./target/release/qdynn-bridge --config-file config.toml
```

По умолчанию relay слушает `127.0.0.1:8080`. Подключите к нему HTTPS reverse proxy, сохранив `Host`, WS Upgrade и заголовки bridge. Готовый [шаблон Nginx](deploy/nginx.conf.example) включается вручную после проверки `nginx -t`.

Для фоновой работы через systemd, обновления и отката используйте [инструкцию установки](deploy/README.md). Установщик не настраивает MTProxy, DNS и сертификаты и не перезаписывает действующий Nginx.

## Проверка сервера и ссылка подключения

```sh
./target/release/qdynn-bridge doctor --config-file config.toml
./target/release/qdynn-bridge --config-file config.toml --print-link
```

`doctor` проверяет backend TCP, DNS/TLS/HTTPS, bridge headers, HELLO/WELCOME и WS Upgrade; завершает работу с ненулевым кодом при ошибке. Проверка ограничена 90 секундами и создаёт временные сессии. Она не заменяет проверку сообщений в Telegram с телефона.

`--print-link` выдаёт ссылку и QR-код и завершает процесс. **Вывод содержит секрет** — не публикуйте его в Issues или общих логах.

## Настройки

CLI-параметры и переменные окружения переопределяют TOML. Все лимиты и тайм-ауты имеют defaults; для обычной установки достаточно минимального конфига.

| Настройка | По умолчанию | Назначение |
|---|---|---|
| `listen` | `127.0.0.1:8080` | Внутренний HTTP/WS listener |
| `backend` | `127.0.0.1:2398` | Локальный MTProxy |
| `carrier_mode` | `websocket` | `websocket`, `https`, `websocket-lanes`, `https-lanes`, `auto` |
| `startup_timeout_ms` | `15000` | Общий startup/recovery budget |
| `websocket_open_timeout_ms` | `4000` | Максимальное ожидание открытия WS |
| `request_timeout_ms` | `90000` | Deadline HTTP-запроса и контроль зависшей WS-отправки |
| `admin_listen` | выключен | Приватные `/healthz`, `/readyz`, `/metrics` |
| `public_dir` | встроенная страница | Каталог своего статического сайта |

Полный список: [config.example.toml](config.example.toml) и `qdynn-bridge --help`. `auto` включайте после проверки целевого клиента. После fallback bridge остаётся на HTTPS до нового запуска; резервные домены и обучение не требуются.

## Docker

Для Linux, если MTProxy работает на хосте, используйте host networking и явно оставьте listener на loopback:

```sh
docker build -t qdynn-bridge .
docker run -d --name qdynn --network host --restart unless-stopped \
  --mount type=bind,src="$PWD/config.toml",dst=/etc/qdynn/config.toml,readonly \
  --mount type=bind,src=/var/www/qdynn,dst=/var/www/qdynn,readonly \
  qdynn-bridge --config-file /etc/qdynn/config.toml --listen 127.0.0.1:8080
```

В минимальном runtime image нет Python: `doctor` запускайте на хосте с подходящим бинарником. Конфигурация и сертификаты не включаются в Docker build context.

## Проверки и документация

```sh
bash scripts/check.sh
```

Команда выполняет fmt, Clippy, Rust-тесты, JS-regressions, build и локальные HTTP/WS/TLS smoke tests. Потребуются Rust, Node.js 22+, Python 3 и OpenSSL. Подробности и ограничения — [разработка и тестирование](docs/development.md).

- [Установка, диагностика, обновление и откат](deploy/README.md)
- [Архитектура](docs/architecture.md)
- [Протокол и recovery](docs/protocol.md)
- [Совместимость клиентов](docs/clients.md)
- [Безопасность и сообщения об уязвимостях](SECURITY.md)
- [Происхождение кода и сторонние условия](THIRD_PARTY_NOTICES.md)

## Лицензирование

Собственный код Qdynn-Bridge — [MIT](LICENSE). Эта лицензия не заменяет условия авторов заимствованных частей. Происхождение bridge и незавершённая проверка лицензии исходного `tproxy-server` описаны в [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). Qdynn-Bridge не является официальным продуктом Telegram.
