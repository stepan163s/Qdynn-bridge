# Совместимость клиентов

Qdynn-Bridge — серверная часть Telegram WEB Proxy. Требуется клиент с WEB proxy adapter: обычная поддержка SOCKS5 или классического MTProxy не обеспечивает работу с этим сервером.

## Контракт

- Клиент формирует bridge URL из hostname и MTProxy secret, создаёт WebView и передаёт бинарные кадры v1.
- Android bridge ожидает `TelegramWebProxy` и одноразовый nonce в fragment `#android=...`.
- MessagePort bridge принимает инициализацию от loopback HTTP-parent с точным Origin и одним переданным портом.
- При recovery старые логические потоки закрываются; клиент должен открыть их заново. Второго WELCOME в том же native channel нет.
- Возобновление после уничтожения WebView/процесса обеспечивает native client, а не серверный JavaScript.

См. [протокол](protocol.md) и исходные материалы Telegram: [tproxy-server](https://github.com/telegramdesktop/tproxy-server), [Android](https://github.com/telegramdesktop/tproxy-server/blob/master/ANDROID.md), [iOS](https://github.com/telegramdesktop/tproxy-server/blob/master/IOS.md).

## Что проверено

Локальные Rust/Node проверки покрывают wire contract, HTTP/WS, lifecycle, fallback и restart recovery. Node VM моделирует native adapter; это не реальный Android WebView или WKWebView. Этот репозиторий не содержит APK, iOS-приложения или заявленной матрицы совместимых выпусков Telegram.

Перед рабочим использованием зафиксируйте точную версию клиента и WebView и проверьте первое подключение, сообщения, передачу файла, смену Wi-Fi/мобильной сети, фон/возврат и рестарт сервера. Для `auto` дополнительно проверьте отказ WS при доступном HTTPS. Не включайте `auto` сразу для всех клиентов только на основании локального теста.
