// Bridge HTML/CSP follows telegramdesktop/tproxy-server; see THIRD_PARTY_NOTICES.md.
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::RngCore;

pub fn generate_nonce() -> String {
    let mut bytes = [0u8; 18];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

pub const PERMISSIONS_POLICY: &str = "accelerometer=(), autoplay=(), camera=(), clipboard-read=(), clipboard-write=(), display-capture=(), encrypted-media=(), fullscreen=(), geolocation=(), gyroscope=(), hid=(), idle-detection=(), magnetometer=(), microphone=(), midi=(), payment=(), picture-in-picture=(), publickey-credentials-create=(), publickey-credentials-get=(), screen-wake-lock=(), serial=(), usb=(), web-share=(), xr-spatial-tracking=()";

pub struct BridgePageSettings<'a> {
    pub hostname: &'a str,
    pub bridge_capability: &'a str,
    pub bootstrap_token: &'a str,
    pub nonce: &'a str,
    pub carrier_mode: &'a str,
    pub startup_timeout_ms: u64,
    pub websocket_open_timeout_ms: u64,
    pub request_timeout_ms: u64,
}

/// Builds the dynamic secure bridge HTML document matching official Telegram tproxy-server.
pub fn render_bridge_page(settings: BridgePageSettings<'_>) -> String {
    let host_json = serde_json::to_string(&format!("https://{}", settings.hostname)).unwrap();
    let capability_json = serde_json::to_string(settings.bridge_capability).unwrap();
    let token_json = serde_json::to_string(settings.bootstrap_token).unwrap();
    let carrier_json = serde_json::to_string(settings.carrier_mode).unwrap();
    let batch_limit_str = "2097152";
    let bridge_script = include_str!("bridge.js");
    let startup_timeout = settings.startup_timeout_ms.to_string();
    let websocket_timeout = settings.websocket_open_timeout_ms.to_string();
    let request_timeout = settings.request_timeout_ms.to_string();

    let template = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Connection</title>
</head>
<body>
<script nonce="__NONCE__">
__BRIDGE_SCRIPT__
</script>
</body>
</html>"##;

    template
        .replace("__BRIDGE_SCRIPT__", bridge_script)
        .replace("__NONCE__", settings.nonce)
        .replace("__ORIGIN__", &host_json)
        .replace("__BRIDGE_CAPABILITY__", &capability_json)
        .replace("__BOOTSTRAP__", &token_json)
        .replace("__CARRIER_MODE__", &carrier_json)
        .replace("__BATCH_LIMIT__", batch_limit_str)
        .replace("__STARTUP_TIMEOUT_MS__", &startup_timeout)
        .replace("__WEBSOCKET_OPEN_TIMEOUT_MS__", &websocket_timeout)
        .replace("__REQUEST_TIMEOUT_MS__", &request_timeout)
}

/// Rich, modern, realistic default camouflage landing website.
pub fn render_camouflage_page(hostname: &str) -> String {
    let template = r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{HOSTNAME}} &bull; Cloud Infrastructure & Edge Network</title>
    <style>
        :root {
            --bg: #0b0f19;
            --surface: #131b2e;
            --border: #1e293b;
            --primary: #38bdf8;
            --text: #f8fafc;
            --muted: #94a3b8;
            --accent: #10b981;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background-color: var(--bg);
            color: var(--text);
            line-height: 1.6;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
        }
        header {
            border-bottom: 1px solid var(--border);
            padding: 18px 32px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: rgba(11, 15, 25, 0.8);
            backdrop-filter: blur(12px);
        }
        .logo {
            font-weight: 700;
            font-size: 18px;
            letter-spacing: -0.02em;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .logo-icon {
            width: 14px;
            height: 14px;
            border-radius: 50%;
            background: var(--accent);
            box-shadow: 0 0 12px var(--accent);
        }
        nav a {
            color: var(--muted);
            text-decoration: none;
            margin-left: 24px;
            font-size: 14px;
            transition: color 0.2s;
        }
        nav a:hover {
            color: var(--text);
        }
        main {
            flex: 1;
            max-width: 1080px;
            margin: 0 auto;
            padding: 64px 24px;
            width: 100%;
        }
        .hero {
            text-align: center;
            margin-bottom: 64px;
        }
        .status-pill {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            background: rgba(16, 185, 129, 0.1);
            color: var(--accent);
            border: 1px solid rgba(16, 185, 129, 0.2);
            padding: 6px 14px;
            border-radius: 9999px;
            font-size: 13px;
            font-weight: 500;
            margin-bottom: 24px;
        }
        h1 {
            font-size: 42px;
            font-weight: 800;
            letter-spacing: -0.03em;
            margin-bottom: 20px;
            background: linear-gradient(135deg, #ffffff 0%, #94a3b8 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .subtitle {
            font-size: 18px;
            color: var(--muted);
            max-width: 640px;
            margin: 0 auto;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 24px;
            margin-bottom: 48px;
        }
        .card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 32px;
            transition: transform 0.2s, border-color 0.2s;
        }
        .card:hover {
            transform: translateY(-2px);
            border-color: rgba(56, 189, 248, 0.3);
        }
        .card h3 {
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 12px;
            color: #ffffff;
        }
        .card p {
            color: var(--muted);
            font-size: 14px;
            line-height: 1.6;
        }
        .metrics {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 24px;
            display: flex;
            justify-content: space-around;
            flex-wrap: wrap;
            gap: 20px;
            text-align: center;
        }
        .metric-value {
            font-size: 28px;
            font-weight: 700;
            color: var(--primary);
        }
        .metric-label {
            font-size: 13px;
            color: var(--muted);
            margin-top: 4px;
        }
        footer {
            border-top: 1px solid var(--border);
            padding: 24px 32px;
            text-align: center;
            font-size: 13px;
            color: var(--muted);
        }
    </style>
</head>
<body>
    <header>
        <div class="logo">
            <div class="logo-icon"></div>
            <span>Cloud Edge &bull; {{HOSTNAME}}</span>
        </div>
        <nav>
            <a href="#services">Services</a>
            <a href="#status">Network Status</a>
            <a href="/robots.txt">Robots</a>
        </nav>
    </header>

    <main>
        <div class="hero">
            <div class="status-pill">
                <span>All Global Edge Points Operational</span>
            </div>
            <h1>Next-Generation Cloud Gateway</h1>
            <p class="subtitle">Distributed low-latency content acceleration, secure application transport and DDoS mitigation across global points of presence.</p>
        </div>

        <div class="grid" id="services">
            <div class="card">
                <h3>Edge Content Delivery</h3>
                <p>Intelligent routing with sub-millisecond edge caching and real-time asset optimization for modern web applications.</p>
            </div>
            <div class="card">
                <h3>Secure Gateway & TLS 1.3</h3>
                <p>Hardware-accelerated cryptographic processing, HTTP/2 multiplexing, and zero-trust perimeter isolation.</p>
            </div>
            <div class="card">
                <h3>Real-time WebSockets</h3>
                <p>Persistent bidirectional streaming channels engineered for high-concurrency microservices and telemetry.</p>
            </div>
        </div>

        <div class="metrics" id="status">
            <div>
                <div class="metric-value">99.99%</div>
                <div class="metric-label">Uptime SLA</div>
            </div>
            <div>
                <div class="metric-value">&lt; 1 ms</div>
                <div class="metric-label">Edge Latency</div>
            </div>
            <div>
                <div class="metric-value">HTTP/2</div>
                <div class="metric-label">Active Protocol</div>
            </div>
            <div>
                <div class="metric-value">Healthy</div>
                <div class="metric-label">Node Status</div>
            </div>
        </div>
    </main>

    <footer>
        &copy; 2026 {{HOSTNAME}} &bull; Enterprise Cloud Edge Gateway &bull; All systems normal
    </footer>
</body>
</html>"##;

    template.replace("{{HOSTNAME}}", hostname)
}
