# cf-vps-monitor Fork

[简体中文](https://github.com/aiqiji7/cf-vps-monitor/blob/main/README.md) | [English](https://github.com/aiqiji7/cf-vps-monitor/blob/main/README-EN.md)

This project is a Cloudflare Worker + D1 based monitoring panel for VPS probes, website checks, and LLM endpoint availability checks.

## Fork Notice

This repository is a secondary-development fork based on the original project.

- Current repository: https://github.com/aiqiji7/cf-vps-monitor
- Original repository: https://github.com/kadidalax/cf-vps-monitor

### Main Differences From The Original Project

- Added LLM endpoint availability monitoring for OpenAI-compatible `/v1/chat/completions` APIs.
- Added Provider grouping for LLM endpoints. If the Provider name is empty, the API URL domain is used automatically.
- Added configurable website high-latency threshold, LLM high-latency threshold, and LLM timeout threshold.
- User-facing response times and thresholds are displayed in seconds `s`; internal database fields and detection logic still use milliseconds for compatibility.
- Added distinct visual states for normal, high latency, timeout, down, error, and pending.
- Timeout and high-latency states use different badge colors, row backgrounds, and 24-hour history bar colors.
- Frontend page refresh queues one public LLM endpoint availability check automatically.
- LLM endpoints support output preview, expected-content matching, public visibility toggle, notification toggle, and 24-hour status history.
- The admin page separates website thresholds from LLM thresholds.
- VPS monitoring, website monitoring, Telegram/ntfy notifications, custom background, opacity, sorting, and visibility controls are retained and improved.

## Features

### VPS Monitoring

- Reports VPS metrics through the `cf-vps-monitor.sh` agent.
- Displays CPU, memory, disk, upload/download speed, total traffic, uptime, and last update time.
- Generates server ID, API key, and one-click installation commands in the admin panel.
- Supports sorting, public visibility, and admin management.

### Website Monitoring

- Adds HTTP/HTTPS websites for availability checks.
- Displays status, status code, response time, and 24-hour history.
- Supports configurable website high-latency threshold in seconds `s`.
- Supports sorting, public visibility, and admin management.

### LLM Endpoint Monitoring

- Supports OpenAI-compatible endpoints such as `https://api.example.com/v1/chat/completions`.
- Supports API key and model name configuration.
- Test prompt and expected-content matching are global settings shared by all endpoints (one configuration covers all models), configured in the LLM management section.
- Supports batch adding multiple models via newline, comma-separated text, or JSON array.
- Supports Provider grouping. If Provider name is empty, the API URL domain is used, for example `https://api.openai.com/v1/chat/completions` becomes `api.openai.com`.
- The same API URL with different Provider names is treated as separate Providers; models are not merged into one group.
- Statuses include normal, high latency, timeout, down, error, and pending.
- Supports configurable LLM high-latency threshold and a global LLM timeout threshold (shared by all endpoints) in seconds `s`.
- Queues one public LLM endpoint check when the frontend page loads or refreshes.
- The 24-hour history bar distinguishes high latency, timeout, and down/error states.

### Notifications And UI

- Telegram notifications.
- ntfy notifications.
- Custom background image and page opacity.
- Light and dark themes.

## Status Colors

- Normal: green.
- High latency: yellow.
- Timeout: orange.
- Down/error: red.
- Pending/no record: gray.

## Deployment Requirements

- A Cloudflare account.
- A Cloudflare D1 database.
- A Cloudflare Worker.
- `JWT_SECRET` is recommended. Change the default password immediately after first login.

## Deployment Steps

### 1. Create A D1 Database

1. Log in to the Cloudflare dashboard.
2. Go to `Storage & Databases` → `D1 SQL Database`.
3. Click `Create Database`.
4. Use any database name, such as `vps-monitor-db`.

### 2. Create A Worker And Deploy Code

1. Go to `Workers & Pages`.
2. Create a Worker.
3. Open the online Worker editor.
4. Delete the default code.
5. Copy the full content of `worker.js` from this repository.
6. Paste it into the Worker editor and deploy.

### 3. Configure Environment Variables

Add these variables in Worker `Settings` → `Variables and Secrets`:

| Variable | Required | Description |
| --- | --- | --- |
| `JWT_SECRET` | Recommended | Token signing secret. Use a random string of 30+ characters. |
| `USERNAME` | Optional | Initial admin username. Uses default if empty. |
| `PASSWORD` | Optional | Initial admin password. Uses default if empty. |

Default login:

- Username: `admin`
- Password: `monitor2025!`

Change the password immediately after first login.

### 4. Bind The D1 Database

1. Open Worker settings.
2. Add a D1 database binding.
3. The binding variable name must be `DB`.
4. Select your D1 database and redeploy.

### 5. Initialize Database

After deployment and D1 binding, visit:

```text
https://your-worker-url/api/init-db
```

If initialization succeeds, you will see a response like:

```json
{"success": true, "message": "数据库初始化完成"}
```

### 6. Add A Cron Trigger

Add a Cron trigger for scheduled website and LLM checks.

Hourly execution is a reasonable starting point. The admin panel can configure the LLM check frequency multiplier:

- `1`: check LLM endpoints on every Cron run.
- `2`: check LLM endpoints every two Cron runs.
- `3`: check LLM endpoints every three Cron runs.

## Usage

### Login

Open your Worker URL and click admin login, or visit:

```text
https://your-worker-url/login.html
```

Change the default password immediately after login.

### Add A VPS Server

1. Click add server in the admin panel.
2. Enter the server name and description.
3. Save it to generate server ID and API key.
4. Copy the one-click installation command and run it on your VPS.

You can also download the agent manually:

```bash
wget -O cf-vps-monitor.sh https://raw.githubusercontent.com/aiqiji7/cf-vps-monitor/main/cf-vps-monitor.sh && chmod +x cf-vps-monitor.sh && ./cf-vps-monitor.sh
```

Or:

```bash
curl -O https://raw.githubusercontent.com/aiqiji7/cf-vps-monitor/main/cf-vps-monitor.sh && chmod +x cf-vps-monitor.sh && ./cf-vps-monitor.sh
```

The agent needs:

- Server ID.
- API key.
- Worker URL.

### Add Website Monitoring

1. Click add monitored website in the admin panel.
2. Enter website name and URL.
3. Save it.
4. Configure website high-latency threshold in seconds `s` in the website management section.

### Add LLM Endpoints

1. Click add LLM endpoint in the admin panel.
2. Enter API URL, such as `https://api.example.com/v1/chat/completions`.
3. Enter one or more model names.
4. Optionally enter an API key. Test prompt and expected content are global settings configured in the LLM management section (shared by all endpoints).
5. Provider name can be empty. The system will use the API URL domain automatically. Different Provider names on the same API URL become separate Provider groups.
6. Configure these in the LLM management section:
   - LLM high-latency threshold in seconds `s`.
   - LLM timeout threshold (global, shared by all endpoints) in seconds `s`.
   - LLM check frequency multiplier.
   - Test prompt and expected-content matching (global, shared by all endpoints; one configuration covers all models).

## Time Unit Notes

The UI uses seconds `s` for:

- Response time.
- Website high-latency threshold.
- LLM high-latency threshold.
- LLM timeout threshold.

Internal code, database fields, and API payloads still use milliseconds. Field names such as `_ms` are intentionally preserved for compatibility.

## Notes

- Cloudflare Worker and D1 have quota limits. More VPS nodes and shorter report intervals consume more requests and writes.
- LLM endpoint checks call external APIs. Page refresh queues one extra public LLM endpoint check, which may increase external API usage.
- The default password is unsafe. Change it after first login.
- Protect API keys, Telegram tokens, and ntfy topics.
- If the panel fails after deployment, check Worker logs, ensure the D1 binding name is `DB`, and confirm `/api/init-db` has been visited.

## Credits

Thanks to the original project author for the base implementation:

- Original project: https://github.com/kadidalax/cf-vps-monitor

This repository is a secondary-development fork. Current behavior is defined by this repository's code.
