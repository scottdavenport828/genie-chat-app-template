# Genie Chat App

A branded, production-ready chat interface for [Databricks Genie](https://docs.databricks.com/en/genie/index.html), deployed as a [Databricks App](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html). Ask natural-language questions about your data and get AI-generated SQL answers — all through a polished web UI.

## Architecture

```
Browser  ──▶  Databricks App (Flask + Gunicorn)  ──▶  Genie API  ──▶  Your Data
```

- **Flask** serves the single-page chat UI and proxies API calls
- **Gunicorn** (gthread) handles concurrent users with 1 worker / 12 threads
- **Genie API** (via `databricks-sdk`) translates natural language to SQL, executes it, and returns results
- **Databricks Apps** hosts the app, injects OAuth credentials, and manages the service principal

## Prerequisites

1. **Databricks workspace** with Unity Catalog enabled
2. **Databricks CLI** v0.200+ installed and configured with a named profile
   ```bash
   databricks auth login --profile my-profile --host https://your-workspace.cloud.databricks.com
   ```
3. **A Genie Space** set up in your workspace with at least one table attached
   (AI/BI Genie > "New" > add tables > save)

## Setup

### 1. Clone this repo

```bash
git clone <this-repo-url>
cd genie-chat-app-template
```

### 2. Configure your Genie Space ID

Open `app.yaml` and replace `<YOUR_GENIE_SPACE_ID>` with your Genie Space ID.

**How to find your Genie Space ID:**
1. Open your Databricks workspace
2. Navigate to **AI/BI Genie** in the left sidebar
3. Open your Genie Space
4. Copy the ID from the URL: `https://<workspace>/genie/rooms/<THIS_IS_YOUR_SPACE_ID>`

```yaml
# app.yaml
env:
  - name: GENIE_SPACE_ID
    value: "abc123def456abc789def012abc345de"   # <-- paste yours here
```

### 3. Configure your Databricks CLI profile

Open `databricks.yml` and replace `<YOUR_CLI_PROFILE>` with your CLI profile name:

```yaml
# databricks.yml
workspace:
  profile: my-profile   # <-- your CLI profile name
```

### 4. Deploy with Databricks Asset Bundles

```bash
# Validate the bundle configuration
databricks bundle validate -t dev --profile my-profile

# Deploy the app
databricks bundle deploy -t dev --profile my-profile
```

After the first deploy, the Databricks Apps platform creates a service principal for your app. You need to grant it access to your Genie Space.

### 5. Grant the app service principal access to Genie Space

1. Go to your Databricks workspace > **AI/BI Genie** > open your Genie Space
2. Click **Share** (top right)
3. Search for the app's service principal (usually named after your app, e.g. `genie-chat-app`)
4. Grant **Can Run** permission
5. Click **Save**

### 6. Open and verify

```bash
# Get the app URL
databricks apps get genie-chat-app --profile my-profile
```

Open the URL in your browser. Type a question and verify you get a response from Genie.

## Customization

### Branding & Colors

All styling is in `templates/index.html` within the `<style>` block. Key CSS values:

| Element | CSS Property | Default | What it controls |
|---------|-------------|---------|-----------------|
| Header background | `.header { background: }` | `#002139` | Top navigation bar |
| Send button | `.send-btn { background: }` | `#002139` | Chat send button |
| User message bubble | `.message.user .bubble { background: }` | `#002139` | User's chat bubbles |
| Sidebar section header | `.sidebar-section-header { background: }` | `#002139` | Active nav section |
| Bot avatar gradient | `.bot-avatar { background: }` | `#0097a7 → #00796b` | Bot icon circle |

### Logo

Replace `static/spec_large.png` with your own logo. It renders at `max-width: 180px` in the sidebar.

### App Title & Subtitle

In `templates/index.html`, update:
- **Header title** (line ~135): `<span class="header-title">Call Center AI Insights</span>`
- **Page title** (line ~169): `<h1>AI Forensics Chat</h1>`
- **Subtitle** (line ~170): `<div class="subtitle">Ask questions about...</div>`
- **HTML title** (line ~6): `<title>AI Forensics Chat - Spectrum</title>`

### Quick Questions

The four starter question cards are in `templates/index.html` (~lines 183-198). Each card has:
```html
<div class="qq-card" onclick="askQuickQuestion('Your question here')">
  <div class="qq-title"><span class="qq-icon">&#128201;</span> Card Title</div>
  <div class="qq-desc">Your question here</div>
  <span class="qq-tag">Category Tag</span>
</div>
```

Replace the questions, titles, icons, and tags to match your use case. You can add or remove cards — the grid wraps automatically.

### Input Placeholder

Update the placeholder text in the `<input>` element (~line 207):
```html
<input ... placeholder="Ask me about call center metrics, agent performance, or anything else...">
```

## File Structure

```
genie-chat-app-template/
├── app.py              # Flask routes: serves UI, proxies Genie API calls
├── genie_client.py     # Genie API client with retry, backoff, and polling
├── app.yaml            # Databricks App config: startup command + env vars
├── databricks.yml      # DABs bundle config: app name, profile, resources
├── requirements.txt    # Python dependencies (Flask, databricks-sdk)
├── .gitignore          # Ignores __pycache__, .databricks/, .env
├── templates/
│   └── index.html      # Single-page chat UI (HTML + CSS + JS)
└── static/
    └── spec_large.png  # Sidebar logo image
```

### How the files work together

- `app.yaml` tells Databricks Apps to start Gunicorn, which loads `app.py`
- `app.py` reads `GENIE_SPACE_ID` from environment and creates a `GenieClient`
- When a user sends a question, the browser calls `POST /api/ask`
- `app.py` calls `genie_client.py` which uses the Databricks SDK to talk to the Genie API
- The Genie API translates the question to SQL, runs it, and returns results
- `genie_client.py` polls with exponential backoff (1s → 60s) until the query completes or times out (10 min)
- The response (text + SQL) is sent back to the browser and rendered in the chat

## Conversation Persistence Setup

To enable per-user conversation history that survives page reloads, the app uses a Delta table to record conversation ownership. Set these two env vars in `app.yaml`:

```yaml
env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: "<your-warehouse-id>"
  - name: CONVERSATION_TABLE
    value: "<catalog>.<schema>.<table>"
```

### Creating the table (first-time setup)

Run this in a Databricks notebook or SQL editor as a user with `CREATE TABLE` privilege on the target schema:

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<table> (
  user_email     STRING  NOT NULL,
  conversation_id STRING NOT NULL,
  title          STRING,
  created_at     TIMESTAMP
);
```

Then grant the app service principal access:

```sql
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<app-service-principal>`;
GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.<table> TO `<app-service-principal>`;
```

### Upgrading from an earlier version (schema migration)

If you already have the table without the `title` column, add it with:

```sql
ALTER TABLE <catalog>.<schema>.<table> ADD COLUMN title STRING;
```

Existing rows will have `NULL` for title and display as "Untitled" in the sidebar. New conversations will automatically store the opening question as the title.

## Troubleshooting

### "GENIE_SPACE_ID not configured"
The `GENIE_SPACE_ID` env var in `app.yaml` is still set to the placeholder. Replace `<YOUR_GENIE_SPACE_ID>` with your actual Genie Space ID.

### "Query FAILED" or permission errors
The app's service principal doesn't have access to your Genie Space. Follow Step 5 above to grant **Can Run** permission.

### App deploys but shows a blank page
Check the app logs:
```bash
databricks apps get-logs genie-chat-app --profile my-profile
```
Common causes:
- Missing `requirements.txt` dependency
- Syntax error in `app.yaml`

### Genie returns no results
- Verify your Genie Space has tables attached and that you can query it from the Genie UI directly
- Ensure the tables are accessible to the app's service principal (UC permissions)

### Timeout after 600 seconds
The Genie API can be slow for complex queries. The 600s (10-minute) timeout is the Databricks-recommended maximum. If queries consistently time out, simplify the question or optimize the underlying tables.

### Redeployment doesn't pick up changes
Databricks Apps and Flask can cache static files aggressively. The app sets `SEND_FILE_MAX_AGE_DEFAULT = 0` to prevent Flask-side caching. If you still see stale content, try a hard refresh (Ctrl+Shift+R) or redeploy:
```bash
databricks bundle deploy -t dev --profile my-profile
databricks apps deploy genie-chat-app --profile my-profile
```

## Architecture Notes

### Threading Model
Gunicorn runs with 1 worker and 12 gthread threads. Each Genie query blocks a thread while polling, so 12 threads supports ~12 concurrent users. Increase `--threads` in `app.yaml` if you need more concurrency.

### Polling & Retry Strategy
The Genie client implements production-grade resilience:
- **Exponential backoff polling**: starts at 1s, doubles each poll, caps at 60s
- **Retryable error detection**: automatically retries on connection errors, timeouts, rate limits, and 5xx responses
- **3 retries per API call** with jittered backoff to avoid thundering herd
- **10-minute total timeout** per query (Databricks recommended)

### Conversation Management
The app supports multi-turn conversations. The first question creates a new Genie conversation; follow-up questions continue the same conversation for context-aware answers. The sidebar shows recent conversations and lets users switch between them.
