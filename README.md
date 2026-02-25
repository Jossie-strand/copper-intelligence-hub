[README.md](https://github.com/user-attachments/files/25536370/README.md)
# Copper Intelligence Hub — GitHub Actions Setup

## What This Does
A GitHub Actions workflow runs Python scripts every weeknight at 10:30 PM ET
(after CME updates at ~9:30 PM). Each script fetches a data source, parses it,
and appends a timestamped row to your Google Sheet.

Current feeds: COMEX Copper Inventory
Planned: LME, SHFE, copper price, mine disruptions

---

## One-Time Setup (~20 minutes)

### Step 1 — Create the GitHub repo

1. Go to github.com → click **+** → **New repository**
2. Name it: `copper-intelligence-hub`
3. Set to **Private**
4. Click **Create repository**
5. Upload all files from this folder maintaining the folder structure

---

### Step 2 — Create a Google Service Account

This gives GitHub permission to write to your Google Sheet.

1. Go to **console.cloud.google.com**
2. Create a new project (or use existing) — name it `Copper Hub`
3. Go to **APIs & Services → Enable APIs**
   - Enable **Google Sheets API**
   - Enable **Google Drive API**
4. Go to **APIs & Services → Credentials → Create Credentials → Service Account**
   - Name: `copper-hub-writer`
   - Click through to finish
5. Click the service account you just created → **Keys** tab → **Add Key → JSON**
6. This downloads a `.json` file — **keep this safe, treat it like a password**
7. Open the JSON file and copy the `client_email` field (looks like `copper-hub-writer@your-project.iam.gserviceaccount.com`)

---

### Step 3 — Share your Google Sheet with the service account

1. Open your `3-exchange-inventory-tracker` Google Sheet
2. Click **Share**
3. Paste the `client_email` from Step 2
4. Set permission to **Editor**
5. Click **Send**

---

### Step 4 — Add secrets to GitHub

1. In your GitHub repo → **Settings → Secrets and variables → Actions**
2. Click **New repository secret**

**Secret 1:**
- Name: `GOOGLE_SERVICE_ACCOUNT_JSON`
- Value: the entire contents of the JSON file from Step 2 (paste the whole thing)

---

### Step 5 — Test it manually

1. In your GitHub repo → **Actions** tab
2. Click **Copper Intelligence Hub — Daily Feeds**
3. Click **Run workflow** → **Run workflow**
4. Watch the run — click into `comex-inventory` job to see logs
5. Check your Google Sheet for a new row

If the run shows ✅ green, you're live. It will now run automatically every weeknight.

---

## Adding More Feeds Later

To add LME, SHFE, or any other feed:
1. Create `feeds/lme_inventory.py` (following the same pattern as `comex_inventory.py`)
2. Uncomment the relevant job block in `.github/workflows/copper_feeds.yml`
3. Push to GitHub — it will run on the next schedule

---

## Connecting Claude Code

Once the data is flowing into your Sheet, Claude Code can:
- Read all tabs and synthesize a weekly brief automatically
- Flag when inventory signals cross your thresholds (cancelled warrant >30%, combined <200kt)
- Generate the mine disruption log updates
- Draft the full weekly brief document

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| HTTP 403 from CME | CME may have temporarily blocked the IP — re-run next day |
| `Parse failed — total_st is None` | CME changed XLS structure — check the row logs in the Action output |
| `GOOGLE_SERVICE_ACCOUNT_JSON` error | Re-paste the full JSON into the GitHub secret, ensure no truncation |
| Sheet not found | Confirm sheet name exactly matches `"3-exchange-inventory-tracker"` and service account has Editor access |
