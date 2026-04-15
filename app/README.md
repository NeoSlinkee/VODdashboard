# Support Engineer Report Tracker - Local Web App

A local web application for tracking weekly support engineer reports. No internet required - runs entirely on your local network.

## Quick Start

### First Time Setup
1. Make sure Python 3.8+ is installed ([Download Python](https://www.python.org/downloads/))
2. Double-click `setup_and_run.bat`
3. Wait for dependencies to install
4. App will open automatically

### After Setup
Just double-click `run.bat` to start the app.

### Access the App
- **On this computer:** http://localhost:5000
- **From other devices:** http://YOUR_IP:5000

To find your IP address, open Command Prompt and type: `ipconfig`

---

## Features

### Quick Entry
Log daily activities in one simple form:
- Site visits
- Ruckus monitoring data (APs offline, network issues)
- Ticket counts

### Detailed Entry
Each category has its own detailed entry form:
- **Site Visits** - Full details of on-site visits
- **Monitoring** - Ruckus controller data with AP status
- **Service Calls** - Individual or bulk ticket entry

### Reports
- Auto-generates weekly reports per agent
- Aggregates all daily entries into summary
- Submit with achievements/challenges notes

### Company Summary
- Team-wide dashboard
- All agents' metrics in one view
- Export to CSV for your company reports
- Track missing reports

---

## Workflow

### Daily (Each Support Engineer)
1. Go to **Quick Entry**
2. Select your name + date
3. Fill in any site visits
4. Add Ruckus monitoring data if checked any sites
5. Enter ticket counts from Freshdesk
6. Click Save

### Friday (Before 2pm)
1. Each agent: Go to Dashboard → Generate Report
2. Review the auto-generated report
3. Add achievements/challenges
4. Submit

### Friday (After 2pm - Manager)
1. Go to **Company Summary**
2. Check all reports are in
3. Export to CSV
4. Use for company reporting

---

## Data Storage

All data is stored locally in `reports.db` (SQLite database).
No cloud, no external servers - everything stays on your machine.

---

## Troubleshooting

**App won't start:**
- Make sure Python is installed
- Run Command Prompt as Administrator
- Try: `pip install -r requirements.txt`

**Can't access from other devices:**
- Check Windows Firewall allows port 5000
- Make sure devices are on same network
- Use your actual IP, not localhost

**Need to reset everything:**
- Delete `reports.db` file
- Restart the app
