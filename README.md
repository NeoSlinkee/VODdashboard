# Weekly Support Engineer Report Tracking System

## Overview
This system tracks weekly reports from Support Engineers, collecting key metrics and activities for company reporting.

## Report Deadline
**Every Friday by 2:00 PM**

## What Each Report Must Include

### 1. Sites Visited
- Site name/location
- Date visited
- What was discussed
- Information obtained
- Action items/follow-ups

### 2. Proactive Monitoring
- Sites logged in to remotely
- Monitoring activities performed
- Issues discovered:
  - APs offline
  - Site migrations
  - Network issues
  - Other anomalies
- Resolution status

### 3. Service Call Metrics
- Total calls handled
- Total calls closed
- Source: Freshdesk ticket numbers OR manual count
- Notable tickets/escalations

---

## Folder Structure

```
/reports/
    /weekly/
        /YYYY-MM-DD/           # Week ending date
            agent1_report.md
            agent2_report.md
            weekly_summary.md
    /templates/
        weekly_report_template.md
/data/
    master_tracking.csv        # Aggregated data for all weeks
    agent_list.csv             # List of support engineers
/scripts/
    aggregate_reports.py       # Script to compile weekly data
```

---

## Process

### For Support Engineers:
1. Block 30 minutes on Friday morning (before 2pm) for report writing
2. Copy the template from `/templates/weekly_report_template.md`
3. Save to `/reports/weekly/[week-ending-date]/[your-name]_report.md`
4. Fill in all sections completely
5. Submit by 2:00 PM Friday

### For Manager:
1. Run the aggregation script after 2pm Friday
2. Review individual reports in the weekly folder
3. Check `weekly_summary.md` for compiled data
4. Use `master_tracking.csv` for trend analysis and company reporting

---

## Quick Start

1. Add your support engineers to `data/agent_list.csv`
2. Share this folder with all support engineers
3. Have each engineer use the template each Friday
4. Run `python scripts/aggregate_reports.py` to compile data

---

## Calendar Reminders to Set

| Who | Reminder | Day | Time |
|-----|----------|-----|------|
| All Support Engineers | "Complete Weekly Report" | Friday | 11:00 AM |
| All Support Engineers | "Report Due" | Friday | 1:30 PM |
| Manager | "Collect & Review Reports" | Friday | 2:15 PM |
| Manager | "Compile Company Report" | Friday | 3:00 PM |
