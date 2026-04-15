"""
Weekly Report Aggregation Script
Compiles individual support engineer reports into a summary and updates master tracking.

Usage: python aggregate_reports.py [week_ending_date]
Example: python aggregate_reports.py 2026-03-27
"""

import os
import re
import csv
from datetime import datetime, timedelta
from pathlib import Path

# Configuration
BASE_DIR = Path(__file__).parent.parent
REPORTS_DIR = BASE_DIR / "reports" / "weekly"
DATA_DIR = BASE_DIR / "data"
MASTER_CSV = DATA_DIR / "master_tracking.csv"
AGENT_LIST = DATA_DIR / "agent_list.csv"


def get_current_week_ending():
    """Get the Friday date of the current week."""
    today = datetime.now()
    days_until_friday = (4 - today.weekday()) % 7
    if days_until_friday == 0 and today.hour >= 14:
        days_until_friday = 7
    friday = today + timedelta(days=days_until_friday)
    return friday.strftime("%Y-%m-%d")


def parse_report(report_path):
    """Parse a markdown report and extract key metrics."""
    with open(report_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    metrics = {
        'agent_name': '',
        'week_ending': '',
        'sites_visited': 0,
        'sites_monitored': 0,
        'aps_offline': 0,
        'migrations_found': 0,
        'tickets_handled': 0,
        'tickets_closed': 0,
        'tickets_escalated': 0,
        'notes': ''
    }
    
    # Extract agent name
    name_match = re.search(r'\*\*Support Engineer:\*\*\s*(.+)', content)
    if name_match:
        metrics['agent_name'] = name_match.group(1).strip()
    
    # Extract week ending
    week_match = re.search(r'\*\*Week Ending:\*\*\s*(\d{4}-\d{2}-\d{2})', content)
    if week_match:
        metrics['week_ending'] = week_match.group(1)
    
    # Count sites visited (rows in the Sites Visited table)
    visit_table = re.findall(r'\|\s*\d{4}-\d{2}-\d{2}\s*\|', content)
    metrics['sites_visited'] = len(visit_table)
    
    # Count sites monitored (rows in the Proactive Monitoring table)
    monitor_section = re.search(r'## 2\. Proactive Monitoring.*?## 3\.', content, re.DOTALL)
    if monitor_section:
        monitor_rows = re.findall(r'\|\s*\d{4}-\d{2}-\d{2}\s*\|', monitor_section.group())
        metrics['sites_monitored'] = len(monitor_rows)
    
    # Extract issue counts
    aps_match = re.search(r'APs Offline.*?Count:\s*(\d+)', content)
    if aps_match:
        metrics['aps_offline'] = int(aps_match.group(1))
    
    migrations_match = re.search(r'Site Migrations.*?Count:\s*(\d+)', content)
    if migrations_match:
        metrics['migrations_found'] = int(migrations_match.group(1))
    
    # Extract ticket metrics from the table
    tickets_section = re.search(r'### Summary Metrics.*?### Data Source', content, re.DOTALL)
    if tickets_section:
        section_text = tickets_section.group()
        
        handled_match = re.search(r'Total Calls/Tickets Handled\s*\|\s*(\d+)', section_text)
        if handled_match:
            metrics['tickets_handled'] = int(handled_match.group(1))
        
        closed_match = re.search(r'Tickets Closed\s*\|\s*(\d+)', section_text)
        if closed_match:
            metrics['tickets_closed'] = int(closed_match.group(1))
        
        escalated_match = re.search(r'Tickets Escalated\s*\|\s*(\d+)', section_text)
        if escalated_match:
            metrics['tickets_escalated'] = int(escalated_match.group(1))
    
    return metrics


def load_agents():
    """Load list of active agents."""
    agents = []
    if AGENT_LIST.exists():
        with open(AGENT_LIST, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('Active', '').lower() == 'yes':
                    agents.append(row['Agent_Name'])
    return agents


def create_weekly_summary(week_ending, all_metrics):
    """Generate a weekly summary markdown file."""
    summary = f"""# Weekly Summary Report

**Week Ending:** {week_ending}  
**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M")}

---

## Team Overview

| Agent | Sites Visited | Sites Monitored | APs Offline | Migrations | Tickets Handled | Tickets Closed |
|-------|---------------|-----------------|-------------|------------|-----------------|----------------|
"""
    
    totals = {
        'sites_visited': 0,
        'sites_monitored': 0,
        'aps_offline': 0,
        'migrations_found': 0,
        'tickets_handled': 0,
        'tickets_closed': 0
    }
    
    for m in all_metrics:
        summary += f"| {m['agent_name']} | {m['sites_visited']} | {m['sites_monitored']} | {m['aps_offline']} | {m['migrations_found']} | {m['tickets_handled']} | {m['tickets_closed']} |\n"
        for key in totals:
            totals[key] += m[key]
    
    summary += f"| **TOTAL** | **{totals['sites_visited']}** | **{totals['sites_monitored']}** | **{totals['aps_offline']}** | **{totals['migrations_found']}** | **{totals['tickets_handled']}** | **{totals['tickets_closed']}** |\n"
    
    summary += f"""

---

## Key Metrics

- **Total Sites Visited:** {totals['sites_visited']}
- **Total Sites Monitored:** {totals['sites_monitored']}
- **Total APs Found Offline:** {totals['aps_offline']}
- **Total Migrations Detected:** {totals['migrations_found']}
- **Total Tickets Handled:** {totals['tickets_handled']}
- **Total Tickets Closed:** {totals['tickets_closed']}
- **Ticket Close Rate:** {(totals['tickets_closed']/max(totals['tickets_handled'],1)*100):.1f}%

---

## Missing Reports

"""
    
    agents = load_agents()
    submitted = [m['agent_name'] for m in all_metrics]
    missing = [a for a in agents if a not in submitted]
    
    if missing:
        for agent in missing:
            summary += f"- ⚠️ {agent}\n"
    else:
        summary += "All reports received ✓\n"
    
    summary += """

---

*This summary was auto-generated by the report aggregation script.*
"""
    
    return summary


def append_to_master(all_metrics):
    """Append weekly metrics to the master tracking CSV."""
    file_exists = MASTER_CSV.exists() and MASTER_CSV.stat().st_size > 0
    
    with open(MASTER_CSV, 'a', newline='', encoding='utf-8') as f:
        fieldnames = ['Week_Ending', 'Agent_Name', 'Sites_Visited', 'Sites_Monitored', 
                      'APs_Offline', 'Migrations_Found', 'Tickets_Handled', 
                      'Tickets_Closed', 'Tickets_Escalated', 'Notes']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        for m in all_metrics:
            writer.writerow({
                'Week_Ending': m['week_ending'],
                'Agent_Name': m['agent_name'],
                'Sites_Visited': m['sites_visited'],
                'Sites_Monitored': m['sites_monitored'],
                'APs_Offline': m['aps_offline'],
                'Migrations_Found': m['migrations_found'],
                'Tickets_Handled': m['tickets_handled'],
                'Tickets_Closed': m['tickets_closed'],
                'Tickets_Escalated': m['tickets_escalated'],
                'Notes': m['notes']
            })


def main(week_ending=None):
    """Main execution function."""
    if week_ending is None:
        week_ending = get_current_week_ending()
    
    print(f"📊 Aggregating reports for week ending: {week_ending}")
    
    week_dir = REPORTS_DIR / week_ending
    
    if not week_dir.exists():
        print(f"⚠️  No reports directory found for {week_ending}")
        print(f"   Expected: {week_dir}")
        print(f"   Creating directory...")
        week_dir.mkdir(parents=True, exist_ok=True)
        return
    
    # Find all report files
    report_files = list(week_dir.glob("*_report.md"))
    
    if not report_files:
        print(f"⚠️  No report files found in {week_dir}")
        return
    
    print(f"📁 Found {len(report_files)} report(s)")
    
    all_metrics = []
    for report_file in report_files:
        if report_file.name.startswith("weekly_summary"):
            continue
        print(f"   Processing: {report_file.name}")
        metrics = parse_report(report_file)
        if not metrics['week_ending']:
            metrics['week_ending'] = week_ending
        all_metrics.append(metrics)
    
    # Generate summary
    summary = create_weekly_summary(week_ending, all_metrics)
    summary_path = week_dir / "weekly_summary.md"
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write(summary)
    print(f"✅ Generated: {summary_path}")
    
    # Append to master tracking
    append_to_master(all_metrics)
    print(f"✅ Updated: {MASTER_CSV}")
    
    print("\n📈 Week Summary:")
    print(f"   Reports processed: {len(all_metrics)}")
    print(f"   Total tickets handled: {sum(m['tickets_handled'] for m in all_metrics)}")
    print(f"   Total sites visited: {sum(m['sites_visited'] for m in all_metrics)}")


if __name__ == "__main__":
    import sys
    week = sys.argv[1] if len(sys.argv) > 1 else None
    main(week)
