# Calendar Setup Instructions

Set up these recurring calendar events for your team:

---

## For Support Engineers

### Event 1: Daily Activity Log Reminder
- **Title:** 📝 Log Daily Activities
- **Time:** End of each day (e.g., 4:30 PM)
- **Recurrence:** Daily (Mon-Fri)
- **Notes:** 
  - Log sites visited
  - Log monitoring activities
  - Note ticket counts
  - Update daily_log.md

### Event 2: Weekly Report Reminder
- **Title:** 📊 Complete Weekly Report - DUE 2PM
- **Time:** 11:00 AM Friday
- **Recurrence:** Weekly
- **Notes:**
  - Use template in /templates/weekly_report_template.md
  - Save to /reports/weekly/[week-ending-date]/
  - Include all daily activities
  - Due by 2:00 PM

### Event 3: Report Due Warning
- **Title:** ⚠️ Weekly Report Due in 30 Minutes
- **Time:** 1:30 PM Friday
- **Recurrence:** Weekly
- **Notes:** Final reminder - submit report NOW

---

## For Manager/Team Lead

### Event 1: Report Collection
- **Title:** 📥 Collect Support Engineer Reports
- **Time:** 2:15 PM Friday
- **Recurrence:** Weekly
- **Notes:**
  - Check /reports/weekly/ folder
  - Run aggregate_reports.py
  - Follow up on missing reports

### Event 2: Compile Company Report
- **Title:** 📈 Compile Weekly Company Report
- **Time:** 3:00 PM Friday (block 1 hour)
- **Recurrence:** Weekly
- **Notes:**
  - Review weekly_summary.md
  - Use company_report_template.md
  - Finalize for company submission

### Event 3: Report Submission
- **Title:** 📤 Submit Company Report
- **Time:** 4:30 PM Friday
- **Recurrence:** Weekly
- **Notes:** Final submission to company

---

## Outlook Calendar Setup Steps

1. Open Outlook Calendar
2. Click "New Appointment" or "New Event"
3. Fill in title and time
4. Click "Recurrence" to set weekly pattern
5. Add notes/description
6. Click "Save & Close"

## Google Calendar Setup Steps

1. Open Google Calendar
2. Click on the time slot
3. Add event title
4. Click "More options"
5. Under "Does not repeat", select "Weekly on Friday"
6. Add description
7. Click "Save"

---

## Time Blocking Recommendation

Each Friday, block this time for report processing:

| Time | Duration | Task |
|------|----------|------|
| 11:00 AM - 12:00 PM | 1 hour | Support Engineers: Write weekly report |
| 2:00 PM - 2:30 PM | 30 min | Manager: Collect and verify reports |
| 2:30 PM - 3:30 PM | 1 hour | Manager: Run scripts, compile summary |
| 3:30 PM - 4:30 PM | 1 hour | Manager: Create company report |
