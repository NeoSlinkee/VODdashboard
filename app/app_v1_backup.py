"""
Support Engineer Weekly Report Tracking System
Local web application for easy data entry and report generation.

Run with: python app.py
Access at: http://localhost:5000 or http://YOUR_IP:5000
"""

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
import os
import csv
from io import StringIO

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-change-this'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///reports.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# ============== Database Models ==============

class Agent(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(120))
    active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    site_visits = db.relationship('SiteVisit', backref='agent', lazy=True)
    monitoring_logs = db.relationship('MonitoringLog', backref='agent', lazy=True)
    service_calls = db.relationship('ServiceCall', backref='agent', lazy=True)

class SiteVisit(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    agent_id = db.Column(db.Integer, db.ForeignKey('agent.id'), nullable=False)
    date = db.Column(db.Date, nullable=False)
    site_name = db.Column(db.String(200), nullable=False)
    location = db.Column(db.String(200))
    discussion_topics = db.Column(db.Text)
    info_obtained = db.Column(db.Text)
    follow_up_actions = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class MonitoringLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    agent_id = db.Column(db.Integer, db.ForeignKey('agent.id'), nullable=False)
    date = db.Column(db.Date, nullable=False)
    site_name = db.Column(db.String(200), nullable=False)
    purpose = db.Column(db.String(200))
    
    # Ruckus Controller specific fields
    aps_offline = db.Column(db.Integer, default=0)
    aps_total = db.Column(db.Integer, default=0)
    network_issues = db.Column(db.Boolean, default=False)
    site_migration = db.Column(db.Boolean, default=False)
    
    issues_found = db.Column(db.Text)
    resolution_status = db.Column(db.String(50))  # resolved, pending, escalated
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class ServiceCall(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    agent_id = db.Column(db.Integer, db.ForeignKey('agent.id'), nullable=False)
    date = db.Column(db.Date, nullable=False)
    ticket_number = db.Column(db.String(50))
    summary = db.Column(db.String(500))
    status = db.Column(db.String(50))  # open, closed, escalated
    time_spent_minutes = db.Column(db.Integer)
    resolution = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class WeeklyReport(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    agent_id = db.Column(db.Integer, db.ForeignKey('agent.id'), nullable=False)
    week_ending = db.Column(db.Date, nullable=False)
    
    # Aggregated metrics
    sites_visited = db.Column(db.Integer, default=0)
    sites_monitored = db.Column(db.Integer, default=0)
    aps_offline_total = db.Column(db.Integer, default=0)
    network_issues_count = db.Column(db.Integer, default=0)
    migrations_count = db.Column(db.Integer, default=0)
    tickets_handled = db.Column(db.Integer, default=0)
    tickets_closed = db.Column(db.Integer, default=0)
    tickets_escalated = db.Column(db.Integer, default=0)
    
    achievements = db.Column(db.Text)
    challenges = db.Column(db.Text)
    notes = db.Column(db.Text)
    
    submitted_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    agent = db.relationship('Agent', backref='weekly_reports')

# ============== Helper Functions ==============

def get_week_ending(date=None):
    """Get the Friday of the current week."""
    if date is None:
        date = datetime.now()
    days_until_friday = (4 - date.weekday()) % 7
    if days_until_friday == 0 and date.weekday() == 4:
        return date.date()
    return (date + timedelta(days=days_until_friday)).date()

def get_week_start(week_ending):
    """Get Monday of the week (5 days before Friday)."""
    return week_ending - timedelta(days=4)

# ============== Routes ==============

@app.route('/')
def index():
    agents = Agent.query.filter_by(active=True).all()
    week_ending = get_week_ending()
    recent_reports = WeeklyReport.query.order_by(WeeklyReport.week_ending.desc()).limit(10).all()
    return render_template('index.html', agents=agents, week_ending=week_ending, recent_reports=recent_reports)

# ----- Agent Management -----

@app.route('/agents')
def agents():
    all_agents = Agent.query.all()
    return render_template('agents.html', agents=all_agents)

@app.route('/agents/add', methods=['GET', 'POST'])
def add_agent():
    if request.method == 'POST':
        agent = Agent(
            name=request.form['name'],
            email=request.form.get('email', ''),
            active=True
        )
        db.session.add(agent)
        db.session.commit()
        flash(f'Agent {agent.name} added successfully!', 'success')
        return redirect(url_for('agents'))
    return render_template('add_agent.html')

@app.route('/agents/<int:id>/toggle')
def toggle_agent(id):
    agent = Agent.query.get_or_404(id)
    agent.active = not agent.active
    db.session.commit()
    flash(f'Agent {agent.name} {"activated" if agent.active else "deactivated"}.', 'info')
    return redirect(url_for('agents'))

# ----- Site Visits -----

@app.route('/site-visits')
def site_visits():
    visits = SiteVisit.query.order_by(SiteVisit.date.desc()).limit(50).all()
    return render_template('site_visits.html', visits=visits)

@app.route('/site-visits/add', methods=['GET', 'POST'])
def add_site_visit():
    agents = Agent.query.filter_by(active=True).all()
    if request.method == 'POST':
        visit = SiteVisit(
            agent_id=request.form['agent_id'],
            date=datetime.strptime(request.form['date'], '%Y-%m-%d').date(),
            site_name=request.form['site_name'],
            location=request.form.get('location', ''),
            discussion_topics=request.form.get('discussion_topics', ''),
            info_obtained=request.form.get('info_obtained', ''),
            follow_up_actions=request.form.get('follow_up_actions', '')
        )
        db.session.add(visit)
        db.session.commit()
        flash('Site visit logged successfully!', 'success')
        return redirect(url_for('site_visits'))
    return render_template('add_site_visit.html', agents=agents, today=datetime.now().strftime('%Y-%m-%d'))

# ----- Monitoring Logs (Ruckus Controller Data) -----

@app.route('/monitoring')
def monitoring():
    logs = MonitoringLog.query.order_by(MonitoringLog.date.desc()).limit(50).all()
    return render_template('monitoring.html', logs=logs)

@app.route('/monitoring/add', methods=['GET', 'POST'])
def add_monitoring():
    agents = Agent.query.filter_by(active=True).all()
    if request.method == 'POST':
        log = MonitoringLog(
            agent_id=request.form['agent_id'],
            date=datetime.strptime(request.form['date'], '%Y-%m-%d').date(),
            site_name=request.form['site_name'],
            purpose=request.form.get('purpose', 'Proactive Monitoring'),
            aps_offline=int(request.form.get('aps_offline', 0)),
            aps_total=int(request.form.get('aps_total', 0)),
            network_issues='network_issues' in request.form,
            site_migration='site_migration' in request.form,
            issues_found=request.form.get('issues_found', ''),
            resolution_status=request.form.get('resolution_status', 'pending'),
            notes=request.form.get('notes', '')
        )
        db.session.add(log)
        db.session.commit()
        flash('Monitoring log added successfully!', 'success')
        return redirect(url_for('monitoring'))
    return render_template('add_monitoring.html', agents=agents, today=datetime.now().strftime('%Y-%m-%d'))

# ----- Service Calls / Tickets -----

@app.route('/service-calls')
def service_calls():
    calls = ServiceCall.query.order_by(ServiceCall.date.desc()).limit(50).all()
    return render_template('service_calls.html', calls=calls)

@app.route('/service-calls/add', methods=['GET', 'POST'])
def add_service_call():
    agents = Agent.query.filter_by(active=True).all()
    if request.method == 'POST':
        call = ServiceCall(
            agent_id=request.form['agent_id'],
            date=datetime.strptime(request.form['date'], '%Y-%m-%d').date(),
            ticket_number=request.form.get('ticket_number', ''),
            summary=request.form.get('summary', ''),
            status=request.form.get('status', 'open'),
            time_spent_minutes=int(request.form.get('time_spent', 0)) if request.form.get('time_spent') else 0,
            resolution=request.form.get('resolution', '')
        )
        db.session.add(call)
        db.session.commit()
        flash('Service call logged successfully!', 'success')
        return redirect(url_for('service_calls'))
    return render_template('add_service_call.html', agents=agents, today=datetime.now().strftime('%Y-%m-%d'))

@app.route('/service-calls/bulk', methods=['GET', 'POST'])
def bulk_service_calls():
    """Bulk add tickets - paste from Freshdesk export."""
    agents = Agent.query.filter_by(active=True).all()
    if request.method == 'POST':
        agent_id = request.form['agent_id']
        tickets_handled = int(request.form.get('tickets_handled', 0))
        tickets_closed = int(request.form.get('tickets_closed', 0))
        date = datetime.strptime(request.form['date'], '%Y-%m-%d').date()
        
        # Create a summary entry
        call = ServiceCall(
            agent_id=agent_id,
            date=date,
            ticket_number=f"BULK-{date.strftime('%Y%m%d')}",
            summary=f"Bulk entry: {tickets_handled} handled, {tickets_closed} closed",
            status='closed' if tickets_handled == tickets_closed else 'open'
        )
        db.session.add(call)
        db.session.commit()
        flash(f'Bulk entry added: {tickets_handled} handled, {tickets_closed} closed', 'success')
        return redirect(url_for('service_calls'))
    return render_template('bulk_service_calls.html', agents=agents, today=datetime.now().strftime('%Y-%m-%d'))

# ----- Weekly Reports -----

@app.route('/reports')
def reports():
    all_reports = WeeklyReport.query.order_by(WeeklyReport.week_ending.desc()).all()
    return render_template('reports.html', reports=all_reports)

@app.route('/reports/generate/<int:agent_id>')
def generate_report(agent_id):
    """Generate weekly report for an agent."""
    agent = Agent.query.get_or_404(agent_id)
    week_ending = get_week_ending()
    week_start = get_week_start(week_ending)
    
    # Check if report already exists
    existing = WeeklyReport.query.filter_by(agent_id=agent_id, week_ending=week_ending).first()
    if existing:
        flash('Report for this week already exists. Updating...', 'info')
        report = existing
    else:
        report = WeeklyReport(agent_id=agent_id, week_ending=week_ending)
    
    # Aggregate site visits
    visits = SiteVisit.query.filter(
        SiteVisit.agent_id == agent_id,
        SiteVisit.date >= week_start,
        SiteVisit.date <= week_ending
    ).all()
    report.sites_visited = len(visits)
    
    # Aggregate monitoring logs
    logs = MonitoringLog.query.filter(
        MonitoringLog.agent_id == agent_id,
        MonitoringLog.date >= week_start,
        MonitoringLog.date <= week_ending
    ).all()
    report.sites_monitored = len(logs)
    report.aps_offline_total = sum(log.aps_offline for log in logs)
    report.network_issues_count = sum(1 for log in logs if log.network_issues)
    report.migrations_count = sum(1 for log in logs if log.site_migration)
    
    # Aggregate service calls
    calls = ServiceCall.query.filter(
        ServiceCall.agent_id == agent_id,
        ServiceCall.date >= week_start,
        ServiceCall.date <= week_ending
    ).all()
    report.tickets_handled = len(calls)
    report.tickets_closed = sum(1 for c in calls if c.status == 'closed')
    report.tickets_escalated = sum(1 for c in calls if c.status == 'escalated')
    
    if not existing:
        db.session.add(report)
    db.session.commit()
    
    flash(f'Report generated for {agent.name}!', 'success')
    return redirect(url_for('view_report', id=report.id))

@app.route('/reports/view/<int:id>')
def view_report(id):
    report = WeeklyReport.query.get_or_404(id)
    week_start = get_week_start(report.week_ending)
    
    # Get detailed data
    visits = SiteVisit.query.filter(
        SiteVisit.agent_id == report.agent_id,
        SiteVisit.date >= week_start,
        SiteVisit.date <= report.week_ending
    ).all()
    
    logs = MonitoringLog.query.filter(
        MonitoringLog.agent_id == report.agent_id,
        MonitoringLog.date >= week_start,
        MonitoringLog.date <= report.week_ending
    ).all()
    
    calls = ServiceCall.query.filter(
        ServiceCall.agent_id == report.agent_id,
        ServiceCall.date >= week_start,
        ServiceCall.date <= report.week_ending
    ).all()
    
    return render_template('view_report.html', report=report, visits=visits, logs=logs, calls=calls)

@app.route('/reports/submit/<int:id>', methods=['POST'])
def submit_report(id):
    report = WeeklyReport.query.get_or_404(id)
    report.achievements = request.form.get('achievements', '')
    report.challenges = request.form.get('challenges', '')
    report.notes = request.form.get('notes', '')
    report.submitted_at = datetime.utcnow()
    db.session.commit()
    flash('Report submitted successfully!', 'success')
    return redirect(url_for('view_report', id=id))

# ----- Company Summary -----

@app.route('/summary')
def weekly_summary():
    week_ending = request.args.get('week', get_week_ending().isoformat())
    week_ending_date = datetime.strptime(week_ending, '%Y-%m-%d').date()
    
    reports = WeeklyReport.query.filter_by(week_ending=week_ending_date).all()
    agents = Agent.query.filter_by(active=True).all()
    
    # Calculate totals
    totals = {
        'sites_visited': sum(r.sites_visited for r in reports),
        'sites_monitored': sum(r.sites_monitored for r in reports),
        'aps_offline': sum(r.aps_offline_total for r in reports),
        'network_issues': sum(r.network_issues_count for r in reports),
        'migrations': sum(r.migrations_count for r in reports),
        'tickets_handled': sum(r.tickets_handled for r in reports),
        'tickets_closed': sum(r.tickets_closed for r in reports),
        'tickets_escalated': sum(r.tickets_escalated for r in reports)
    }
    
    # Find missing reports
    submitted_agent_ids = [r.agent_id for r in reports]
    missing_agents = [a for a in agents if a.id not in submitted_agent_ids]
    
    return render_template('summary.html', 
                         reports=reports, 
                         totals=totals, 
                         week_ending=week_ending_date,
                         missing_agents=missing_agents)

@app.route('/summary/export')
def export_summary():
    """Export summary as CSV."""
    week_ending = request.args.get('week', get_week_ending().isoformat())
    week_ending_date = datetime.strptime(week_ending, '%Y-%m-%d').date()
    
    reports = WeeklyReport.query.filter_by(week_ending=week_ending_date).all()
    
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Agent', 'Sites Visited', 'Sites Monitored', 'APs Offline', 
                    'Network Issues', 'Migrations', 'Tickets Handled', 'Tickets Closed'])
    
    for r in reports:
        writer.writerow([r.agent.name, r.sites_visited, r.sites_monitored, 
                        r.aps_offline_total, r.network_issues_count, r.migrations_count,
                        r.tickets_handled, r.tickets_closed])
    
    from flask import Response
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=summary_{week_ending}.csv'}
    )

# ----- Quick Entry (Dashboard) -----

@app.route('/quick-entry', methods=['GET', 'POST'])
def quick_entry():
    """Quick entry form for daily logging."""
    agents = Agent.query.filter_by(active=True).all()
    
    if request.method == 'POST':
        agent_id = request.form['agent_id']
        date = datetime.strptime(request.form['date'], '%Y-%m-%d').date()
        
        # Site visit
        if request.form.get('site_visited'):
            visit = SiteVisit(
                agent_id=agent_id,
                date=date,
                site_name=request.form['site_visited'],
                discussion_topics=request.form.get('visit_notes', '')
            )
            db.session.add(visit)
        
        # Monitoring
        if request.form.get('site_monitored'):
            log = MonitoringLog(
                agent_id=agent_id,
                date=date,
                site_name=request.form['site_monitored'],
                aps_offline=int(request.form.get('aps_offline', 0)),
                aps_total=int(request.form.get('aps_total', 0)),
                network_issues='network_issues' in request.form,
                site_migration='site_migration' in request.form,
                notes=request.form.get('monitoring_notes', ''),
                resolution_status=request.form.get('resolution_status', 'resolved')
            )
            db.session.add(log)
        
        # Tickets
        tickets_handled = int(request.form.get('tickets_handled', 0))
        tickets_closed = int(request.form.get('tickets_closed', 0))
        if tickets_handled > 0:
            call = ServiceCall(
                agent_id=agent_id,
                date=date,
                ticket_number=f"DAILY-{date.strftime('%Y%m%d')}",
                summary=f"Daily total: {tickets_handled} handled, {tickets_closed} closed",
                status='closed' if tickets_handled == tickets_closed else 'open'
            )
            db.session.add(call)
        
        db.session.commit()
        flash('Daily entry saved!', 'success')
        return redirect(url_for('quick_entry'))
    
    return render_template('quick_entry.html', agents=agents, today=datetime.now().strftime('%Y-%m-%d'))

# ============== Initialize ==============

def init_db():
    """Initialize the database."""
    with app.app_context():
        db.create_all()
        # Add sample agent if none exist
        if Agent.query.count() == 0:
            sample = Agent(name='Sample Agent', email='sample@example.com')
            db.session.add(sample)
            db.session.commit()
            print("Created sample agent. Add your real agents via the web interface.")

if __name__ == '__main__':
    init_db()
    # Run on all interfaces so it's accessible from other devices on your network
    print("\n" + "="*60)
    print("  Support Engineer Report Tracking System")
    print("="*60)
    print("\n  Access locally:    http://localhost:5000")
    print("  Access on network: http://YOUR_IP:5000")
    print("\n  Press Ctrl+C to stop the server")
    print("="*60 + "\n")
    app.run(host='0.0.0.0', port=5000, debug=True)
