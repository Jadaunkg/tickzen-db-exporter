#!/usr/bin/env python3
"""
Exporter Monitoring Dashboard Generator
=======================================
Queries the database to gather status information about the 5 instances,
today's sync progress, history, and failures, then generates a simple static HTML dashboard.
"""

import os
import sys
import logging
from datetime import datetime, date
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from database.azure_postgres_client import AzurePostgresClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def generate_html(data):
    # Setup status colors and layout with flat styling (no gradients)
    today_str = datetime.now().strftime("%B %d, %Y")
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TickZen Exporter Dashboard</title>
    <style>
        :root {{
            --bg-color: #f8f9fa;
            --card-bg: #ffffff;
            --text-color: #212529;
            --text-muted: #6c757d;
            --border-color: #dee2e6;
            --primary: #0d6efd;
            --success: #198754;
            --warning: #ffc107;
            --danger: #dc3545;
            --pending: #6c757d;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background-color: var(--bg-color);
            color: var(--text-color);
            margin: 0;
            padding: 20px;
            line-height: 1.5;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        
        header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 2px solid var(--border-color);
            padding-bottom: 15px;
            margin-bottom: 25px;
        }}
        
        h1, h2, h3 {{
            margin: 0;
            font-weight: 600;
        }}
        
        .date {{
            color: var(--text-muted);
            font-size: 0.95rem;
        }}
        
        /* Stats Grid */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .stat-card {{
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            padding: 20px;
            text-align: center;
        }}
        
        .stat-val {{
            font-size: 2rem;
            font-weight: bold;
            margin: 10px 0 5px 0;
        }}
        
        .stat-label {{
            color: var(--text-muted);
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        /* Instance List */
        .section-title {{
            margin-bottom: 15px;
            padding-bottom: 5px;
            border-bottom: 1px solid var(--border-color);
        }}
        
        .instances-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        
        .instance-card {{
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-top: 4px solid var(--primary);
            border-radius: 4px;
            padding: 15px;
        }}
        
        .instance-card.active-0 {{ border-top-color: #0d6efd; }}
        .instance-card.active-1 {{ border-top-color: #6610f2; }}
        .instance-card.active-2 {{ border-top-color: #6f42c1; }}
        .instance-card.active-3 {{ border-top-color: #d63384; }}
        .instance-card.active-4 {{ border-top-color: #fd7e14; }}
        
        .instance-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }}
        
        .instance-range {{
            font-size: 0.8rem;
            background: #e9ecef;
            padding: 2px 8px;
            border-radius: 12px;
            color: #495057;
            font-weight: bold;
        }}
        
        .progress-container {{
            margin: 15px 0;
        }}
        
        .progress-bar-bg {{
            background: #e9ecef;
            height: 12px;
            border-radius: 6px;
            overflow: hidden;
        }}
        
        .progress-bar-fill {{
            background: var(--success);
            height: 100%;
            width: 0%;
            transition: width 0.3s ease;
        }}
        
        .progress-text {{
            display: flex;
            justify-content: space-between;
            font-size: 0.8rem;
            color: var(--text-muted);
            margin-top: 5px;
        }}
        
        .ticker-list-box {{
            margin-top: 15px;
            background: #f8f9fa;
            border: 1px solid var(--border-color);
            padding: 8px;
            font-family: monospace;
            font-size: 0.75rem;
            max-height: 80px;
            overflow-y: auto;
            border-radius: 3px;
            word-break: break-all;
        }}
        
        /* Table styles */
        table {{
            width: 100%;
            border-collapse: collapse;
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            overflow: hidden;
            margin-bottom: 30px;
        }}
        
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }}
        
        th {{
            background-color: #f1f3f5;
            font-weight: bold;
            font-size: 0.85rem;
            text-transform: uppercase;
            color: #495057;
        }}
        
        tr:last-child td {{
            border-bottom: none;
        }}
        
        .badge {{
            display: inline-block;
            padding: 3px 8px;
            font-size: 0.75rem;
            font-weight: bold;
            border-radius: 3px;
            text-transform: uppercase;
        }}
        
        .badge.success {{ background: #d1e7dd; color: #0f5132; }}
        .badge.failed {{ background: #f8d7da; color: #842029; }}
        .badge.pending {{ background: #e2e3e5; color: #41464b; }}
        
        .error-msg {{
            color: var(--danger);
            font-family: monospace;
            font-size: 0.8rem;
        }}
        
        .footer {{
            text-align: center;
            margin-top: 40px;
            padding-top: 15px;
            border-top: 1px solid var(--border-color);
            color: var(--text-muted);
            font-size: 0.8rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div>
                <h1>TickZen Exporter Monitoring Dashboard</h1>
                <div class="date">Live Status as of {today_str}</div>
            </div>
            <div style="font-weight: bold; color: var(--primary);">Option A: Database-Driven Sync</div>
        </header>
        
        <!-- Summary Stats -->
        <div class="stats-grid">
            <div class="stat-card" style="border-left: 4px solid var(--primary)">
                <div class="stat-label">Total Active Tickers</div>
                <div class="stat-val" style="color: var(--primary)">{data['total_active']}</div>
            </div>
            <div class="stat-card" style="border-left: 4px solid var(--success)">
                <div class="stat-label">Synced Today</div>
                <div class="stat-val" style="color: var(--success)">{data['synced_today']}</div>
            </div>
            <div class="stat-card" style="border-left: 4px solid var(--warning)">
                <div class="stat-label">Pending Today</div>
                <div class="stat-val" style="color: var(--warning)">{data['pending_today']}</div>
            </div>
            <div class="stat-card" style="border-left: 4px solid var(--danger)">
                <div class="stat-label">Failures Today</div>
                <div class="stat-val" style="color: var(--danger)">{data['failed_today']}</div>
            </div>
        </div>
        
        <!-- Instance Division Workload -->
        <h2 class="section-title">5-Instance Workload Partitioning</h2>
        <div class="instances-grid">
        """
        
    for inst in data['instances']:
        pct = (inst['synced'] / inst['total'] * 100) if inst['total'] > 0 else 0
        html += f"""
            <div class="instance-card active-{inst['idx']}">
                <div class="instance-header">
                    <h3>Instance {inst['idx']}</h3>
                    <span class="instance-range">{inst['range_label']}</span>
                </div>
                <div class="progress-container">
                    <div class="progress-bar-bg">
                        <div class="progress-bar-fill" style="width: {pct:.1f}%"></div>
                    </div>
                    <div class="progress-text">
                        <span>Synced: <strong>{inst['synced']}</strong> / {inst['total']}</span>
                        <span>{pct:.0f}% Done</span>
                    </div>
                </div>
                <div class="progress-text" style="margin-top: 0;">
                    <span style="color: var(--danger)">Failures today: {inst['failed']}</span>
                    <span style="color: var(--warning)">Pending: {inst['pending']}</span>
                </div>
                <div class="ticker-list-box">
                    <strong>Symbols ({len(inst['tickers'])}):</strong><br>
                    {', '.join(inst['tickers'])}
                </div>
            </div>
        """
        
    html += """
        </div>
        
        <!-- Sync logs history -->
        <h2 class="section-title">Recent Sync History (Daily Summary)</h2>
        <table>
            <thead>
                <tr>
                    <th>Date</th>
                    <th>Sync Status</th>
                    <th>Records Processed</th>
                    <th>Duration (Avg/Max)</th>
                </tr>
            </thead>
            <tbody>
    """
    
    if not data['history']:
        html += """
                <tr>
                    <td colspan="4" style="text-align: center; color: var(--text-muted)">No sync logs history found.</td>
                </tr>
        """
    else:
        for row in data['history']:
            status_badge = "success" if row['status'].upper() == "SUCCESS" else ("failed" if row['status'].upper() == "FAILED" else "pending")
            html += f"""
                <tr>
                    <td><strong>{row['sync_day']}</strong></td>
                    <td><span class="badge {status_badge}">{row['status']}</span></td>
                    <td>{row['count']} tickers</td>
                    <td>{row['avg_dur']:.1f}s avg / {row['max_dur']:.1f}s max</td>
                </tr>
            """
            
    html += """
            </tbody>
        </table>
        
        <!-- Recent Failures -->
        <h2 class="section-title">Recent Sync Failures & Errors</h2>
        <table>
            <thead>
                <tr>
                    <th>Time</th>
                    <th>Ticker</th>
                    <th>Sync Type</th>
                    <th>Error Message</th>
                </tr>
            </thead>
            <tbody>
    """
    
    if not data['failures']:
        html += """
                <tr>
                    <td colspan="4" style="text-align: center; color: var(--success); font-weight: bold;">✓ No sync errors found in the last 20 operations!</td>
                </tr>
        """
    else:
        for err in data['failures']:
            html += f"""
                <tr>
                    <td style="white-space: nowrap;">{err['sync_date']}</td>
                    <td><strong>{err['symbol']}</strong></td>
                    <td><span class="badge pending">{err['sync_type']}</span></td>
                    <td class="error-msg">{err['error_message']}</td>
                </tr>
            """
            
    html += f"""
            </tbody>
        </table>
        
        <div class="footer">
            Generated automatically on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} &bull; TickZen Stock Database Exporter
        </div>
    </div>
</body>
</html>
"""
    return html

def main():
    logger.info("Connecting to database to compile statistics...")
    try:
        db = AzurePostgresClient()
        
        # 1. Fetch active tickers
        res = db.table('monitored_tickers').select('symbol').eq('is_active', True).execute()
        if not hasattr(res, 'data') or not res.data:
            logger.error("No active tickers found in monitored_tickers table.")
            sys.exit(1)
            
        tickers = [r['symbol'].upper() for r in res.data if r.get('symbol')]
        tickers.sort()
        
        # 2. Fetch current stock state (today's sync)
        stocks_res = db.table('stocks').select('symbol', 'last_sync_date', 'last_sync_status').execute()
        stocks_map = {}
        for s in stocks_res.data:
            stocks_map[s['symbol'].upper()] = {
                'last_sync_date': s.get('last_sync_date'),
                'last_sync_status': s.get('last_sync_status')
            }
            
        # 3. Calculate instance partitions
        total_instances = 5
        chunk_size = (len(tickers) + total_instances - 1) // total_instances
        
        instances_data = []
        today_str = date.today().isoformat()
        
        global_synced = 0
        global_failed = 0
        global_pending = 0
        
        for idx in range(total_instances):
            start_idx = idx * chunk_size
            end_idx = min(start_idx + chunk_size, len(tickers))
            inst_tickers = tickers[start_idx:end_idx]
            
            # Identify workload range label
            range_label = "EMPTY"
            if inst_tickers:
                range_label = f"{inst_tickers[0]} - {inst_tickers[-1]}"
                
            inst_synced = 0
            inst_failed = 0
            inst_pending = 0
            
            for t in inst_tickers:
                if t in stocks_map:
                    last_sync = stocks_map[t]['last_sync_date']
                    last_status = stocks_map[t]['last_sync_status'] or ''
                    
                    if last_sync:
                        if hasattr(last_sync, 'strftime'):
                            last_sync_str = last_sync.strftime('%Y-%m-%d')
                        else:
                            last_sync_str = str(last_sync)
                            
                        if last_sync_str.startswith(today_str):
                            if last_status.upper() == 'SUCCESS' or last_status.upper() == 'PARTIAL':
                                inst_synced += 1
                                global_synced += 1
                            else:
                                inst_failed += 1
                                global_failed += 1
                        else:
                            inst_pending += 1
                            global_pending += 1
                    else:
                        inst_pending += 1
                        global_pending += 1
                else:
                    # Not in stocks table yet -> Pending
                    inst_pending += 1
                    global_pending += 1
                    
            instances_data.append({
                'idx': idx,
                'range_label': range_label,
                'tickers': inst_tickers,
                'total': len(inst_tickers),
                'synced': inst_synced,
                'failed': inst_failed,
                'pending': inst_pending
            })
            
        # 4. Fetch Sync History (summary of last 15 days)
        # Using raw cursor to do group by and sorting efficiently
        history = []
        with db.connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    DATE(sync_date) as sync_day,
                    sync_status,
                    COUNT(*) as count,
                    AVG(sync_duration_seconds) as avg_dur,
                    MAX(sync_duration_seconds) as max_dur
                FROM data_sync_log
                GROUP BY sync_day, sync_status
                ORDER BY sync_day DESC, sync_status
                LIMIT 20;
            """)
            rows = cursor.fetchall()
            for row in rows:
                history.append({
                    'sync_day': str(row[0]),
                    'status': row[1],
                    'count': row[2],
                    'avg_dur': float(row[3] or 0),
                    'max_dur': float(row[4] or 0)
                })
                
        # 5. Fetch recent failures (last 20 failures)
        failures = []
        with db.connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    l.sync_date,
                    s.symbol,
                    l.sync_type,
                    l.error_message
                FROM data_sync_log l
                LEFT JOIN stocks s ON l.stock_id = s.id
                WHERE l.sync_status = 'FAILED' OR l.sync_status = 'failed'
                ORDER BY l.sync_date DESC
                LIMIT 20;
            """)
            rows = cursor.fetchall()
            for row in rows:
                failures.append({
                    'sync_date': row[0].strftime('%Y-%m-%d %H:%M:%S') if row[0] else 'N/A',
                    'symbol': row[1] or 'Unknown',
                    'sync_type': row[2],
                    'error_message': row[3] or 'No details'
                })
                
        # Aggregate dashboard metrics
        dashboard_data = {
            'total_active': len(tickers),
            'synced_today': global_synced,
            'pending_today': global_pending,
            'failed_today': global_failed,
            'instances': instances_data,
            'history': history,
            'failures': failures
        }
        
        # Generate and save HTML
        html_content = generate_html(dashboard_data)
        out_path = project_root / 'dashboard.html'
        with open(out_path, 'w') as f:
            f.write(html_content)
            
        logger.info(f"✅ Dashboard generated successfully at: {out_path}")
        
    except Exception as e:
        logger.error(f"❌ Failed to generate dashboard: {e}")
        logger.exception("Traceback:")
        sys.exit(1)

if __name__ == "__main__":
    main()
