#!/usr/bin/env python3
"""
Exporter Monitoring Dashboard Generator
=======================================
Queries the database to gather status information and renders a static HTML
dashboard.

Each of the 5 parallel Render/GitHub Actions instances sets an INSTANCE_INDEX
env var (0-4). When set, this generates a dashboard scoped to THAT instance's
own ticker slice - its own progress, its own history, its own failures - with
just a compact one-line summary of the other 4 instances for context (not
their full ticker lists). This is what each instance's own web server serves.

Run without INSTANCE_INDEX (e.g. locally) to get the full multi-instance
overview instead.
"""

import os
import sys
import math
import logging
from collections import defaultdict
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

TODAY_STALE_THRESHOLD_DAYS = 2


def build_instance_partitions(tickers, total_instances):
    """Alphabetically slice `tickers` into `total_instances` chunks (same logic
    as cron_update_runner.run_sync_cycle - keep these in sync)."""
    chunk_size = (len(tickers) + total_instances - 1) // total_instances if tickers else 0
    partitions = []
    for idx in range(total_instances):
        start = idx * chunk_size
        end = min(start + chunk_size, len(tickers))
        chunk = tickers[start:end]
        partitions.append({
            'idx': idx,
            'tickers': chunk,
            'range_label': f"{chunk[0]} - {chunk[-1]}" if chunk else "EMPTY",
        })
    return partitions


def summarize_partition(ticker_list, stocks_map, today_str):
    """Compute synced/pending/failed/stale/success-rate for one set of tickers."""
    synced = failed = pending = 0
    stale = []
    statuses = {}  # symbol -> 'good' | 'critical' | 'pending', for chip rendering
    for t in ticker_list:
        info = stocks_map.get(t)
        last_sync = info.get('last_sync_date') if info else None
        status = (info.get('last_sync_status') or '').upper() if info else ''

        if not last_sync:
            pending += 1
            statuses[t] = 'pending'
            continue

        last_sync_str = last_sync.strftime('%Y-%m-%d') if hasattr(last_sync, 'strftime') else str(last_sync)

        if last_sync_str.startswith(today_str):
            if status in ('SUCCESS', 'PARTIAL'):
                synced += 1
                statuses[t] = 'good'
            else:
                failed += 1
                statuses[t] = 'critical'
        else:
            pending += 1
            statuses[t] = 'pending'
            try:
                last_date = last_sync.date() if hasattr(last_sync, 'date') else datetime.strptime(last_sync_str, '%Y-%m-%d').date()
                days_stale = (date.today() - last_date).days
                if days_stale >= TODAY_STALE_THRESHOLD_DAYS:
                    stale.append({'symbol': t, 'last_sync': last_sync_str, 'days_stale': days_stale})
            except Exception:
                pass

    success_rate = (synced / (synced + failed) * 100) if (synced + failed) > 0 else None
    return {
        'total': len(ticker_list),
        'synced': synced,
        'failed': failed,
        'pending': pending,
        'success_rate': success_rate,
        'stale': sorted(stale, key=lambda x: -x['days_stale'])[:15],
        'statuses': statuses,
    }


def fetch_history(db, stock_ids=None, limit=20):
    query = """
        SELECT
            DATE(l.sync_date) as sync_day,
            l.sync_status,
            COUNT(*) as count,
            AVG(l.sync_duration_seconds) as avg_dur,
            MAX(l.sync_duration_seconds) as max_dur
        FROM data_sync_log l
    """
    params = []
    if stock_ids is not None:
        query += " WHERE l.stock_id = ANY(%s)"
        params.append(stock_ids)
    query += " GROUP BY sync_day, l.sync_status ORDER BY sync_day DESC, l.sync_status LIMIT %s;"
    params.append(limit)

    with db.connection.cursor() as cursor:
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()

    return [{
        'sync_day': str(r[0]),
        'status': r[1],
        'count': r[2],
        'avg_dur': float(r[3] or 0),
        'max_dur': float(r[4] or 0),
    } for r in rows]


def fetch_failures(db, stock_ids=None, limit=20):
    query = """
        SELECT l.sync_date, s.symbol, l.sync_type, l.error_message
        FROM data_sync_log l
        LEFT JOIN stocks s ON l.stock_id = s.id
        WHERE (l.sync_status = 'FAILED' OR l.sync_status = 'failed')
    """
    params = []
    if stock_ids is not None:
        query += " AND l.stock_id = ANY(%s)"
        params.append(stock_ids)
    query += " ORDER BY l.sync_date DESC LIMIT %s;"
    params.append(limit)

    with db.connection.cursor() as cursor:
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()

    return [{
        'sync_date': r[0].strftime('%Y-%m-%d %H:%M:%S') if r[0] else 'N/A',
        'symbol': r[1] or 'Unknown',
        'sync_type': r[2],
        'error_message': r[3] or 'No details',
    } for r in rows]


def fetch_at_risk_tickers(db, symbols):
    """Tickers with 1-2 consecutive hard failures - one more away from auto-prune."""
    if not symbols:
        return []
    try:
        res = (
            db.table('monitored_tickers')
            .select('symbol', 'consecutive_failures')
            .in_('symbol', symbols)
            .execute()
        )
        rows = [r for r in (res.data or []) if (r.get('consecutive_failures') or 0) > 0]
        rows.sort(key=lambda r: -r['consecutive_failures'])
        return rows[:15]
    except Exception as e:
        logger.warning(f"Could not fetch at-risk tickers (consecutive_failures column may not exist yet): {e}")
        return []


# =====================================================================================
# RENDERING - dark-first, ring/chip/bar-chart dashboard (see dataviz skill for the
# palette + mark specs this follows: status colors are reserved & always icon+label
# paired, the hero ring is the one >=48px figure per view, bars are 24px thin with
# 4px rounded caps and 2px gaps, a raw <details> table backs every chart).
# =====================================================================================

def _ring_svg(pct, size=196, stroke=16, value_label="", sub_label="", accent="var(--accent)"):
    pct = max(0, min(100, pct))
    r = (size - stroke) / 2
    c = 2 * math.pi * r
    offset = c * (1 - pct / 100)
    cx = cy = size / 2
    return f"""
    <div class="ring-wrap" style="width:{size}px;height:{size}px;">
        <svg width="{size}" height="{size}" viewBox="0 0 {size} {size}" class="ring-svg">
            <circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="var(--ring-track)" stroke-width="{stroke}" />
            <circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="{accent}" stroke-width="{stroke}"
                stroke-linecap="round" stroke-dasharray="{c:.2f}" stroke-dashoffset="{offset:.2f}"
                transform="rotate(-90 {cx} {cy})">
                <title>{value_label} {sub_label}</title>
            </circle>
        </svg>
        <div class="ring-center">
            <div class="ring-value">{value_label}</div>
            <div class="ring-sub">{sub_label}</div>
        </div>
    </div>"""


def _mini_ring_svg(pct, size=72, stroke=7):
    pct = max(0, min(100, pct))
    r = (size - stroke) / 2
    c = 2 * math.pi * r
    offset = c * (1 - pct / 100)
    cx = cy = size / 2
    tone = 'good' if pct >= 90 else ('warning' if pct >= 40 else 'critical')
    return f"""<svg width="{size}" height="{size}" viewBox="0 0 {size} {size}" class="mini-ring">
        <circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="var(--ring-track)" stroke-width="{stroke}" />
        <circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="var(--status-{tone})" stroke-width="{stroke}"
            stroke-linecap="round" stroke-dasharray="{c:.2f}" stroke-dashoffset="{offset:.2f}"
            transform="rotate(-90 {cx} {cy})" />
        <text x="{cx}" y="{cy + 4}" text-anchor="middle" class="mini-ring-text">{pct:.0f}%</text>
    </svg>"""


def render_sibling_row(part, summary):
    pct = (summary['synced'] / summary['total'] * 100) if summary['total'] > 0 else 0
    return f"""
            <div class="sibling-tile">
                {_mini_ring_svg(pct)}
                <div class="sibling-meta">
                    <div class="sibling-idx">Instance {part['idx']}</div>
                    <div class="sibling-range">{part['range_label']}</div>
                    <div class="sibling-counts">
                        <span class="dot dot-good"></span>{summary['synced']}
                        <span class="dot dot-pending"></span>{summary['pending']}
                        <span class="dot dot-critical"></span>{summary['failed']}
                    </div>
                </div>
            </div>"""


def _ticker_chip_cloud(tickers, statuses):
    chip_class = {'good': 'chip-good', 'critical': 'chip-critical', 'pending': 'chip-pending'}
    chips = []
    for t in tickers:
        tone = statuses.get(t, 'pending')
        chips.append(f'<span class="chip {chip_class[tone]}" title="{t}: {tone}">{t}</span>')
    return ''.join(chips)


def render_instance_block(part, summary, highlight=False):
    pct = (summary['synced'] / summary['total'] * 100) if summary['total'] > 0 else 0
    rate = f"{summary['success_rate']:.0f}%" if summary['success_rate'] is not None else "—"
    chips = _ticker_chip_cloud(part['tickers'], summary['statuses'])
    open_attr = " open" if highlight else ""
    label = f"Instance {part['idx']}{' — this dashboard' if highlight else ''}"
    css_class = "instance-block highlight" if highlight else "instance-block"
    return f"""
        <details class="{css_class}"{open_attr}>
            <summary>
                <span class="instance-summary-left">
                    <span class="instance-dot" style="background:var(--cat-{part['idx'] % 5})"></span>
                    <strong>{label}</strong>
                    <span class="instance-range">{part['range_label']}</span>
                </span>
                <span class="instance-summary-right">
                    <span class="mini-bar-bg"><span class="mini-bar-fill" style="width:{pct:.0f}%"></span></span>
                    {pct:.0f}% &bull; {summary['synced']}/{summary['total']} &bull; rate {rate}
                </span>
            </summary>
            <div class="chip-cloud">{chips}</div>
        </details>"""


def _history_chart(history_rows, max_days=7):
    by_day = defaultdict(lambda: {'good': 0, 'critical': 0, 'other': 0})
    for h in history_rows:
        status = h['status'].upper()
        bucket = 'good' if status in ('SUCCESS', 'PARTIAL') else ('critical' if status == 'FAILED' else 'other')
        by_day[h['sync_day']][bucket] += h['count']

    days = sorted(by_day.keys())[-max_days:]
    if not days:
        return '<div class="empty-state">No sync log history yet.</div>'

    max_total = max((v['good'] + v['critical'] + v['other']) for v in by_day.values()) or 1
    chart_h = 100
    bar_w = 12
    inner_gap = 3
    group_w = bar_w * 2 + inner_gap
    group_gap = 22
    svg_w = len(days) * (group_w + group_gap)

    bars = []
    labels = []
    for i, d in enumerate(days):
        v = by_day[d]
        gx = i * (group_w + group_gap)
        good_h = round(v['good'] / max_total * chart_h, 1)
        crit_h = round(v['critical'] / max_total * chart_h, 1)
        bars.append(
            f'<rect x="{gx}" y="{chart_h - good_h:.1f}" width="{bar_w}" height="{good_h:.1f}" rx="4" class="bar-good"><title>{d}: {v["good"]} synced</title></rect>'
        )
        bars.append(
            f'<rect x="{gx + bar_w + inner_gap}" y="{chart_h - crit_h:.1f}" width="{bar_w}" height="{crit_h:.1f}" rx="4" class="bar-critical"><title>{d}: {v["critical"]} failed</title></rect>'
        )
        labels.append(f'<text x="{gx + group_w / 2 - inner_gap / 2}" y="{chart_h + 18}" text-anchor="middle" class="bar-day-label">{d[5:]}</text>')

    return f"""
        <div class="legend-row">
            <span><span class="dot dot-good"></span>Synced</span>
            <span><span class="dot dot-critical"></span>Failed</span>
        </div>
        <svg viewBox="0 0 {svg_w} {chart_h + 26}" class="history-chart" preserveAspectRatio="xMinYMid meet">
            <line x1="0" y1="{chart_h}" x2="{svg_w}" y2="{chart_h}" class="chart-baseline" />
            {''.join(bars)}
            {''.join(labels)}
        </svg>"""


def generate_html(data, ctx):
    today_str = datetime.now().strftime("%B %d, %Y")
    generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    summary = data['summary']

    scope_label = f"Instance {ctx['instance_idx']} of {ctx['total_instances']}" if ctx['scoped'] else f"All {ctx['total_instances']} instances &bull; overview"
    hero_pct = (summary['synced'] / summary['total'] * 100) if summary['total'] > 0 else 0
    rate_label = f"{summary['success_rate']:.0f}%" if summary['success_rate'] is not None else "—"

    # ---- Hero ring + stat tiles ----
    hero_ring = _ring_svg(
        hero_pct,
        value_label=f"{hero_pct:.0f}%",
        sub_label=f"{summary['synced']} / {summary['total']} synced today",
    )
    avg_sync_label = f"{data['avg_sync_seconds']:.1f}s" if data['avg_sync_seconds'] is not None else "—"
    stat_tiles = "".join([
        f'<div class="tile"><div class="tile-label">Pending</div><div class="tile-val tone-warning">{summary["pending"]}</div></div>',
        f'<div class="tile"><div class="tile-label">Failed today</div><div class="tile-val tone-critical">{summary["failed"]}</div></div>',
        f'<div class="tile"><div class="tile-label">Success rate</div><div class="tile-val tone-good">{rate_label}</div></div>',
        f'<div class="tile"><div class="tile-label">Avg sync time</div><div class="tile-val">{avg_sync_label}</div></div>',
    ])

    # ---- Own ticker chips (scoped) or nothing (overview shows per-instance below) ----
    own_chips_section = ""
    if ctx['scoped'] and data['my_partition']:
        chips = _ticker_chip_cloud(data['my_partition']['tickers'], summary['statuses'])
        own_chips_section = f"""
        <h2 class="section-title">My Tickers <span class="section-count">({summary['total']})</span></h2>
        <div class="legend-row">
            <span><span class="dot dot-good"></span>Synced</span>
            <span><span class="dot dot-pending"></span>Pending</span>
            <span><span class="dot dot-critical"></span>Failed</span>
        </div>
        <div class="chip-cloud panel">{chips}</div>
        """

    # ---- Sibling instances ----
    if ctx['scoped']:
        siblings_section = f"""
        <h2 class="section-title">Other Instances</h2>
        <div class="sibling-row">
            {''.join(render_sibling_row(p, s) for p, s in data['siblings'])}
        </div>
        """
    else:
        siblings_section = f"""
        <h2 class="section-title">All Instances</h2>
        <div class="instance-block-list">
            {''.join(render_instance_block(p, s) for p, s in data['siblings'])}
        </div>
        """

    # ---- Stale / at-risk feed ----
    if not data['stale']:
        stale_html = '<div class="feed-row empty-state"><span class="text-good">✓</span> Nothing is stale &mdash; everything synced within the last day.</div>'
    else:
        stale_html = ''.join(
            f"""<div class="feed-row">
                <span class="status-chip chip-critical-solid">{s['days_stale']}d</span>
                <strong>{s['symbol']}</strong>
                <span class="feed-sub">last synced {s['last_sync']}</span>
            </div>""" for s in data['stale']
        )

    if not data['at_risk']:
        risk_html = '<div class="feed-row empty-state"><span class="text-good">✓</span> No tickers close to auto-pruning.</div>'
    else:
        risk_html = ''.join(
            f"""<div class="feed-row">
                <span class="status-chip chip-warning-solid">{r['consecutive_failures']}/3</span>
                <strong>{r['symbol']}</strong>
                <span class="feed-sub">consecutive hard failures</span>
            </div>""" for r in data['at_risk']
        )

    # ---- History chart + raw table fallback ----
    history_chart_html = _history_chart(data['history'])
    history_raw_rows = ''.join(
        f"<tr><td>{h['sync_day']}</td><td>{h['status']}</td><td>{h['count']}</td><td>{h['avg_dur']:.1f}s / {h['max_dur']:.1f}s</td></tr>"
        for h in data['history']
    ) or '<tr><td colspan="4">No data</td></tr>'

    # ---- Failures feed ----
    if not data['failures']:
        failures_html = '<div class="feed-row empty-state"><span class="text-good">✓</span> No sync errors in the last 20 operations.</div>'
    else:
        failures_html = ''.join(
            f"""<div class="feed-row feed-row-error">
                <span class="status-chip chip-critical-solid">{err['sync_type']}</span>
                <strong>{err['symbol']}</strong>
                <span class="feed-sub">{err['sync_date']}</span>
                <div class="error-msg">{err['error_message']}</div>
            </div>""" for err in data['failures']
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TickZen &mdash; {scope_label}</title>
<style>
:root {{
    color-scheme: dark;
    --page: #0d0d0d;
    --surface: #1a1a19;
    --surface-2: #202020;
    --ink: #ffffff;
    --ink-secondary: #c3c2b7;
    --ink-muted: #898781;
    --border: rgba(255,255,255,0.10);
    --gridline: #2c2c2a;
    --ring-track: #2c2c2a;
    --accent: #3987e5;
    --accent-soft: rgba(57,135,229,0.14);
    --status-good: #0ca30c;
    --status-good-soft: rgba(12,163,12,0.16);
    --status-warning: #fab219;
    --status-warning-soft: rgba(250,178,25,0.16);
    --status-critical: #e66767;
    --status-critical-soft: rgba(230,103,103,0.16);
    --cat-0: #3987e5;
    --cat-1: #9085e9;
    --cat-2: #d55181;
    --cat-3: #c98500;
    --cat-4: #199e70;
}}
@media (prefers-color-scheme: light) {{
    :root {{
        color-scheme: light;
        --page: #f9f9f7;
        --surface: #fcfcfb;
        --surface-2: #f1f0ec;
        --ink: #0b0b0b;
        --ink-secondary: #52514e;
        --ink-muted: #898781;
        --border: rgba(11,11,11,0.10);
        --gridline: #e1e0d9;
        --ring-track: #e1e0d9;
        --accent: #2a78d6;
        --accent-soft: rgba(42,120,214,0.10);
        --status-good: #0ca30c;
        --status-good-soft: rgba(12,163,12,0.12);
        --status-warning: #fab219;
        --status-warning-soft: rgba(250,178,25,0.20);
        --status-critical: #d03b3b;
        --status-critical-soft: rgba(208,59,59,0.12);
        --cat-0: #2a78d6;
        --cat-1: #4a3aa7;
        --cat-2: #e87ba4;
        --cat-3: #eda100;
        --cat-4: #1baf7a;
    }}
}}
* {{ box-sizing: border-box; }}
body {{
    margin: 0; padding: 0 0 80px 0;
    background: var(--page); color: var(--ink);
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    line-height: 1.5;
}}
.wrap {{ max-width: 1180px; margin: 0 auto; padding: 0 24px; }}
header {{
    background: var(--surface); border-bottom: 1px solid var(--border);
    padding: 22px 0; margin-bottom: 32px;
}}
header .wrap {{ display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; }}
.brand {{ display: flex; align-items: center; gap: 10px; font-weight: 700; font-size: 1.05rem; letter-spacing: -0.01em; }}
.brand-mark {{ width: 10px; height: 10px; border-radius: 3px; background: var(--accent); }}
.scope-pill {{ font-size: 0.72rem; font-weight: 700; color: var(--accent); background: var(--accent-soft); padding: 4px 12px; border-radius: 999px; }}
.header-meta {{ text-align: right; font-size: 0.75rem; color: var(--ink-muted); }}

.hero {{ display: grid; grid-template-columns: auto 1fr; gap: 32px; align-items: center; margin-bottom: 40px; }}
.ring-wrap {{ position: relative; }}
.ring-svg {{ display: block; }}
.ring-center {{ position: absolute; inset: 0; display: flex; flex-direction: column; align-items: center; justify-content: center; text-align: center; }}
.ring-value {{ font-size: 2.6rem; font-weight: 800; letter-spacing: -0.03em; }}
.ring-sub {{ font-size: 0.72rem; color: var(--ink-muted); max-width: 130px; margin-top: 2px; }}
.tiles {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 14px; }}
.tile {{ background: var(--surface); border: 1px solid var(--border); border-radius: 14px; padding: 16px 18px; }}
.tile-label {{ font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.06em; color: var(--ink-muted); font-weight: 700; }}
.tile-val {{ font-size: 1.6rem; font-weight: 700; margin-top: 6px; letter-spacing: -0.02em; }}
.tile-val.tone-warning {{ color: var(--status-warning); }}
.tile-val.tone-critical {{ color: var(--status-critical); }}
.tile-val.tone-good {{ color: var(--status-good); }}

.section-title {{ font-size: 1rem; font-weight: 700; margin: 0 0 14px 0; letter-spacing: -0.01em; }}
.section-count {{ color: var(--ink-muted); font-weight: 500; }}

.legend-row {{ display: flex; gap: 18px; font-size: 0.75rem; color: var(--ink-secondary); margin-bottom: 10px; }}
.legend-row span {{ display: inline-flex; align-items: center; gap: 6px; }}
.dot {{ width: 8px; height: 8px; border-radius: 50%; display: inline-block; }}
.dot-good {{ background: var(--status-good); }}
.dot-pending {{ background: var(--ink-muted); }}
.dot-critical {{ background: var(--status-critical); }}

.chip-cloud {{ display: flex; flex-wrap: wrap; gap: 6px; }}
.chip-cloud.panel {{ background: var(--surface); border: 1px solid var(--border); border-radius: 14px; padding: 16px; max-height: 220px; overflow-y: auto; margin-bottom: 36px; }}
.chip {{
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.72rem; font-weight: 600;
    padding: 4px 9px; border-radius: 7px; border: 1px solid transparent;
}}
.chip-good {{ background: var(--status-good-soft); color: var(--status-good); }}
.chip-pending {{ background: var(--surface-2); color: var(--ink-muted); border-color: var(--border); }}
.chip-critical {{ background: var(--status-critical-soft); color: var(--status-critical); }}

.sibling-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-bottom: 40px; }}
.sibling-tile {{
    background: var(--surface); border: 1px solid var(--border); border-radius: 14px;
    padding: 14px; display: flex; align-items: center; gap: 14px;
}}
.mini-ring {{ flex-shrink: 0; }}
.mini-ring-text {{ font-size: 15px; font-weight: 700; fill: var(--ink); font-family: system-ui, sans-serif; }}
.sibling-idx {{ font-weight: 700; font-size: 0.85rem; }}
.sibling-range {{ font-size: 0.7rem; color: var(--ink-muted); margin: 2px 0 6px 0; }}
.sibling-counts {{ font-size: 0.72rem; color: var(--ink-secondary); display: flex; gap: 8px; align-items: center; }}

.instance-block-list {{ display: flex; flex-direction: column; gap: 10px; margin-bottom: 40px; }}
.instance-block {{ background: var(--surface); border: 1px solid var(--border); border-radius: 14px; overflow: hidden; }}
.instance-block.highlight {{ border-color: var(--accent); }}
.instance-block summary {{
    list-style: none; cursor: pointer; padding: 14px 18px; display: flex; justify-content: space-between;
    align-items: center; flex-wrap: wrap; gap: 10px; font-size: 0.82rem;
}}
.instance-block summary::-webkit-details-marker {{ display: none; }}
.instance-summary-left {{ display: flex; align-items: center; gap: 10px; }}
.instance-dot {{ width: 9px; height: 9px; border-radius: 3px; }}
.instance-range {{ font-size: 0.7rem; color: var(--ink-muted); background: var(--surface-2); padding: 2px 9px; border-radius: 999px; }}
.instance-summary-right {{ display: flex; align-items: center; gap: 10px; color: var(--ink-secondary); font-size: 0.75rem; }}
.mini-bar-bg {{ width: 80px; height: 6px; border-radius: 4px; background: var(--ring-track); display: inline-block; overflow: hidden; }}
.mini-bar-fill {{ display: block; height: 100%; background: var(--accent); }}
.instance-block .chip-cloud {{ padding: 0 18px 16px 18px; max-height: 140px; overflow-y: auto; }}

.two-col {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(380px, 1fr)); gap: 20px; margin-bottom: 40px; }}
.panel-card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 14px; padding: 18px; }}
.feed-row {{
    display: flex; align-items: center; gap: 10px; padding: 10px 0; border-bottom: 1px solid var(--border);
    font-size: 0.82rem; flex-wrap: wrap;
}}
.feed-row:last-child {{ border-bottom: none; }}
.feed-row-error {{ flex-direction: column; align-items: flex-start; gap: 4px; }}
.feed-sub {{ color: var(--ink-muted); font-size: 0.72rem; margin-left: auto; }}
.status-chip {{ font-size: 0.68rem; font-weight: 800; padding: 2px 8px; border-radius: 6px; text-transform: uppercase; letter-spacing: 0.03em; }}
.chip-critical-solid {{ background: var(--status-critical-soft); color: var(--status-critical); }}
.chip-warning-solid {{ background: var(--status-warning-soft); color: var(--status-warning); }}
.error-msg {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.72rem; color: var(--status-critical); width: 100%; }}
.empty-state {{ color: var(--ink-muted); padding: 6px 0; }}
.text-good {{ color: var(--status-good); }}

.history-chart {{ width: 100%; height: auto; overflow: visible; }}
.bar-good {{ fill: var(--status-good); }}
.bar-critical {{ fill: var(--status-critical); }}
.chart-baseline {{ stroke: var(--gridline); stroke-width: 1; }}
.bar-day-label {{ font-size: 9px; fill: var(--ink-muted); font-family: system-ui, sans-serif; }}

details.raw-table summary {{ cursor: pointer; font-size: 0.75rem; color: var(--ink-muted); margin-top: 10px; }}
table.raw {{ width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.75rem; }}
table.raw th, table.raw td {{ text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--border); color: var(--ink-secondary); }}

.footer {{ text-align: center; margin-top: 30px; padding-top: 18px; border-top: 1px solid var(--border); color: var(--ink-muted); font-size: 0.75rem; }}

@media (max-width: 640px) {{
    .hero {{ grid-template-columns: 1fr; justify-items: center; text-align: center; }}
}}
</style>
</head>
<body>
<header>
    <div class="wrap">
        <div class="brand">
            <span class="brand-mark"></span> TickZen Exporter
            <span class="scope-pill">{scope_label}</span>
        </div>
        <div class="header-meta">
            {ctx['db_type']}<br>{today_str} &bull; generated {generated_at}
        </div>
    </div>
</header>

<div class="wrap">

    <div class="hero">
        {hero_ring}
        <div class="tiles">{stat_tiles}</div>
    </div>

    {own_chips_section}
    {siblings_section}

    <div class="two-col">
        <div class="panel-card">
            <h2 class="section-title">⚠ Stale ({TODAY_STALE_THRESHOLD_DAYS}+ days)</h2>
            {stale_html}
        </div>
        <div class="panel-card">
            <h2 class="section-title">🛑 At Risk of Auto-Prune</h2>
            {risk_html}
        </div>
    </div>

    <h2 class="section-title">Sync History &mdash; last 7 days</h2>
    <div class="panel-card" style="margin-bottom: 40px;">
        {history_chart_html}
        <details class="raw-table">
            <summary>View raw log</summary>
            <table class="raw">
                <thead><tr><th>Date</th><th>Status</th><th>Tickers</th><th>Duration (avg/max)</th></tr></thead>
                <tbody>{history_raw_rows}</tbody>
            </table>
        </details>
    </div>

    <h2 class="section-title">Recent Failures</h2>
    <div class="panel-card" style="margin-bottom: 20px;">
        {failures_html}
    </div>

    <div class="footer">
        Generated automatically on {generated_at} &bull; TickZen Stock Database Exporter
    </div>
</div>
</body>
</html>
"""


def main():
    logger.info("Connecting to database to compile dashboard statistics...")
    try:
        db = AzurePostgresClient()

        # 1. Determine scope: is this a specific instance, or the full overview?
        total_instances = int(os.getenv("TOTAL_INSTANCES", "5") or 5)
        instance_index_env = os.getenv("INSTANCE_INDEX")
        instance_index = int(instance_index_env) if instance_index_env is not None else None
        scoped = instance_index is not None

        # 2. Fetch active tickers (all instances - needed to compute partitions consistently)
        res = db.table('monitored_tickers').select('symbol').eq('is_active', True).execute()
        if not hasattr(res, 'data') or not res.data:
            logger.error("No active tickers found in monitored_tickers table.")
            sys.exit(1)

        tickers = sorted({r['symbol'].upper() for r in res.data if r.get('symbol')})
        partitions = build_instance_partitions(tickers, total_instances)

        # 3. Fetch stock sync state, scoped to just the tickers we actually need
        stocks_res = (
            db.table('stocks')
            .select('id', 'symbol', 'last_sync_date', 'last_sync_status')
            .in_('symbol', tickers)
            .execute()
        )
        stocks_map = {}
        symbol_to_id = {}
        for s in (stocks_res.data or []):
            sym = s['symbol'].upper()
            stocks_map[sym] = {'last_sync_date': s.get('last_sync_date'), 'last_sync_status': s.get('last_sync_status')}
            symbol_to_id[sym] = s.get('id')

        today_str = date.today().isoformat()

        # 4. Compute per-instance summaries
        summaries = [summarize_partition(p['tickers'], stocks_map, today_str) for p in partitions]

        if scoped:
            my_partition = partitions[instance_index]
            my_summary = summaries[instance_index]
            siblings = [(p, s) for i, (p, s) in enumerate(zip(partitions, summaries)) if i != instance_index]
            scoped_symbols = my_partition['tickers']
            scoped_ids = [symbol_to_id[t] for t in scoped_symbols if t in symbol_to_id] or None
        else:
            my_partition = None
            merged_statuses = {}
            for s in summaries:
                merged_statuses.update(s['statuses'])
            my_summary = {
                'total': len(tickers),
                'synced': sum(s['synced'] for s in summaries),
                'failed': sum(s['failed'] for s in summaries),
                'pending': sum(s['pending'] for s in summaries),
                'success_rate': None,
                'stale': sorted(
                    [row for s in summaries for row in s['stale']],
                    key=lambda x: -x['days_stale']
                )[:15],
                'statuses': merged_statuses,
            }
            if (my_summary['synced'] + my_summary['failed']) > 0:
                my_summary['success_rate'] = my_summary['synced'] / (my_summary['synced'] + my_summary['failed']) * 100
            siblings = list(zip(partitions, summaries))
            scoped_symbols = tickers
            scoped_ids = None  # None = no filter = global

        # 5. Sync history + failures, scoped to this instance's stock IDs when applicable
        history = fetch_history(db, stock_ids=scoped_ids)
        failures = fetch_failures(db, stock_ids=scoped_ids)
        avg_sync_seconds = None
        if history:
            weighted = [(h['avg_dur'], h['count']) for h in history if h['avg_dur']]
            if weighted:
                avg_sync_seconds = sum(d * c for d, c in weighted) / sum(c for _, c in weighted)

        # 6. At-risk tickers (near auto-prune threshold)
        at_risk = fetch_at_risk_tickers(db, scoped_symbols)

        dashboard_data = {
            'summary': my_summary,
            'my_partition': my_partition,
            'siblings': siblings,
            'history': history,
            'failures': failures,
            'stale': my_summary['stale'],
            'at_risk': at_risk,
            'avg_sync_seconds': avg_sync_seconds,
        }

        ctx = {
            'scoped': scoped,
            'instance_idx': instance_index,
            'total_instances': total_instances,
            'db_type': 'Azure PostgreSQL',
        }

        html_content = generate_html(dashboard_data, ctx)
        out_path = project_root / 'dashboard.html'
        with open(out_path, 'w') as f:
            f.write(html_content)

        logger.info(f"✅ Dashboard generated successfully at: {out_path} (scope={'instance ' + str(instance_index) if scoped else 'overview'})")

    except Exception as e:
        logger.error(f"❌ Failed to generate dashboard: {e}")
        logger.exception("Traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
