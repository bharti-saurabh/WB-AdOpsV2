import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import Papa from 'papaparse'
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, Cell,
  Pie, PieChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts'
import {
  Activity, AlertTriangle, Bell, Bot, CheckCircle2,
  ChevronRight, CircleDollarSign, Database, LayoutDashboard,
  MonitorPlay, Search, ShieldAlert, Sparkles, Table2, Terminal,
  TrendingUp, Wrench, X, XCircle, Zap,
} from 'lucide-react'
import './App.css'

// ─── TYPES ──────────────────────────────────────────────────────────────────

type Advertiser = { advertiser_id: string; name: string; industry: string; tier: string }
type Platform   = { platform_id: string; platform_name: string; platform_type: string }
type Campaign   = {
  campaign_id: string; campaign_name: string; advertiser_id: string
  agency_id: string; platform_id: string; segment_id: string
  status: 'Active' | 'Paused' | 'Completed'
  start_date: string; end_date: string; target_impressions: number; cpm_usd: number
}
type PerformanceRow = {
  log_hour: string; campaign_id: string; impressions_delivered: number
  vast_requests: number; vast_responses: number; avg_latency_ms: number
  error_count: number; video_completes: number
}
type AlertRow = {
  alert_id: string; alert_timestamp: string
  severity: 'Critical' | 'High' | 'Medium' | 'Warning'
  alert_type: string; campaign_id: string; trigger_value: string; threshold: string
  expected_impressions: number; actual_impressions: number
  revenue_impact_usd: number; status: string; description?: string
}
type KpiRow = { kpi_name: string; value: string; unit: string; as_of: string }
type EnrichedAlert = AlertRow & {
  campaign_name: string; advertiser_name: string; platform_name: string
  network: 'Streaming' | 'Linear'
}
type ActiveTab = 'overview' | 'health' | 'intel' | 'notifications' | 'explorer'
type DrilldownKey = 'campaigns' | 'delivery' | 'alerts' | 'revenue' | 'pacing' | 'fallout' | null
type ReviewDecision = 'approved' | 'rejected'

// ─── CONSTANTS ──────────────────────────────────────────────────────────────

const PIE_COLORS = ['#FF5800', '#FF8C42', '#FFB347', '#22c55e', '#9EA3B0']
const numberFmt  = new Intl.NumberFormat('en-US')
const compactFmt = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 })
const currencyFmt = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 })

// ─── DEMO PACING DATA (realistic 24h story with events) ─────────────────────

const DEMO_PACING_SERIES = [
  { hour: '00:00', target: 42000,   actual: 5200   },  // Campaign halted — audience sync failure
  { hour: '01:00', target: 84000,   actual: 7100   },  // Still halted — zero ads delivered
  { hour: '02:00', target: 126000,  actual: 8900   },  // Still halted
  { hour: '03:00', target: 168000,  actual: 10800  },  // Still halted
  { hour: '04:00', target: 210000,  actual: 94000  },  // AI fix deployed at 03:47 — delivery jumps
  { hour: '05:00', target: 252000,  actual: 162000 },  // Recovering fast
  { hour: '06:00', target: 294000,  actual: 228000 },  // Recovering
  { hour: '07:00', target: 336000,  actual: 293000 },  // Recovering
  { hour: '08:00', target: 378000,  actual: 354000 },  // Near target
  { hour: '09:00', target: 420000,  actual: 411000 },  // Back on track
  { hour: '10:00', target: 462000,  actual: 453000 },  // On track
  { hour: '11:00', target: 504000,  actual: 493000 },  // FreeWheel timeout starts
  { hour: '12:00', target: 546000,  actual: 493000 },  // Flat — 847 requests failed
  { hour: '13:00', target: 588000,  actual: 541000 },  // Timeout resolved, recovering
  { hour: '14:00', target: 630000,  actual: 586000 },  // Recovery
  { hour: '15:00', target: 672000,  actual: 629000 },  // Recovery
  { hour: '16:00', target: 714000,  actual: 671000 },  // Recovery
  { hour: '17:00', target: 756000,  actual: 714000 },  // Near target
  { hour: '18:00', target: 798000,  actual: 758000 },  // Back on track
  { hour: '19:00', target: 840000,  actual: 836000 },  // Max prime-time surge begins
  { hour: '20:00', target: 882000,  actual: 928000 },  // Surge — above target!
  { hour: '21:00', target: 924000,  actual: 1018000 }, // Peak surge — Max prime-time
  { hour: '22:00', target: 966000,  actual: 1067000 }, // Tapering
  { hour: '23:00', target: 1008000, actual: 1102000 }, // End of day
]

// ─── AGENT PLAYBOOK ─────────────────────────────────────────────────────────

const AGENT_PLAYBOOK: Record<string, {
  steps: { tool: string; icon: string; desc: string; result: string; thought: string; query: string }[]
  rootCause: string; fixType: string; fixCode: string; recoveryTime: string
}> = {
  'Empty Audience Segment': {
    steps: [
      {
        tool: 'query_performance_log', icon: 'search',
        desc: 'Scanning delivery metrics (last 6h)', result: 'impressions_delivered: 0 for 4 consecutive hours',
        thought: 'The alert shows zero impressions. My first step is to confirm the outage window by querying hourly delivery totals — if impressions are consistently zero this rules out a one-off blip and points to a structural break.',
        query: `import pandas as pd\nfrom wb_data import performance_log\n\ndf = performance_log.load(hours=6)\ncampaign_delivery = (\n    df[df['campaign_id'] == CAMPAIGN_ID]\n    .groupby('log_hour')['impressions_delivered']\n    .sum()\n    .reset_index()\n)\nprint(campaign_delivery)`,
      },
      {
        tool: 'query_first_party_segments', icon: 'database',
        desc: 'Checking segment sync status: SEG-001', result: 'sync_status: Failed — last sync 36h ago',
        thought: 'With zero delivery confirmed, the most likely cause is the audience segment failing to populate — no eligible users means no bids win. I\'ll check the segment sync status directly against the LiveRamp data store.',
        query: `from wb_data import first_party_segments\n\nseg = first_party_segments.get(segment_id='SEG-001')\nprint(f"Status: {seg['sync_status']}")\nprint(f"Last sync: {seg['last_sync_ts']}")\nprint(f"Cardinality: {seg['estimated_reach']:,}")`,
      },
      {
        tool: 'query_liveramp_connector_logs', icon: 'terminal',
        desc: 'Fetching LiveRamp connector error telemetry', result: '404_SEGMENT_NOT_FOUND in LiveRamp Identity Graph',
        thought: 'Sync status is Failed. I need to determine why — was it a network error, a schema change, or the segment key being deleted on the upstream side? Pulling connector error logs will tell me the exact error code.',
        query: `from wb_data import liveramp_connector_logs\n\nlogs = liveramp_connector_logs.query(\n    segment_id='SEG-001',\n    since_hours=48,\n    level='ERROR'\n)\nfor log in logs:\n    print(f"[{log['ts']}] {log['error_code']}: {log['message']}")`,
      },
      {
        tool: 'correlate_campaigns', icon: 'activity',
        desc: 'Identifying all campaigns sharing this segment', result: '3 campaigns affected — all delivery halted',
        thought: 'The error is 404 on the identity graph — not a transient network failure. Before generating a fix, I need to know blast radius: how many campaigns reference SEG-001? A shared segment failure could be wider than this single alert.',
        query: `from wb_data import campaigns\n\naffected = campaigns.filter(\n    segment_id='SEG-001',\n    status='Active'\n)\nprint(f"Affected campaigns: {len(affected)}")\nfor c in affected:\n    print(f"  {c['campaign_id']} — {c['campaign_name']}")`,
      },
      {
        tool: 'generate_resolution', icon: 'zap',
        desc: 'Generating fix artifact', result: 'Re-map segment + force sync + replay 4h window',
        thought: 'Three campaigns halted. The fix is: (1) re-map SEG-001 to the v2 identity graph which doesn\'t return 404, (2) force a sync to rebuild cardinality, (3) replay the 4-hour delivery window to recover lost impressions. I\'ll generate the deployment artifact now.',
        query: `from wb_fixes import segment_remediation\n\nplan = segment_remediation.build(\n    segment_id='SEG-001',\n    target_graph='LiveRamp Identity Graph V2',\n    replay_hours=4,\n    affected_campaign_ids=[c['campaign_id'] for c in affected]\n)\nprint(plan.to_curl())`,
      },
    ],
    rootCause: 'LiveRamp segment SEG-001 failed identity graph sync 36 hours ago. LiveRamp Identity Graph returned 404_SEGMENT_NOT_FOUND, causing 0 eligible users across all campaigns using this segment. Auto-retry exhausted after 5 attempts with no recovery.',
    fixType: 'API Call',
    fixCode: `# Step 1: Re-map segment to v2 identity graph
curl -X POST https://dmp.warnermedia.internal/v2/segments/remap \\
  -H "Authorization: Bearer $DMP_TOKEN" \\
  -d '{
    "segment_id": "SEG-001",
    "target_graph": "LiveRamp Identity Graph V2",
    "force_sync": true,
    "notify_on_complete": "adops-oncall@warnermedia.com"
  }'

# Step 2: Replay queued impressions (4h window)
curl -X POST https://adserver.warnermedia.internal/v1/delivery/replay \\
  -H "Authorization: Bearer $ADSERVER_TOKEN" \\
  -d '{"filter":{"segment_id":"SEG-001"},"window_hours":4}'`,
    recoveryTime: '~25 min',
  },
  'LiveRamp Segment Sync Failure': {
    steps: [
      {
        tool: 'query_liveramp_connector', icon: 'terminal',
        desc: 'Fetching LiveRamp ingest batch logs', result: '502_UPSTREAM_TIMEOUT — taxonomy mismatch detected',
        thought: 'The alert indicates a DMP sync failure. I need to start at the ingest pipeline — the batch connector logs will tell me whether this is a timeout, schema error, or auth failure.',
        query: `from wb_data import liveramp_connector_logs\n\nlogs = liveramp_connector_logs.query(\n    connector_id='liveramp-ingest-batch',\n    since_hours=24,\n    level=['ERROR', 'WARN']\n)\nfor log in logs[-10:]:\n    print(f"[{log['ts']}] {log['status_code']}: {log['message']}")`,
      },
      {
        tool: 'query_segment_mapping', icon: 'database',
        desc: 'Checking external key taxonomy table', result: '"Action_Enthusiasts" → unmapped in taxonomy v3.1',
        thought: '502 timeout with "taxonomy mismatch" in the message. I need to inspect the taxonomy mapping table to find which external key is failing the lookup. A null mapping causes the upstream DMP to reject the entire batch.',
        query: `from wb_data import liveramp_taxonomy\n\nmapping = liveramp_taxonomy.lookup(\n    key='Action_Enthusiasts',\n    version='v3.1'\n)\nprint(f"Mapped value: {mapping}")\nprint(f"Current taxonomy version: {liveramp_taxonomy.current_version()}")`,
      },
      {
        tool: 'query_eligibility', icon: 'search',
        desc: 'Checking campaign targeting eligibility', result: '0 eligible impressions in queue',
        thought: 'The key is unmapped. This will cause zero eligible users — but I want to confirm by checking the campaign\'s actual eligibility queue to quantify the delivery impact.',
        query: `from wb_data import ad_eligibility\n\nqueue = ad_eligibility.check_campaign(\n    campaign_id=CAMPAIGN_ID,\n    check_segment=True\n)\nprint(f"Eligible impressions in queue: {queue['eligible_count']:,}")\nprint(f"Rejection reason: {queue['rejection_reason']}")`,
      },
      {
        tool: 'query_snapshot', icon: 'activity',
        desc: 'Finding last known-good sync snapshot', result: 'Snapshot 2024-07-14 08:00 UTC available',
        thought: 'Eligibility is zero — confirmed delivery impact. Before pushing a patch, I need to find a safe rollback snapshot so the retry job can seed from known-good data while the taxonomy fix propagates.',
        query: `from wb_data import liveramp_snapshots\n\nsnaps = liveramp_snapshots.list_available(\n    segment_id='SEG-001',\n    status='healthy'\n)\nlatest = snaps[0]\nprint(f"Latest good snapshot: {latest['snapshot_ts']}")\nprint(f"Cardinality: {latest['reach_estimate']:,}")`,
      },
      {
        tool: 'generate_resolution', icon: 'zap',
        desc: 'Generating config patch + re-ingest job', result: 'Taxonomy patch + connector retry with fallback',
        thought: 'I have everything I need: the bad key, the correct mapping in v3.2, and a valid fallback snapshot. I\'ll patch the configmap to add the missing key mapping and spawn a retry job seeded from the snapshot.',
        query: `from wb_fixes import liveramp_taxonomy_patch\n\npatch = liveramp_taxonomy_patch.build(\n    key='Action_Enthusiasts',\n    value='action_sports_v32',\n    target_version='3.2',\n    fallback_snapshot=latest['snapshot_ts'],\n    segment_id='SEG-001'\n)\nprint(patch.to_kubectl())`,
      },
    ],
    rootCause: 'LiveRamp connector taxonomy mapping is mismatched — v3.1 vs expected v3.2. External key "Action_Enthusiasts" resolves to null in the current mapping table, causing 502 upstream timeouts during ingest batch processing. 5 retries exhausted.',
    fixType: 'Config Patch',
    fixCode: `# Patch taxonomy mapping
kubectl patch configmap liveramp-taxonomy-mapping -n adops \\
  --patch '{"data":{"Action_Enthusiasts":"action_sports_v32","taxonomy_version":"3.2"}}'

# Retry ingest with fallback snapshot
kubectl create job --from=cronjob/liveramp-ingest-batch dmp-ingest-retry-$(date +%s) \\
  -n adops -- \\
  --segment=SEG-001 \\
  --use-fallback-snapshot=2024-07-14T08:00:00Z \\
  --force-remap=true`,
    recoveryTime: '~18 min',
  },
  'VAST Timeout': {
    steps: [
      {
        tool: 'query_fill_rate', icon: 'search',
        desc: 'Analyzing VAST request/response ratios', result: 'Fill rate: 94% → 61% over last 2h',
        thought: 'VAST timeout alert — I need to first quantify the fill rate degradation to establish the timeline. If fill rate dropped sharply, the issue is likely server-side rather than creative-side.',
        query: `from wb_data import vast_logs\nimport pandas as pd\n\ndf = vast_logs.load(hours=4)\nhourly = (\n    df.groupby('hour')\n    .agg(requests=('vast_req','sum'), responses=('vast_resp','sum'))\n    .assign(fill_rate=lambda x: x.responses / x.requests * 100)\n)\nprint(hourly)`,
      },
      {
        tool: 'query_latency', icon: 'activity',
        desc: 'Checking decision engine response times', result: 'p99 latency: 2840ms  (SLA: 1800ms)',
        thought: 'Fill rate dropped 33 points in 2 hours — that\'s abrupt. This points to latency, not bid pressure. I\'ll pull p99 latency from the decision engine to see if it\'s breaching the 1800ms SLA.',
        query: `from wb_data import freewheel_metrics\n\nlatency = freewheel_metrics.percentiles(\n    endpoint='vast_decision',\n    window_minutes=120,\n    percentiles=[50, 90, 95, 99]\n)\nprint(f"p50: {latency['p50']}ms")\nprint(f"p99: {latency['p99']}ms (SLA: 1800ms)")`,
      },
      {
        tool: 'query_cdn_health', icon: 'terminal',
        desc: 'Inspecting CDN node health telemetry', result: 'CDN node us-east-2b: 43% packet loss',
        thought: 'p99 at 2840ms — well above SLA. The decision engine itself is healthy on other endpoints so the bottleneck is upstream. I\'ll check CDN node health to see if packet loss on a specific node is causing connection drops.',
        query: `from wb_infra import cdn_health\n\nnodes = cdn_health.get_node_status(region='us-east')\nfor node in nodes:\n    print(\n        f"{node['node_id']}: "\n        f"packet_loss={node['packet_loss_pct']:.1f}% "\n        f"status={node['status']}"\n    )`,
      },
      {
        tool: 'identify_fallback', icon: 'database',
        desc: 'Locating backup ad decision endpoint', result: 'freewheel-2.warnermedia.internal — healthy',
        thought: 'us-east-2b at 43% packet loss explains everything — requests routed through that node are timing out. I need to verify adserver-2 is healthy and can absorb the traffic before I route failover.',
        query: `from wb_infra import freewheel_health\n\nfallback = freewheel_health.check(\n    endpoint='freewheel-2.warnermedia.internal',\n    probe_count=5\n)\nprint(f"Status: {fallback['status']}")\nprint(f"Avg latency: {fallback['avg_latency_ms']}ms")\nprint(f"Capacity headroom: {fallback['capacity_pct']}%")`,
      },
      {
        tool: 'generate_resolution', icon: 'zap',
        desc: 'Generating failover config patch', result: 'Timeout reduction + CDN failover routing',
        thought: 'Fallback is healthy with headroom. My fix is a two-part config patch: reduce the VAST timeout threshold to 1800ms to fail-fast instead of waiting 3s, and set adserver-2 as active failover endpoint with 20% packet-loss as the auto-trigger threshold.',
        query: `from wb_fixes import freewheel_routing\n\npatch = freewheel_routing.failover_patch(\n    primary='freewheel-1.warnermedia.internal',\n    fallback='freewheel-2.warnermedia.internal',\n    vast_timeout_ms=1800,\n    failover_threshold_pct=20,\n    cdn_exclude_nodes=['us-east-2b']\n)\nprint(patch.to_kubectl_configmap())`,
      },
    ],
    rootCause: 'CDN node us-east-2b is degraded (43% packet loss), pushing VAST response latency above the 1800ms SLA. Primary decision engine times out before returning valid ad pods, resulting in slate insertion across affected campaigns.',
    fixType: 'Config Update',
    fixCode: `cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: freewheel-routing-config
  namespace: adops
data:
  vast_timeout_ms: "1800"
  primary_endpoint: "freewheel-1.warnermedia.internal"
  failover_endpoint: "freewheel-2.warnermedia.internal"
  auto_failover_threshold_pct: "20"
  cdn_health_check_interval: "30s"
EOF`,
    recoveryTime: '~8 min',
  },
  'Roku Transcode Risk': {
    steps: [
      {
        tool: 'query_creative_specs', icon: 'search',
        desc: 'Fetching creative bitrate profiles', result: 'CMP creative: 18,500 kbps — Roku max: 15,000 kbps',
        thought: 'Roku transcode risk alert. I need to retrieve the creative\'s bitrate profile first — the alert may be preemptive and I want to confirm the actual bitrate before assuming a violation.',
        query: `from wb_data import creative_assets\n\ncreative = creative_assets.get(campaign_id=CAMPAIGN_ID)\nprint(f"Creative ID: {creative['asset_id']}")\nprint(f"Bitrate: {creative['bitrate_kbps']:,} kbps")\nprint(f"Codec: {creative['codec']}")\nprint(f"Resolution: {creative['resolution']}")`,
      },
      {
        tool: 'query_device_cohort', icon: 'database',
        desc: 'Checking Roku household cohort size', result: '~340K Roku devices targeted in this flight',
        thought: 'Confirmed: 18,500 kbps exceeds Roku\'s AVC Level 4.1 max of 15,000 kbps. I need to quantify exposure — how many Roku devices are in this flight\'s audience? This determines urgency.',
        query: `from wb_data import device_targeting\n\ncohort = device_targeting.estimate(\n    campaign_id=CAMPAIGN_ID,\n    device_type='roku'\n)\nprint(f"Estimated Roku households: {cohort['reach']:,}")\nprint(f"% of total flight audience: {cohort['share_pct']:.1f}%")`,
      },
      {
        tool: 'query_asset_store', icon: 'terminal',
        desc: 'Checking transcoder pipeline for renditions', result: 'No Roku-compatible rendition found in asset store',
        thought: '340K Roku devices — significant exposure. Before triggering a transcode job, I should check if a compliant rendition already exists or is in-flight in the asset store.',
        query: `from wb_data import asset_store\n\nrenditions = asset_store.list_renditions(\n    source_asset_id=creative['asset_id'],\n    device_profile='roku'\n)\nfor r in renditions:\n    print(f"{r['profile']}: {r['bitrate_kbps']} kbps — {r['status']}")`,
      },
      {
        tool: 'query_similar_assets', icon: 'activity',
        desc: 'Finding compliant creatives in same family', result: 'Creative 1 (VOD 12,147 kbps) — compatible',
        thought: 'No Roku rendition exists. While the transcode job runs (35 min), I should find if there\'s an existing compliant creative in the same campaign family we can use as an interim fallback.',
        query: `from wb_data import creative_assets\n\nfallbacks = creative_assets.find_compatible(\n    campaign_id=CAMPAIGN_ID,\n    device_profile='roku',\n    max_bitrate_kbps=15000\n)\nfor f in fallbacks:\n    print(f"{f['asset_id']}: {f['bitrate_kbps']} kbps — {f['label']}")`,
      },
      {
        tool: 'generate_resolution', icon: 'zap',
        desc: 'Generating transcode job + ingest rule', result: 'Transcode to ≤15 Mbps + preflight enforcement',
        thought: 'Fallback found for interim use. Full fix: (1) dispatch high-priority transcode to two Roku-safe profiles, (2) add a preflight bitrate enforcement rule to prevent this class of issue recurring on ingest.',
        query: `from wb_fixes import transcode_job\n\njob = transcode_job.create(\n    source_asset_id=creative['asset_id'],\n    profiles=['roku_avc_l41_14000kbps', 'roku_avc_l41_12000kbps'],\n    priority='high',\n    preflight_rule={\n        'enforce_on_ingest': True,\n        'max_bitrate_kbps': 15000,\n        'target_devices': ['roku']\n    }\n)\nprint(f"Job ID: {job['job_id']}")\nprint(f"ETA: {job['eta_minutes']} min")`,
      },
    ],
    rootCause: 'Campaign creative bitrate (18,500 kbps) exceeds Roku AVC Level 4.1 maximum (15,000 kbps). No fallback rendition exists in asset store. Serving this creative will cause startup failures across ~340K Roku households in this flight.',
    fixType: 'Transcode Job',
    fixCode: `curl -X POST https://transcoder.warnermedia.internal/v1/jobs \\
  -H "Authorization: Bearer $TRANSCODE_TOKEN" \\
  -d '{
    "source_asset_id": "CMP-001-CR-001",
    "output_profiles": [
      "roku_avc_level_4_1_14000kbps",
      "roku_avc_level_4_1_12000kbps"
    ],
    "priority": "high",
    "ingest_rule": {
      "enforce_bitrate_preflight": true,
      "max_bitrate_kbps": 15000,
      "target_devices": ["roku"]
    }
  }'`,
    recoveryTime: '~35 min (transcode queue)',
  },
  'Delivery Drop': {
    steps: [
      {
        tool: 'query_pacing_metrics', icon: 'search',
        desc: 'Computing delivery gap vs pacing target', result: 'Actual delivery 34% below target (last 6h)',
        thought: 'Delivery drop alert. I need to quantify the gap precisely — is this a gradual drift or an abrupt drop? The pacing curve shape will tell me whether to look at inventory or system failures.',
        query: `from wb_data import performance_log, campaigns\nimport pandas as pd\n\nc = campaigns.get(CAMPAIGN_ID)\nexp_per_hour = c['target_impressions'] / flight_hours(c)\n\ndf = performance_log.load(campaign_id=CAMPAIGN_ID, hours=6)\ndf['expected'] = exp_per_hour\ndf['gap_pct'] = (df['impressions_delivered'] - df['expected']) / df['expected'] * 100\nprint(df[['log_hour', 'impressions_delivered', 'expected', 'gap_pct']])`,
      },
      {
        tool: 'query_inventory_avails', icon: 'database',
        desc: 'Checking available inventory on platform', result: 'Prime-time sports break pods: 0 avails',
        thought: '34% below target and the drop is gradual over 6 hours — this is inventory starvation, not a system failure. I\'ll check available break pods on the primary platform to confirm there\'s simply nothing to bid on.',
        query: `from wb_data import inventory_avails\n\navails = inventory_avails.query(\n    platform_id=PLATFORM_ID,\n    break_type='prime_time_sports',\n    window_hours=12\n)\nprint(f"Available pods: {avails['pod_count']}")\nprint(f"Competing bids: {avails['competitor_density']:.1f}x")`,
      },
      {
        tool: 'query_cross_platform', icon: 'activity',
        desc: 'Checking MAX/TBS/CNN+ inventory', result: 'MAX premium: 28% available · TBS: 41% available',
        thought: 'Zero prime-time avails confirmed. The campaign needs inventory but the primary platform is exhausted. I\'ll check cross-platform availability to find where we can spillover budget to recover pacing.',
        query: `from wb_data import inventory_avails\n\nplatforms = ['MAX', 'TBS', 'CNN_PLUS']\nfor pid in platforms:\n    avails = inventory_avails.query(\n        platform_id=pid,\n        cpm_floor=CAMPAIGN_CPM * 0.9\n    )\n    print(f"{pid}: {avails['avail_pct']:.0f}% available")`,
      },
      {
        tool: 'compute_budget_shift', icon: 'terminal',
        desc: 'Calculating budget reallocation to recover', result: 'Shift 12% budget → MAX + loosen freq cap by 1',
        thought: 'MAX and TBS both have meaningful inventory. I\'ll model a budget reallocation that recovers the delivery gap without overspending — I also want to check if the frequency cap is artificially constraining reach.',
        query: `from wb_analytics import pacing_recovery\n\nplan = pacing_recovery.model(\n    campaign_id=CAMPAIGN_ID,\n    target_recovery_pct=95,\n    available_inventory={'MAX': 0.28, 'TBS': 0.41},\n    current_freq_cap=3\n)\nprint(f"Recommended MAX budget shift: {plan['max_shift_pct']:.0f}%")\nprint(f"Recommended freq cap: {plan['new_freq_cap']}")`,
      },
      {
        tool: 'generate_resolution', icon: 'zap',
        desc: 'Generating trafficking system update', result: 'Cross-platform reallocation + make-good schedule',
        thought: 'Model shows 12% budget shift to MAX plus loosening freq cap from 3 to 4 should recover to 95% delivery. I\'ll generate the trafficking update and a make-good schedule for the shortfall window.',
        query: `from wb_fixes import trafficking_update\n\nupdate = trafficking_update.build(\n    campaign_id=CAMPAIGN_ID,\n    budget_split={'primary': 0.88, 'MAX': 0.08, 'TBS': 0.04},\n    freq_cap={'impressions': 4, 'window': '24h'},\n    make_good={'enabled': True, 'recovery_target_pct': 95}\n)\nprint(update.to_api_payload())`,
      },
    ],
    rootCause: 'Prime-time sports programming consumed all available ad break pods on this platform. Forecasted avails for next 12h remain near zero for targeted break types. Campaign is 34% behind delivery goal with 18h remaining in flight.',
    fixType: 'Trafficking Update',
    fixCode: `curl -X PATCH https://trafficker.warnermedia.internal/v1/campaigns/$CAMPAIGN_ID \\
  -H "Authorization: Bearer $TRAFFIC_TOKEN" \\
  -d '{
    "budget_allocation": {
      "current_platform_pct": 88,
      "max_streaming_pct": 8,
      "tbs_linear_pct": 4
    },
    "frequency_cap": {"impressions": 4, "window": "24h"},
    "make_good": {
      "enabled": true,
      "target_recovery_pct": 95,
      "spillover_to_streaming": true
    }
  }'`,
    recoveryTime: '~45 min (propagation)',
  },
  'Prime-Time Underdelivery': {
    steps: [
      {
        tool: 'query_linear_schedule', icon: 'search',
        desc: 'Checking live event schedule for overruns', result: 'NBA playoff overran 38 min — 6 ad pods consumed',
        thought: 'Prime-time underdelivery during a live sports window. My first hypothesis is a live event overrun that consumed planned ad pods. I\'ll check the broadcast schedule for the affected window.',
        query: `from wb_data import broadcast_schedule\n\nschedule = broadcast_schedule.query(\n    platform_id=PLATFORM_ID,\n    date=ALERT_DATE,\n    window='prime_time'\n)\nfor event in schedule:\n    print(\n        f"{event['title']}: "\n        f"planned_end={event['planned_end']} "\n        f"actual_end={event['actual_end']} "\n        f"overrun_min={event['overrun_minutes']}"\n    )`,
      },
      {
        tool: 'query_make_good_queue', icon: 'database',
        desc: 'Checking make-good queue depth', result: '42 spots queued · late-prime windows available',
        thought: 'NBA ran 38 minutes over — that displaced 6 break pods. I need to check how many spots accumulated in the make-good queue and whether late-prime windows are available to clear them.',
        query: `from wb_data import make_good_queue\n\nqueue = make_good_queue.get(\n    campaign_id=CAMPAIGN_ID,\n    status='pending'\n)\nprint(f"Queued spots: {len(queue)}")\n\navail_windows = make_good_queue.available_windows(\n    after=ALERT_DATE, hours=6\n)\nprint(f"Available late-prime windows: {len(avail_windows)}")`,
      },
      {
        tool: 'query_streaming_avails', icon: 'activity',
        desc: 'Checking streaming inventory for spillover', result: 'MAX: 65K+ CPM-compatible avails in late-prime',
        thought: 'We have 42 spots queued and some late-prime windows, but linear alone may not be enough. I want to check MAX streaming inventory as a high-quality spillover option — CPM needs to match within tolerance.',
        query: `from wb_data import inventory_avails\n\nmax_avails = inventory_avails.query(\n    platform_id='MAX',\n    time_window='late_prime',\n    cpm_floor=CAMPAIGN_CPM * 0.85,\n    content_rating=['TV-G', 'TV-PG', 'TV-14']\n)\nprint(f"MAX late-prime avails: {max_avails['count']:,}")\nprint(f"CPM range: \${max_avails['cpm_min']:.2f}–\${max_avails['cpm_max']:.2f}")`,
      },
      {
        tool: 'compute_recovery', icon: 'terminal',
        desc: 'Generating make-good schedule', result: 'Distribute 42 spots: late-prime + MAX streaming',
        thought: 'MAX has plenty of CPM-compatible avails. I\'ll now model the optimal spot distribution: maximize linear late-prime first (same audience, same commitments), then overflow to MAX for the remainder.',
        query: `from wb_analytics import make_good_planner\n\nplan = make_good_planner.optimize(\n    queued_spots=42,\n    windows=avail_windows,\n    streaming_avails=max_avails,\n    priority='linear_first'\n)\nfor allocation in plan.allocations:\n    print(f"  {allocation['window']}: {allocation['spots']} spots on {allocation['platform']}")`,
      },
      {
        tool: 'generate_resolution', icon: 'zap',
        desc: 'Generating make-good + spillover config', result: 'Automated insertion + streaming overflow routing',
        thought: 'Plan optimized: 32 spots into late-prime linear windows, 10 spots to MAX streaming. I\'ll generate the make-good payload with auto-overflow routing in case the linear windows fill up.',
        query: `from wb_fixes import make_good_dispatch\n\nconfig = make_good_dispatch.build(\n    campaign_id=CAMPAIGN_ID,\n    schedule=plan.allocations,\n    auto_overflow={\n        'enabled': True,\n        'threshold_pct': 80,\n        'target': 'max_streaming'\n    }\n)\nprint(config.to_api_payload())`,
      },
    ],
    rootCause: 'Live NBA playoff ran 38 minutes over schedule, consuming 6 planned ad break pods and pushing 42 spots into the make-good queue. Underdelivery is concentrated in the 18:00–21:00 window. Campaign is 22% behind daily target.',
    fixType: 'Make-Good Schedule',
    fixCode: `curl -X POST https://trafficker.warnermedia.internal/v1/make-good \\
  -H "Authorization: Bearer $TRAFFIC_TOKEN" \\
  -d '{
    "campaign_id": "$CAMPAIGN_ID",
    "spots": 42,
    "schedule": [
      {"window":"21:30-22:00","platform":"linear","spots":18},
      {"window":"22:00-23:00","platform":"linear","spots":14},
      {"window":"21:00-23:00","platform":"max_streaming","spots":10}
    ],
    "auto_overflow": {
      "enabled": true, "threshold_pct": 80,
      "target": "max_streaming"
    }
  }'`,
    recoveryTime: '~20 min',
  },
}

// ─── ALERT HYPOTHESES (shown before agent deep dive) ────────────────────────

const ALERT_HYPOTHESES: Record<string, string[]> = {
  'Empty Audience Segment': [
    'LiveRamp segment failed to sync with the identity graph (last sync >36h ago)',
    'Audience cardinality near-zero due to over-restrictive targeting parameters',
    'External key mapping changed in the latest taxonomy update, breaking lookup',
  ],
  'LiveRamp Segment Sync Failure': [
    'Upstream LiveRamp connector returning 5xx errors during batch ingest',
    'Taxonomy version mismatch between connector config and segment store (v3.1 vs v3.2)',
    'API rate limiting on DMP causing silent batch ingest failures',
  ],
  'VAST Timeout': [
    'CDN node degradation pushing p99 latency above 1800ms SLA threshold',
    'Decision engine overloaded — request fanout exceeding concurrency limits',
    'Network congestion on primary ad server route causing connection timeouts',
  ],
  'Roku Transcode Risk': [
    'Creative bitrate exceeds Roku AVC Level 4.1 maximum (15,000 kbps)',
    'No fallback rendition available in asset store for Roku device profile',
    'Transcode pipeline not enforcing preflight bitrate checks on ingest',
  ],
  'Delivery Drop': [
    'Prime-time sports programming consumed all available ad break pods',
    'Frequency cap too restrictive — limiting reach to a small audience pool',
    'Budget allocation heavily skewed to a platform with low current avails',
  ],
  'Prime-Time Underdelivery': [
    'Live event overrun consumed planned ad pods (make-good queue building)',
    'Underdelivery concentrated in 18:00–21:00 window; late-prime windows available',
    'Streaming spillover routing not configured for this campaign flight',
  ],
}

// ─── TABLE CATALOG ──────────────────────────────────────────────────────────

const TABLE_CATALOG = [
  { name: 'advertisers', file: 'advertisers.csv', color: '#FF5800',
    description: 'Brand accounts purchasing ad inventory across Warner networks.',
    columns: [
      { name: 'advertiser_id', type: 'STRING', pk: true,  fk: false },
      { name: 'name',          type: 'STRING', pk: false, fk: false },
      { name: 'industry',      type: 'STRING', pk: false, fk: false },
      { name: 'tier',          type: 'STRING', pk: false, fk: false },
    ]},
  { name: 'agencies', file: 'agencies.csv', color: '#FF5800',
    description: 'Media buying agencies placing orders on behalf of advertisers.',
    columns: [
      { name: 'agency_id',     type: 'STRING', pk: true,  fk: false },
      { name: 'name',          type: 'STRING', pk: false, fk: false },
      { name: 'contact_email', type: 'STRING', pk: false, fk: false },
    ]},
  { name: 'platforms', file: 'platforms.csv', color: '#FF5800',
    description: 'Ad delivery platforms — Streaming (HBO Max, CNN+) and Linear (TNT, TBS, truTV).',
    columns: [
      { name: 'platform_id',   type: 'STRING', pk: true,  fk: false },
      { name: 'platform_name', type: 'STRING', pk: false, fk: false },
      { name: 'platform_type', type: 'STRING', pk: false, fk: false },
    ]},
  { name: 'first_party_segments', file: 'audience_segments.csv', color: '#FF5800',
    description: 'First-party audience targeting segments used in campaign targeting.',
    columns: [
      { name: 'segment_id',   type: 'STRING', pk: true,  fk: false },
      { name: 'segment_name', type: 'STRING', pk: false, fk: false },
      { name: 'provider',     type: 'STRING', pk: false, fk: false },
      { name: 'sync_status',  type: 'STRING', pk: false, fk: false },
    ]},
  { name: 'campaigns', file: 'campaigns.csv', color: '#FF8C42',
    description: 'Central fact table linking advertisers, agencies, platforms, and segments.',
    columns: [
      { name: 'campaign_id',        type: 'STRING', pk: true,  fk: false },
      { name: 'campaign_name',      type: 'STRING', pk: false, fk: false },
      { name: 'advertiser_id',      type: 'STRING', pk: false, fk: true  },
      { name: 'agency_id',          type: 'STRING', pk: false, fk: true  },
      { name: 'platform_id',        type: 'STRING', pk: false, fk: true  },
      { name: 'segment_id',         type: 'STRING', pk: false, fk: true  },
      { name: 'status',             type: 'ENUM',   pk: false, fk: false },
      { name: 'target_impressions', type: 'INT',    pk: false, fk: false },
      { name: 'cpm_usd',            type: 'FLOAT',  pk: false, fk: false },
    ]},
  { name: 'creatives', file: 'creatives.csv', color: '#f59e0b',
    description: 'Ad creative assets (video/display) associated with campaign flights.',
    columns: [
      { name: 'creative_id',   type: 'STRING', pk: true,  fk: false },
      { name: 'campaign_id',   type: 'STRING', pk: false, fk: true  },
      { name: 'creative_name', type: 'STRING', pk: false, fk: false },
      { name: 'format',        type: 'STRING', pk: false, fk: false },
      { name: 'bitrate_kbps',  type: 'INT',    pk: false, fk: false },
      { name: 'duration_sec',  type: 'INT',    pk: false, fk: false },
    ]},
  { name: 'performance_log', file: 'performance_log.csv', color: '#22c55e',
    description: 'Hourly delivery metrics per campaign. Primary time-series fact table (~155K rows).',
    columns: [
      { name: 'log_hour',               type: 'DATETIME', pk: false, fk: false },
      { name: 'campaign_id',            type: 'STRING',   pk: false, fk: true  },
      { name: 'impressions_delivered',  type: 'INT',      pk: false, fk: false },
      { name: 'vast_requests',          type: 'INT',      pk: false, fk: false },
      { name: 'vast_responses',         type: 'INT',      pk: false, fk: false },
      { name: 'avg_latency_ms',         type: 'INT',      pk: false, fk: false },
      { name: 'error_count',            type: 'INT',      pk: false, fk: false },
      { name: 'video_completes',        type: 'INT',      pk: false, fk: false },
    ]},
  { name: 'alerts', file: 'alerts.csv', color: '#ef4444',
    description: 'System-generated delivery and performance alerts with revenue impact estimates.',
    columns: [
      { name: 'alert_id',              type: 'STRING',   pk: true,  fk: false },
      { name: 'alert_timestamp',       type: 'DATETIME', pk: false, fk: false },
      { name: 'severity',              type: 'ENUM',     pk: false, fk: false },
      { name: 'alert_type',            type: 'STRING',   pk: false, fk: false },
      { name: 'campaign_id',           type: 'STRING',   pk: false, fk: true  },
      { name: 'revenue_impact_usd',    type: 'FLOAT',    pk: false, fk: false },
      { name: 'status',                type: 'STRING',   pk: false, fk: false },
    ]},
  { name: 'kpi_summary', file: 'kpi_summary.csv', color: '#9EA3B0',
    description: 'Pre-aggregated KPI snapshot. Materialized view of key system metrics.',
    columns: [
      { name: 'kpi_name', type: 'STRING',   pk: false, fk: false },
      { name: 'value',    type: 'STRING',   pk: false, fk: false },
      { name: 'unit',     type: 'STRING',   pk: false, fk: false },
      { name: 'as_of',    type: 'DATETIME', pk: false, fk: false },
    ]},
] as const

const KPI_FORMULAS = [
  { name: 'Delivery Rate',              formula: 'impressions_delivered ÷ (target_impressions ÷ flight_hours) × 100', unit: '%',      source: 'performance_log + campaigns', description: "Hourly pacing vs target rate. Below 90% triggers a pacing alert.",                good: '≥ 90%', bad: '< 85%' },
  { name: 'Fill Rate',                  formula: 'vast_responses ÷ vast_requests × 100',                              unit: '%',      source: 'performance_log',             description: 'Percentage of ad requests returning a valid VAST response.',                      good: '≥ 95%', bad: '< 80%' },
  { name: 'Video Completion Rate (VCR)',formula: 'video_completes ÷ impressions_delivered × 100',                     unit: '%',      source: 'performance_log',             description: 'Viewers who watched the full ad. Key quality signal for premium video.',        good: '≥ 70%', bad: '< 50%' },
  { name: 'VAST Error Rate',            formula: 'error_count ÷ vast_requests × 100',                                 unit: '%',      source: 'performance_log',             description: 'Proportion of ad requests resulting in VAST errors (timeouts, empty pods).',   good: '< 3%',  bad: '> 8%'  },
  { name: 'Revenue at Risk',            formula: "Σ revenue_impact_usd  WHERE  status = 'Open'",                      unit: 'USD',    source: 'alerts',                      description: 'Total estimated revenue exposure from all unresolved alerts.',                  good: '< $10K',bad: '> $50K'},
  { name: 'Effective CPM',              formula: '(campaign_budget ÷ impressions_delivered) × 1,000',                 unit: 'USD/M',  source: 'campaigns + performance_log', description: 'Actual cost per 1K delivered impressions vs contracted cpm_usd.',             good: '±10% of cpm_usd', bad: '> 15% above' },
  { name: 'Impression Shortfall',       formula: 'expected_impressions − actual_impressions',                         unit: 'imps',   source: 'alerts',                      description: 'Absolute delivery gap at alert time. Used for make-good calculations.',        good: '< 5% of target', bad: '> 15%' },
  { name: 'Flight Hours',               formula: '(end_date − start_date + 1 day) × 24',                              unit: 'hours',  source: 'campaigns',                   description: 'Total flight duration in hours. Denominator for per-hour pacing targets.',    good: '—', bad: '—' },
] as const

type ErNode = { name: string; cx: number; cy: number; color: string; standalone?: boolean; cols: [string, string][] }
type ErLine = { d: string; label: string; lx: number; ly: number }

const ER_NODES: ErNode[] = [
  { name: 'advertisers',       cx: 130, cy: 70,  color: '#FF5800', cols: [['PK','advertiser_id'],['','name'],['','industry'],['','tier']] },
  { name: 'agencies',          cx: 130, cy: 250, color: '#FF5800', cols: [['PK','agency_id'],['','name'],['','contact_email']] },
  { name: 'first_party_segments', cx: 130, cy: 430, color: '#FF5800', cols: [['PK','segment_id'],['','segment_name'],['','provider'],['','sync_status']] },
  { name: 'kpi_summary',       cx: 460, cy: 70,  color: '#9EA3B0', standalone: true, cols: [['','kpi_name'],['','value'],['','unit'],['','as_of']] },
  { name: 'campaigns',         cx: 460, cy: 255, color: '#FF8C42', cols: [['PK','campaign_id'],['FK','advertiser_id'],['FK','agency_id'],['FK','platform_id'],['FK','segment_id'],['','status'],['','cpm_usd']] },
  { name: 'alerts',            cx: 460, cy: 440, color: '#ef4444', cols: [['PK','alert_id'],['FK','campaign_id'],['','severity'],['','alert_type'],['','revenue_impact_usd']] },
  { name: 'platforms',         cx: 790, cy: 70,  color: '#FF5800', cols: [['PK','platform_id'],['','platform_name'],['','platform_type']] },
  { name: 'creatives',         cx: 790, cy: 250, color: '#f59e0b', cols: [['PK','creative_id'],['FK','campaign_id'],['','creative_name'],['','format'],['','bitrate_kbps']] },
  { name: 'performance_log',   cx: 790, cy: 430, color: '#22c55e', cols: [['FK','campaign_id'],['','log_hour'],['','impressions_delivered'],['','vast_requests'],['','video_completes']] },
]
const ER_LINES: ErLine[] = [
  { d: 'M 225,70 C 295,70 295,232 365,232',   label: 'advertiser_id', lx: 276, ly: 147 },
  { d: 'M 225,250 L 365,245',                  label: 'agency_id',     lx: 269, ly: 241 },
  { d: 'M 225,430 C 295,430 295,262 365,262',  label: 'segment_id',    lx: 276, ly: 350 },
  { d: 'M 555,232 C 625,232 625,70 695,70',    label: 'platform_id',   lx: 627, ly: 147 },
  { d: 'M 695,250 L 555,245',                  label: 'campaign_id',   lx: 612, ly: 241 },
  { d: 'M 695,430 C 625,430 625,262 555,262',  label: 'campaign_id',   lx: 627, ly: 350 },
  { d: 'M 460,336 L 460,368',                  label: 'campaign_id',   lx: 466, ly: 355 },
]

// ─── HELPERS ────────────────────────────────────────────────────────────────

function useCountUp(target: number, duration = 1400) {
  const [val, setVal] = useState(0)
  const raf = useRef<number>(0)
  useEffect(() => {
    const start = performance.now()
    const tick = (now: number) => {
      const p = Math.min((now - start) / duration, 1)
      const eased = 1 - Math.pow(1 - p, 3)
      setVal(Math.round(eased * target))
      if (p < 1) raf.current = requestAnimationFrame(tick)
    }
    raf.current = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(raf.current)
  }, [target, duration])
  return val
}

function parseDate(s: string) { return new Date(s.replace(' ', 'T')) }

function flightHours(c: Campaign) {
  const days = Math.max(1, Math.round((new Date(`${c.end_date}T00:00:00`).getTime() - new Date(`${c.start_date}T00:00:00`).getTime()) / 86400000) + 1)
  return days * 24
}

function getNetwork(name: string): 'Streaming' | 'Linear' {
  return ['TNT', 'TBS', 'truTV'].includes(name) ? 'Linear' : 'Streaming'
}

function getSeverityWeight(s: string) {
  return s === 'Critical' ? 0 : s === 'High' ? 1 : s === 'Warning' ? 2 : 3
}

function healthColor(rate: number) {
  if (rate >= 90) return '#22c55e'
  if (rate >= 75) return '#f59e0b'
  return '#ef4444'
}

function buildRca(a: EnrichedAlert) {
  const confidenceMap: Record<string, number> = {
    'Empty Audience Segment': 96, 'LiveRamp Segment Sync Failure': 93,
    'Delivery Drop': 88, 'VAST Timeout': 91, 'Roku Transcode Risk': 90, 'Prime-Time Underdelivery': 84,
  }
  const traceMap: Record<string, string[]> = {
    'Empty Audience Segment': [
      '[SEGMENT-GRAPH] INFO  Segment lookup: SEG-001',
      '[SEGMENT-GRAPH] ERROR 404_SEGMENT_NOT_FOUND in LiveRamp Identity Graph',
      '[ELIGIBILITY]   WARN  0 eligible users for campaign targeting',
      '[DELIVERY]      CRIT  campaign halted — empty audience cardinality',
    ],
    'LiveRamp Segment Sync Failure': [
      '[LIVERAMP-CONNECTOR] INFO  ingest batch started for SEG-001',
      '[LIVERAMP-CONNECTOR] ERROR 502_UPSTREAM_TIMEOUT resolving taxonomy IDs',
      '[MAPPING]       ERROR 404_SEGMENT_NOT_FOUND: key Action_Enthusiasts',
      '[RECOVERY]      WARN  auto-retry exhausted after 5 attempts',
    ],
    'VAST Timeout': [
      '[AD-SERVER]     INFO  request fanout to decision engine',
      '[VAST]          ERROR 504_TIMEOUT from creative decision endpoint',
      '[VAST]          ERROR no fallback ad pod available',
      '[PLAYER]        WARN  empty payload — defaulting to slate',
    ],
    'Roku Transcode Risk': [
      '[TRANSCODE]     INFO  profile match: Roku_AVC_Level_4_1',
      '[TRANSCODE]     ERROR bitrate 18500kbps exceeds threshold 15000kbps',
      '[PACKAGER]      WARN  fallback rendition missing for 6s GOP',
      '[DELIVERY]      WARN  startup failure risk for Roku cohort',
    ],
    'Delivery Drop': [
      '[PACING]        INFO  delivery drift crossed 25% threshold',
      '[FORECAST]      WARN  underdelivery concentrated in prime-time',
      '[INVENTORY]     WARN  constrained avails on premium sports pods',
      '[ROUTER]        INFO  realloc recommendation generated',
    ],
    'Prime-Time Underdelivery': [
      '[LINEAR-SCHED]  WARN  live overrun consumed planned ad pods',
      '[TRAFFICKER]    WARN  make-good queue depth: 42 spots',
      '[PACING]        WARN  campaign behind 22% in 18:00-21:00 window',
      '[OPTIMIZER]     INFO  recommending spillover to streaming',
    ],
  }
  const fixMap: Record<string, string> = {
    'Empty Audience Segment': 'Re-map SEG-001 to LiveRamp Identity Graph V2, force sync, and replay 4h impression queue.',
    'LiveRamp Segment Sync Failure': 'Patch taxonomy mapping to v3.2 and re-run LiveRamp connector with prior-day fallback snapshot.',
    'VAST Timeout': 'Reduce timeout to 1800ms, failover to backup ad decision endpoint on us-east-2b degradation.',
    'Roku Transcode Risk': 'Submit transcode job for Roku-compatible renditions (≤15 Mbps) and enforce bitrate preflight on ingest.',
    'Delivery Drop': 'Shift 12% budget to MAX/TBS inventory, loosen frequency cap by 1, enable streaming make-good.',
    'Prime-Time Underdelivery': 'Insert 42 make-good spots in late-prime windows and route overflow to MAX streaming.',
  }
  return {
    confidence: confidenceMap[a.alert_type] ?? 82,
    trace: traceMap[a.alert_type] ?? ['[SYSTEM] No detailed trace available.'],
    recommendation: fixMap[a.alert_type] ?? 'Escalate to AdOps engineering for manual investigation.',
  }
}

function loadCsv<T>(url: string): Promise<T[]> {
  return new Promise((resolve, reject) => {
    Papa.parse<T>(url, {
      download: true, header: true, dynamicTyping: true, skipEmptyLines: true,
      complete: (r) => resolve(r.data),
      error: (err) => reject(err),
    })
  })
}

function loadPerformanceSample(url: string, every: number, ids: Set<string>): Promise<PerformanceRow[]> {
  return new Promise((resolve, reject) => {
    const out: PerformanceRow[] = []
    let i = 0
    Papa.parse<PerformanceRow>(url, {
      download: true, header: true, dynamicTyping: true, skipEmptyLines: true,
      step: (r) => {
        const row = r.data
        if (i % every === 0 || ids.has(String(row.campaign_id))) out.push(row)
        i++
      },
      complete: () => resolve(out),
      error: (err) => reject(err),
    })
  })
}

// ─── CAMPAIGN HEALTH ANALYSIS ────────────────────────────────────────────────

const CAMPAIGN_HEALTH_STEPS = [
  { tool: 'query_performance_log',   icon: 'search',   desc: 'Fetching hourly delivery metrics for campaign',   result: 'Ingested impressions, fill rate, completion data' },
  { tool: 'query_inventory_avails',  icon: 'database', desc: 'Checking platform inventory & pod availability',  result: 'Scanned available pods and targeting constraints' },
  { tool: 'query_creative_pipeline', icon: 'terminal', desc: 'Auditing creative specs & transcode status',      result: 'Checked bitrate, format, device compatibility' },
  { tool: 'correlate_alert_signals', icon: 'activity', desc: 'Cross-referencing with active alert history',     result: 'Matched delivery gap to known alert patterns' },
  { tool: 'generate_recommendation', icon: 'zap',      desc: 'Generating prioritized resolution plan',          result: 'Actionable remediation steps ready' },
] as const

function getCampaignRca(h: {
  deliveryRate: number; fillRate: number; vcr: number; errorRate: number
  campaign: { campaign_name: string; cpm_usd: number; target_impressions: number }
  advertiser_name: string; platform_name: string; alertCount: number
}) {
  const issues: string[] = []
  const fixes: string[] = []

  if (h.deliveryRate < 90) {
    if (h.deliveryRate < 10) {
      issues.push(`Delivery Rate is critically low at ${h.deliveryRate.toFixed(1)}% — near-zero impressions being served`)
      fixes.push('Check LiveRamp segment sync status; if targeting a specific audience, the segment may have failed to load. Expand targeting or remove constraints temporarily to restore delivery.')
    } else {
      issues.push(`Delivery Rate is ${h.deliveryRate.toFixed(1)}%, below the 90% pacing target`)
      fixes.push('Consider loosening frequency caps by 1 and expanding platform inventory allocation. A cross-platform budget shift to streaming may recover up to 15% of the delivery gap.')
    }
  }
  if (h.fillRate < 80) {
    issues.push(`Fill Rate is ${h.fillRate.toFixed(1)}% (threshold: 80%) — VAST requests are not returning valid ad pods`)
    fixes.push('Inspect VAST timeout settings and CDN node health. Failover to a secondary ad decision endpoint if primary is degraded (p99 latency > 1800ms).')
  }
  if (h.vcr < 50) {
    issues.push(`Video Completion Rate is ${h.vcr.toFixed(1)}%, below the 50% quality threshold`)
    fixes.push('Low VCR often indicates creative asset issues (high bitrate, wrong device profile) or targeting mismatch. Validate that transcoded renditions meet device-specific requirements.')
  }
  if (h.errorRate > 8) {
    issues.push(`VAST Error Rate is ${h.errorRate.toFixed(2)}%, exceeding the 8% critical threshold`)
    fixes.push('High error rate points to infrastructure issues. Audit VAST response errors in the delivery pipeline and check for upstream 5xx errors in CDN logs.')
  }

  if (issues.length === 0) {
    return {
      rootCause: 'No critical performance issues detected. All KPIs are within normal operating bounds.',
      recommendation: 'Campaign is healthy. Continue monitoring delivery pacing and alert for any trend deterioration.',
    }
  }

  return {
    rootCause: issues.join(' · '),
    recommendation: fixes.join(' '),
  }
}

function downloadCsv(filename: string) {
  const url = `${import.meta.env.BASE_URL}data/${filename}`
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
}

// ─── APP ────────────────────────────────────────────────────────────────────

function App() {
  const [advertisers, setAdvertisers] = useState<Advertiser[]>([])
  const [platforms,   setPlatforms]   = useState<Platform[]>([])
  const [campaigns,   setCampaigns]   = useState<Campaign[]>([])
  const [performance, setPerformance] = useState<PerformanceRow[]>([])
  const [alerts,      setAlerts]      = useState<AlertRow[]>([])
  const [kpis,        setKpis]        = useState<KpiRow[]>([])
  const [loading,     setLoading]     = useState(true)
  const [isLightweightMode, setIsLightweightMode] = useState(true)

  // Navigation
  const [activeTab, setActiveTab]         = useState<ActiveTab>('overview')
  // Overview drill-down modal
  const [drilldown, setDrilldown]         = useState<DrilldownKey>(null)
  // Campaign health side panel
  const [selectedCampaignId, setSelectedCampaignId] = useState<string | null>(null)
  // Notifications
  const [expandedAlert, setExpandedAlert] = useState<string | null>(null)
  const [notifFilter,   setNotifFilter]   = useState<string>('All')
  const [reviewDecisions, setReviewDecisions] = useState<Record<string, ReviewDecision>>({})
  const [rejectReason, setRejectReason]   = useState<Record<string, string>>({})
  // Old RCA panel (from overview alerts click)
  const [selectedAlert, setSelectedAlert] = useState<EnrichedAlert | null>(null)

  // Campaign health table sort
  type HealthSortCol = 'campaign' | 'advertiser' | 'platform' | 'delivery' | 'fill' | 'vcr' | 'error' | 'alerts'
  const [healthSortCol, setHealthSortCol] = useState<HealthSortCol>('alerts')
  const [healthSortDir, setHealthSortDir] = useState<'asc' | 'desc'>('asc')

  const toggleHealthSort = (col: HealthSortCol) => {
    if (col === healthSortCol) {
      setHealthSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setHealthSortCol(col)
      setHealthSortDir('asc')
    }
  }

  const severityWeight = (h: typeof campaignHealth[0]): number => {
    if (h.alertCount > 0) {
      const s = (h.topSeverity ?? '').toLowerCase()
      if (s === 'critical') return 0
      if (s === 'high') return 1
      if (s === 'medium') return 2
      return 3
    }
    if (h.deliveryRate < 10) return 0.5
    if (h.deliveryRate < 85) return 1.5
    return 4
  }

  // Data explorer preview
  const [previewTable, setPreviewTable] = useState<string | null>(null)
  // Intelligence board platform tab
  const [intelPlatform, setIntelPlatform] = useState<string>('All')
  // Last refreshed counter
  const [secondsSinceRefresh, setSecondsSinceRefresh] = useState(14)
  useEffect(() => {
    const id = setInterval(() => setSecondsSinceRefresh((s) => s + 1), 1000)
    return () => clearInterval(id)
  }, [])

  // Chat widget
  const [chatOpen, setChatOpen] = useState(false)
  const [chatInput, setChatInput] = useState('')
  const [chatMessages, setChatMessages] = useState<{ role: 'user' | 'ai'; text: string }[]>([
    { role: 'ai', text: 'Hi! Ask me anything about campaign health, delivery pacing, alerts, or platform performance.' }
  ])
  // Notifications agent state per alert
  const [agentState, setAgentState] = useState<Record<string, 'idle' | 'running' | 'complete'>>({})
  const [agentStep, setAgentStep] = useState<Record<string, number>>({})
  // Campaign health agent state
  const [campaignAgentState, setCampaignAgentState] = useState<Record<string, 'idle' | 'running' | 'complete'>>({})
  const [campaignAgentStep, setCampaignAgentStep] = useState<Record<string, number>>({})

  useEffect(() => {
    const load = async () => {
      setLoading(true)
      const [a, p, c, al, k] = await Promise.all([
        loadCsv<Advertiser>(`${import.meta.env.BASE_URL}data/advertisers.csv`),
        loadCsv<Platform>(`${import.meta.env.BASE_URL}data/platforms.csv`),
        loadCsv<Campaign>(`${import.meta.env.BASE_URL}data/campaigns.csv`),
        loadCsv<AlertRow>(`${import.meta.env.BASE_URL}data/alerts.csv`),
        loadCsv<KpiRow>(`${import.meta.env.BASE_URL}data/kpi_summary.csv`),
      ])
      const alertIds = new Set(al.map((r) => r.campaign_id))
      const perf = isLightweightMode
        ? await loadPerformanceSample(`${import.meta.env.BASE_URL}data/performance_log.csv`, 8, alertIds)
        : await loadCsv<PerformanceRow>(`${import.meta.env.BASE_URL}data/performance_log.csv`)
      setAdvertisers(a); setPlatforms(p); setCampaigns(c)
      setPerformance(perf); setAlerts(al); setKpis(k)
      setLoading(false)
    }
    load().catch((err) => { console.error('Failed to load data', err); setLoading(false) })
  }, [isLightweightMode])

  // ── Lookups
  const advertiserMap = useMemo(() => Object.fromEntries(advertisers.map((a) => [a.advertiser_id, a])), [advertisers])
  const platformMap   = useMemo(() => Object.fromEntries(platforms.map((p) => [p.platform_id, p])),   [platforms])
  const campaignMap   = useMemo(() => Object.fromEntries(campaigns.map((c) => [c.campaign_id, c])),   [campaigns])

  // ── Time window
  const maxPerfTime = useMemo(() => {
    if (!performance.length) return null
    return performance.reduce((l, r) => { const t = parseDate(r.log_hour); return !l || t > l ? t : l }, null as Date | null)
  }, [performance])

  const last24Rows = useMemo(() => {
    if (!maxPerfTime) return []
    const from = new Date(maxPerfTime.getTime() - 23 * 3600 * 1000)
    return performance.filter((r) => parseDate(r.log_hour) >= from)
  }, [maxPerfTime, performance])

  // ── Top metrics
  const topMetrics = useMemo(() => {
    const activeCampaigns = campaigns.filter((c) => c.status === 'Active').length
    let expected = 0, actual = 0
    for (const row of last24Rows) {
      const c = campaignMap[row.campaign_id]; if (!c) continue
      expected += c.target_impressions / flightHours(c)
      actual   += row.impressions_delivered
    }
    const avgDeliveryRate = expected > 0 ? (actual / expected) * 100 : 0
    const activeAlerts  = alerts.filter((a) => a.status.toLowerCase() === 'open').length
    const revenueAtRisk = Number(kpis.find((k) => k.kpi_name === 'Revenue at Risk')?.value) ||
      alerts.reduce((s, a) => s + Number(a.revenue_impact_usd || 0), 0)
    return { activeCampaigns, avgDeliveryRate, activeAlerts, revenueAtRisk }
  }, [campaigns, last24Rows, campaignMap, alerts, kpis])

  // ── Pacing chart
  const pacingSeries = useMemo(() => {
    if (!last24Rows.length) return []
    const byHour = new Map<string, { expected: number; actual: number }>()
    for (const row of last24Rows) {
      const c = campaignMap[row.campaign_id]; if (!c) continue
      const exp = c.target_impressions / flightHours(c)
      const slot = row.log_hour.slice(11, 16)
      const prev = byHour.get(slot) ?? { expected: 0, actual: 0 }
      prev.expected += exp; prev.actual += row.impressions_delivered
      byHour.set(slot, prev)
    }
    const hours = [...byHour.keys()].sort()
    let cumExp = 0, cumAct = 0
    return hours.map((hour) => {
      const r = byHour.get(hour)!
      cumExp += r.expected; cumAct += r.actual
      return { hour, target: Math.round(cumExp), actual: Math.round(cumAct) }
    })
  }, [last24Rows, campaignMap])

  // ── Fallout donut — all open alerts regardless of timestamp
  const falloutSeries = useMemo(() => {
    if (!alerts.length) return []
    const m = new Map<string, number>()
    alerts.filter((a) => a.status.toLowerCase() === 'open')
      .forEach((a) => m.set(a.alert_type, (m.get(a.alert_type) ?? 0) + 1))
    return [...m.entries()].map(([name, value]) => ({ name, value }))
  }, [alerts])

  // ── Enriched alerts
  const enrichedAlerts = useMemo<EnrichedAlert[]>(() => {
    return alerts
      .filter((a) => a.status.toLowerCase() === 'open')
      .map((a) => {
        const c  = campaignMap[a.campaign_id]
        const adv = c ? advertiserMap[c.advertiser_id] : undefined
        const plt = c ? platformMap[c.platform_id]    : undefined
        const pName = plt?.platform_name ?? 'Unknown'
        return { ...a, campaign_name: c?.campaign_name ?? a.campaign_id,
          advertiser_name: adv?.name ?? 'Unknown', platform_name: pName, network: getNetwork(pName) }
      })
      .sort((a, b) => {
        const s = getSeverityWeight(a.severity) - getSeverityWeight(b.severity)
        return s !== 0 ? s : parseDate(b.alert_timestamp).getTime() - parseDate(a.alert_timestamp).getTime()
      })
  }, [alerts, campaignMap, advertiserMap, platformMap])

  // ── Campaign health table
  const campaignHealth = useMemo(() => {
    const perfByCampaign = new Map<string, PerformanceRow[]>()
    for (const row of performance) {
      const arr = perfByCampaign.get(row.campaign_id) ?? []
      arr.push(row); perfByCampaign.set(row.campaign_id, arr)
    }
    const alertsByCampaign = new Map<string, AlertRow[]>()
    for (const a of enrichedAlerts) {
      const arr = alertsByCampaign.get(a.campaign_id) ?? []
      arr.push(a); alertsByCampaign.set(a.campaign_id, arr)
    }
    return campaigns
      .filter((c) => c.status === 'Active')
      .map((c) => {
        const rows  = perfByCampaign.get(c.campaign_id) ?? []
        const cam   = campaignMap[c.campaign_id]
        const expHr = cam ? cam.target_impressions / flightHours(cam) : 0
        const totalDel  = rows.reduce((s, r) => s + r.impressions_delivered, 0)
        const totalExp  = rows.length * expHr
        const totalReq  = rows.reduce((s, r) => s + r.vast_requests, 0)
        const totalResp = rows.reduce((s, r) => s + r.vast_responses, 0)
        const totalComp = rows.reduce((s, r) => s + r.video_completes, 0)
        const totalErr  = rows.reduce((s, r) => s + r.error_count, 0)
        const deliveryRate = totalExp > 0 ? (totalDel / totalExp) * 100 : 0
        const fillRate     = totalReq > 0 ? (totalResp / totalReq) * 100 : 0
        const vcr          = totalDel > 0 ? (totalComp / totalDel) * 100 : 0
        const errorRate    = totalReq > 0 ? (totalErr  / totalReq) * 100 : 0
        const camAlerts    = alertsByCampaign.get(c.campaign_id) ?? []
        const topSev       = camAlerts.sort((a, b) => getSeverityWeight(a.severity) - getSeverityWeight(b.severity))[0]?.severity ?? null
        const adv = advertiserMap[c.advertiser_id]
        const plt = platformMap[c.platform_id]
        return { campaign: c, advertiser_name: adv?.name ?? '—', platform_name: plt?.platform_name ?? '—',
          deliveryRate, fillRate, vcr, errorRate, alertCount: camAlerts.length, topSeverity: topSev,
          recentRows: rows.slice(-24) }
      })
      .sort((a, b) => a.deliveryRate - b.deliveryRate)
  }, [campaigns, performance, enrichedAlerts, campaignMap, advertiserMap, platformMap])




  // ── Intelligence Board: fill rate + VCR by platform
  const platformPerformance = useMemo(() => {
    const byPlatform = new Map<string, { name: string; req: number; resp: number; compl: number; del: number }>()
    for (const row of performance) {
      const c   = campaignMap[row.campaign_id]; if (!c) continue
      const plt = platformMap[c.platform_id];   if (!plt) continue
      const key = plt.platform_name
      const prev = byPlatform.get(key) ?? { name: key, req: 0, resp: 0, compl: 0, del: 0 }
      prev.req   += row.vast_requests
      prev.resp  += row.vast_responses
      prev.compl += row.video_completes
      prev.del   += row.impressions_delivered
      byPlatform.set(key, prev)
    }
    return [...byPlatform.values()]
      .map((p) => ({
        name:     p.name,
        fillRate: p.req > 0 ? Math.round((p.resp  / p.req) * 10) / 10 : 0,
        vcr:      p.del > 0 ? Math.round((p.compl / p.del) * 10) / 10 : 0,
      }))
      .sort((a, b) => b.fillRate - a.fillRate)
  }, [performance, campaignMap, platformMap])

  // ── Intelligence Board: aggregate KPIs
  const intelKpis = useMemo(() => {
    let req = 0, resp = 0, compl = 0, del = 0, slaPass = 0, slaTotal = 0
    for (const row of last24Rows) {
      req   += row.vast_requests
      resp  += row.vast_responses
      compl += row.video_completes
      del   += row.impressions_delivered
      if (row.avg_latency_ms > 0) {
        slaTotal++
        if (row.avg_latency_ms < 1800) slaPass++
      }
    }
    const fillRate  = req  > 0 ? (resp  / req)  * 100 : 0
    const vcr       = del  > 0 ? (compl / del)  * 100 : 0
    const slaComp   = slaTotal > 0 ? (slaPass / slaTotal) * 100 : 0
    const totalRev  = campaigns.reduce((s, c) => s + (c.target_impressions * c.cpm_usd / 1000), 0)
    const atRisk    = enrichedAlerts.reduce((s, a) => s + Number(a.revenue_impact_usd || 0), 0)
    const revEff    = totalRev > 0 ? ((totalRev - atRisk) / totalRev) * 100 : 0
    return { fillRate, vcr, slaComp, revEff, totalRev, atRisk }
  }, [last24Rows, campaigns, enrichedAlerts])


  // ── Intelligence Board: all distinct platform names
  const intelPlatformNames = useMemo(() => {
    const names = new Set(campaignHealth.map(h => h.platform_name).filter(Boolean))
    return ['All', ...Array.from(names).sort()]
  }, [campaignHealth])

  // ── Intelligence Board: filtered data scoped to selected platform
  const intelFilteredHealth = useMemo(() => {
    if (intelPlatform === 'All') return campaignHealth
    return campaignHealth.filter(h => h.platform_name === intelPlatform)
  }, [campaignHealth, intelPlatform])

  const intelFilteredRows = useMemo(() => {
    if (intelPlatform === 'All') return last24Rows
    return last24Rows.filter(row => {
      const c = campaignMap[row.campaign_id]; if (!c) return false
      return platformMap[c.platform_id]?.platform_name === intelPlatform
    })
  }, [last24Rows, intelPlatform, campaignMap, platformMap])

  // ── Intelligence Board: KPIs scoped to platform
  const intelPlatformKpis = useMemo(() => {
    let req = 0, resp = 0, compl = 0, del = 0, slaPass = 0, slaTotal = 0
    for (const row of intelFilteredRows) {
      req   += row.vast_requests
      resp  += row.vast_responses
      compl += row.video_completes
      del   += row.impressions_delivered
      if (row.avg_latency_ms > 0) { slaTotal++; if (row.avg_latency_ms < 1800) slaPass++ }
    }
    const fillRate = req  > 0 ? (resp  / req)  * 100 : 0
    const vcr      = del  > 0 ? (compl / del)  * 100 : 0
    const slaComp  = slaTotal > 0 ? (slaPass / slaTotal) * 100 : 0
    const pltCampaigns = intelFilteredHealth.length
    const alertCount = intelFilteredHealth.reduce((s, h) => s + h.alertCount, 0)
    const atRisk = intelPlatform === 'All' ? intelKpis.atRisk
      : enrichedAlerts.filter(a => {
          const c = campaignMap[a.campaign_id]
          return c && platformMap[c.platform_id]?.platform_name === intelPlatform
        }).reduce((s, a) => s + Number(a.revenue_impact_usd || 0), 0)
    return { fillRate, vcr, slaComp, pltCampaigns, alertCount, atRisk }
  }, [intelFilteredRows, intelFilteredHealth, intelPlatform, intelKpis.atRisk, enrichedAlerts, campaignMap, platformMap])

  // ── Intelligence Board: AI platform summary text
  const intelPlatformSummary = useMemo(() => {
    const { fillRate, vcr, slaComp, pltCampaigns, alertCount, atRisk } = intelPlatformKpis
    const name = intelPlatform === 'All' ? 'across all platforms' : `on ${intelPlatform}`
    const totalCamps = intelPlatform === 'All'
      ? `${Number(kpis.find((k) => k.kpi_name === 'Total Campaigns')?.value ?? 5247).toLocaleString()}+`
      : `${pltCampaigns}`
    const fillHealth = fillRate >= 85 ? 'strong' : fillRate >= 70 ? 'moderate' : 'poor'
    const vcrHealth  = vcr >= 70 ? 'healthy' : vcr >= 50 ? 'acceptable' : 'below benchmark'
    const slaHealth  = slaComp >= 90 ? 'meeting SLA' : 'breaching SLA'
    return `${totalCamps} active campaign${intelPlatform === 'All' ? 's' : pltCampaigns !== 1 ? 's' : ''} ${name}. Fill rate is ${fillHealth} at ${fillRate.toFixed(1)}%, video completion is ${vcrHealth} at ${vcr.toFixed(1)}%, and VAST latency is ${slaHealth} (${slaComp.toFixed(1)}% of requests under 1800ms). ${alertCount > 0 ? `${alertCount} open alert${alertCount !== 1 ? 's' : ''} account for ${new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',maximumFractionDigits:0}).format(atRisk)} in revenue exposure.` : 'No open alerts — platform health is normal.'} ${fillRate < 75 ? 'Recommend investigating inventory availability and targeting constraints.' : vcr < 50 ? 'Creative engagement is low — consider A/B testing shorter ad formats.' : alertCount > 0 ? 'Review and resolve open alerts in the Notifications tab to reduce revenue exposure.' : 'Platform health indicators are within normal range.'}`
  }, [intelPlatformKpis, intelPlatform, kpis])

  // ── Intelligence Board: hourly engagement scoped to platform
  const intelPlatformEngagement = useMemo(() => {
    const byHour = new Map<string, { req: number; resp: number; compl: number; del: number; err: number }>()
    for (const row of intelFilteredRows) {
      const slot = row.log_hour.slice(11, 16)
      const prev = byHour.get(slot) ?? { req: 0, resp: 0, compl: 0, del: 0, err: 0 }
      prev.req   += row.vast_requests
      prev.resp  += row.vast_responses
      prev.compl += row.video_completes
      prev.del   += row.impressions_delivered
      prev.err   += row.error_count
      byHour.set(slot, prev)
    }
    return [...byHour.keys()].sort().map((hour) => {
      const r = byHour.get(hour)!
      return {
        hour,
        fillRate:  r.req > 0 ? Math.round((r.resp  / r.req) * 1000) / 10 : 0,
        vcr:       r.del > 0 ? Math.round((r.compl / r.del) * 1000) / 10 : 0,
        errorRate: r.req > 0 ? Math.round((r.err   / r.req) * 1000) / 10 : 0,
      }
    })
  }, [intelFilteredRows])

  // ── Chat Q&A engine
  const answerChat = useCallback((q: string): string => {
    const lc = q.toLowerCase()

    // Delivery / pacing
    if (lc.includes('deliver') || lc.includes('pac')) {
      const rate = topMetrics.avgDeliveryRate
      const atRisk = campaignHealth.filter(h => h.deliveryRate < 90).length
      return `Average delivery rate across all active campaigns is ${rate.toFixed(1)}%. ${atRisk} campaign${atRisk !== 1 ? 's are' : ' is'} below the 90% pacing guardrail. ${rate >= 90 ? 'Overall pacing is healthy.' : 'Recommend reviewing inventory avails and frequency caps on underperforming campaigns.'}`
    }
    // Fill rate
    if (lc.includes('fill')) {
      return `Average VAST fill rate is ${intelKpis.fillRate.toFixed(1)}% over the last 24 hours. ${intelKpis.fillRate >= 85 ? 'This is above the 85% target — ad serving infrastructure is performing well.' : 'This is below the 85% target. Check for CDN degradation or VAST timeout issues in the Notifications tab.'}`
    }
    // VCR / completion
    if (lc.includes('vcr') || lc.includes('completion') || lc.includes('view') || lc.includes('engag')) {
      const top = campaignHealth.filter(c => c.vcr > 0).sort((a,b) => b.vcr - a.vcr)[0]
      return `Video completion rate is ${intelKpis.vcr.toFixed(1)}% across active campaigns. ${top ? `Best performer is ${top.campaign.campaign_name} at ${top.vcr.toFixed(1)}% VCR.` : ''} ${intelKpis.vcr >= 70 ? 'Audience engagement is healthy.' : 'Below the 70% benchmark — consider shorter creatives or tighter audience targeting.'}`
    }
    // Alerts
    if (lc.includes('alert') || lc.includes('issue') || lc.includes('problem') || lc.includes('critical')) {
      const crit = enrichedAlerts.filter(a => a.severity === 'Critical').length
      const high = enrichedAlerts.filter(a => a.severity === 'High').length
      return `There are ${enrichedAlerts.length} open alerts: ${crit} Critical, ${high} High, and ${enrichedAlerts.length - crit - high} Medium/Warning. Total revenue at risk is ${currencyFmt.format(topMetrics.revenueAtRisk)}. Head to the Notifications tab to review and resolve each alert.`
    }
    // Revenue
    if (lc.includes('revenue') || lc.includes('risk') || lc.includes('money') || lc.includes('$')) {
      return `${currencyFmt.format(topMetrics.revenueAtRisk)} of committed revenue is currently at risk from ${enrichedAlerts.length} unresolved alert${enrichedAlerts.length !== 1 ? 's' : ''}. Revenue efficiency is ${intelKpis.revEff.toFixed(1)}%. Approving agent-generated fixes in the Notifications tab is the fastest path to recovery.`
    }
    // Platform-specific
    const pltMatch = platforms.find(p => lc.includes(p.platform_name.toLowerCase()))
    if (pltMatch) {
      const pHealth = campaignHealth.filter(h => h.platform_name === pltMatch.platform_name)
      const avgDel = pHealth.length > 0 ? pHealth.reduce((s,h) => s + h.deliveryRate, 0) / pHealth.length : 0
      const pAlerts = enrichedAlerts.filter(a => {
        const c = campaignMap[a.campaign_id]
        return c && platformMap[c.platform_id]?.platform_name === pltMatch.platform_name
      })
      return `${pltMatch.platform_name} has ${pHealth.length} active campaign${pHealth.length !== 1 ? 's' : ''} with an average delivery rate of ${avgDel.toFixed(1)}%. There are ${pAlerts.length} open alert${pAlerts.length !== 1 ? 's' : ''} impacting this platform.`
    }
    // Campaign-specific
    const campMatch = campaigns.find(c => lc.includes(c.campaign_name.toLowerCase()))
    if (campMatch) {
      const h = campaignHealth.find(h => h.campaign.campaign_id === campMatch.campaign_id)
      if (h) {
        return `${campMatch.campaign_name}: delivery at ${h.deliveryRate.toFixed(1)}%, fill rate ${h.fillRate.toFixed(1)}%, VCR ${h.vcr.toFixed(1)}%, error rate ${h.errorRate.toFixed(2)}%. ${h.alertCount > 0 ? `${h.alertCount} open alert${h.alertCount !== 1 ? 's' : ''} — check the Notifications tab for details.` : 'No open alerts.'}`
      }
    }
    // Fallback
    return `I can answer questions about delivery pacing, fill rate, VCR, alerts, revenue at risk, and per-platform or per-campaign health. Try: "What's the fill rate?" or "How is MAX performing?"`
  }, [topMetrics, intelKpis, campaignHealth, enrichedAlerts, platforms, campaigns, campaignMap, platformMap])

  // ── Preview data helper
  const getPreviewData = useCallback((tableName: string): Record<string, unknown>[] => {
    const map: Record<string, Record<string, unknown>[]> = {
      advertisers: advertisers as unknown as Record<string, unknown>[],
      platforms: platforms as unknown as Record<string, unknown>[],
      campaigns: campaigns.slice(0, 50) as unknown as Record<string, unknown>[],
      alerts: alerts.slice(0, 50) as unknown as Record<string, unknown>[],
      kpi_summary: kpis as unknown as Record<string, unknown>[],
      performance_log: performance.slice(0, 50) as unknown as Record<string, unknown>[],
    }
    return map[tableName] ?? []
  }, [advertisers, platforms, campaigns, alerts, kpis, performance])

  // ── Agent trigger for notifications
  const triggerAgentAnalysis = useCallback((alertId: string, playbook: typeof AGENT_PLAYBOOK[string]) => {
    setAgentState(p => ({ ...p, [alertId]: 'running' }))
    setAgentStep(p => ({ ...p, [alertId]: 0 }))
    setExpandedAlert(alertId)
    playbook.steps.forEach((_, i) => {
      setTimeout(() => {
        setAgentStep(p => ({ ...p, [alertId]: i + 1 }))
        if (i === playbook.steps.length - 1) {
          setTimeout(() => setAgentState(p => ({ ...p, [alertId]: 'complete' })), 600)
        }
      }, (i + 1) * 900)
    })
  }, [])

  // ── Agent trigger for campaign health
  const triggerCampaignAnalysis = useCallback((campaignId: string) => {
    setCampaignAgentState(p => ({ ...p, [campaignId]: 'running' }))
    setCampaignAgentStep(p => ({ ...p, [campaignId]: 0 }))
    CAMPAIGN_HEALTH_STEPS.forEach((_, i) => {
      setTimeout(() => {
        setCampaignAgentStep(p => ({ ...p, [campaignId]: i + 1 }))
        if (i === CAMPAIGN_HEALTH_STEPS.length - 1) {
          setTimeout(() => setCampaignAgentState(p => ({ ...p, [campaignId]: 'complete' })), 600)
        }
      }, (i + 1) * 900)
    })
  }, [])

  // ── Auto-trigger campaign analysis when row is selected
  useEffect(() => {
    if (selectedCampaignId && !campaignAgentState[selectedCampaignId]) {
      triggerCampaignAnalysis(selectedCampaignId)
    }
  }, [selectedCampaignId, campaignAgentState, triggerCampaignAnalysis])

  // Count-up animated values — driven from data, must be before early return (Rules of Hooks)
  const totalCampaignsKpi = Number(kpis.find((k) => k.kpi_name === 'Total Campaigns')?.value ?? 5247)
  const countCampaigns = useCountUp(loading ? 0 : totalCampaignsKpi)
  const countAlerts    = useCountUp(loading ? 0 : topMetrics.activeAlerts)

  if (loading) {
    return (
      <div className="loading-shell">
        <div className="loading-card">
          <div className="loading-ring">
            <MonitorPlay size={22} />
          </div>
          <div>
            <p className="loading-title">Building AdOps Intelligence Workspace</p>
            <p className="loading-sub">Loading campaign data, alerts &amp; performance metrics…</p>
          </div>
        </div>
      </div>
    )
  }

  // ── Drill-down data helpers
  const activeByCampaigns = campaigns.filter((c) => c.status === 'Active')
  const streamingCount    = activeByCampaigns.filter((c) => getNetwork(platformMap[c.platform_id]?.platform_name ?? '') === 'Streaming').length
  const linearCount       = activeByCampaigns.length - streamingCount
  const tierBreakdown     = ['Gold', 'Silver', 'Bronze'].map((t) => ({
    name: t, value: activeByCampaigns.filter((c) => advertiserMap[c.advertiser_id]?.tier === t).length,
  }))
  const worstDelivery = campaignHealth
    .filter((h) => h.deliveryRate < 100)
    .map((h) => {
      const totalDel = h.recentRows.reduce((s, r) => s + r.impressions_delivered, 0)
      const revenueAtRisk = Math.max(0, (h.campaign.target_impressions - totalDel) * h.campaign.cpm_usd / 1000)
      return { ...h, revenueAtRisk }
    })
    .sort((a, b) => a.deliveryRate - b.deliveryRate)
  const alertTypeTotals = [...new Map(enrichedAlerts.map((a) => [a.alert_type, { type: a.alert_type, count: 0, revenue: 0 }])).values()]
  enrichedAlerts.forEach((a) => { const t = alertTypeTotals.find((x) => x.type === a.alert_type); if (t) { t.count++; t.revenue += Number(a.revenue_impact_usd) } })
  const topRevenueAlerts = [...enrichedAlerts].sort((a, b) => Number(b.revenue_impact_usd) - Number(a.revenue_impact_usd)).slice(0, 8)

  // ── Unread badge count
  const unreadCount = enrichedAlerts.filter((a) => !reviewDecisions[a.alert_id]).length

  return (
    <div className="app-shell">
      {/* ── ANIMATED BACKGROUND ── */}
      <div className="bg-canvas" aria-hidden="true">
        <div className="orb orb-1" />
        <div className="orb orb-2" />
        <div className="orb orb-3" />
        <div className="orb orb-4" />
      </div>

      {/* ── TOP NAV ── */}
      <header className="top-nav">
        <div className="brand">
          <div className="brand-mark">
            <MonitorPlay size={16} />
          </div>
          <div>
            <strong>AdOps Copilot</strong>
            <span>Warner Network Control Plane</span>
          </div>
        </div>
        <nav className="tabs">
          <button className={`tab ${activeTab === 'overview' ? 'active' : ''}`} onClick={() => setActiveTab('overview')}>
            <LayoutDashboard size={14} /> Overview
          </button>
          <button className={`tab ${activeTab === 'notifications' ? 'active' : ''}`} onClick={() => setActiveTab('notifications')}>
            <Bell size={14} /> Notifications
            {unreadCount > 0 && <span className="notif-badge">{unreadCount}</span>}
          </button>
          <button className={`tab ${activeTab === 'health' ? 'active' : ''}`} onClick={() => setActiveTab('health')}>
            <Activity size={14} /> Campaign Health
          </button>
          <button className={`tab ${activeTab === 'intel' ? 'active' : ''}`} onClick={() => setActiveTab('intel')}>
            <Bot size={14} /> Intelligence Feed
          </button>
          <button className={`tab ${activeTab === 'explorer' ? 'active' : ''}`} onClick={() => setActiveTab('explorer')}>
            <Database size={14} /> Data Explorer
          </button>
        </nav>
        <div className="status-pill" title="1 of 6 monitored systems is degraded. CDN node us-east-2b: 43% packet loss. Ad decision latency above SLA threshold. Click Notifications tab to view active alerts.">
          <span className="live-dot" />
          <span>System Health</span>
          <strong style={{ color: '#ef4444' }}>1 System Degraded</strong>
          <span className="status-pill-hint">CDN node us-east-2b · hover for details</span>
        </div>
      </header>

      {/* ════════════════════════════════════════════════════════════════
          OVERVIEW TAB
      ════════════════════════════════════════════════════════════════ */}
      {activeTab === 'overview' && (
      <main key="overview" className="content-grid tab-content">
        <section className="metric-grid">
          <article className="metric-card clickable" style={{ '--accent': '#FF5800' } as React.CSSProperties} onClick={() => setDrilldown('campaigns')}>
            <div className="metric-head">
              <span>Active Campaigns</span>
              <div className="metric-icon" style={{ background: 'rgba(255,88,0,0.12)', color: '#FF5800' }}><TrendingUp size={15} /></div>
            </div>
            <h2>{numberFmt.format(countCampaigns)}+</h2>
            <p className="positive">Live across Streaming + Linear</p>
            <span className="drill-hint">Explore breakdown <ChevronRight size={11} /></span>
          </article>
          <article className="metric-card clickable" style={{ '--accent': '#FF8C42' } as React.CSSProperties} onClick={() => setDrilldown('delivery')}>
            <div className="metric-head">
              <span>Average Delivery Rate</span>
              <div className="metric-icon" style={{ background: 'rgba(255,140,66,0.10)', color: '#FF8C42' }}><MonitorPlay size={15} /></div>
            </div>
            <h2>94.3<span className="metric-unit">%</span></h2>
            <p className="positive">↑ from 91.2% (7 day rolling average)</p>
            <span className="drill-hint">Explore breakdown <ChevronRight size={11} /></span>
          </article>
          <article className="metric-card clickable" style={{ '--accent': '#ef4444' } as React.CSSProperties} onClick={() => setDrilldown('alerts')}>
            <div className="metric-head">
              <span>Active Alerts</span>
              <div className="metric-icon" style={{ background: 'rgba(239,68,68,0.12)', color: '#ef4444' }}><ShieldAlert size={15} /></div>
            </div>
            <h2>{numberFmt.format(countAlerts)}</h2>
            <p className="negative">Critical issues requiring intervention</p>
            <span className="drill-hint">Explore breakdown <ChevronRight size={11} /></span>
          </article>
          <article className="metric-card clickable" style={{ '--accent': '#f59e0b' } as React.CSSProperties} onClick={() => setDrilldown('revenue')}>
            <div className="metric-head">
              <span>Revenue at Risk</span>
              <div className="metric-icon" style={{ background: 'rgba(245,158,11,0.10)', color: '#f59e0b' }}><CircleDollarSign size={15} /></div>
            </div>
            <h2>${compactFmt.format(topMetrics.revenueAtRisk)}</h2>
            <p className="negative">↓ from $4.2M yesterday · 3 issues AI-resolved</p>
            <span className="drill-hint">Explore breakdown <ChevronRight size={11} /></span>
          </article>
        </section>

        <div className="ai-actions-banner">
          🤖 <strong>AI Actions Today:</strong> 12 issues auto-resolved &nbsp;|&nbsp; $1.8M revenue protected &nbsp;|&nbsp; 68 analyst-hours saved
          <span className="last-refreshed-inline">Last refreshed: {secondsSinceRefresh}s ago</span>
        </div>

        <div className="overview-toolbar">
          <button type="button" className="lightweight-toggle" onClick={() => setIsLightweightMode((p) => !p)}>
            <span className="lt-label">{isLightweightMode ? '⚡ Fast Mode' : '📊 Full Data'}</span>
            <span className="lt-desc">{isLightweightMode ? 'Sampling 1 in 8 rows — faster load' : 'Loading all performance rows'}</span>
          </button>
        </div>

        <section className="chart-card pacing-card clickable" onClick={() => setDrilldown('pacing')}>
          <div className="chart-title-row">
            <h3>Network Delivery Pacing (Last 24h)</h3>
            <span className="drill-hint-inline">Drill in <ChevronRight size={12} /></span>
          </div>
          <ResponsiveContainer width="100%" height={240}>
            <AreaChart data={DEMO_PACING_SERIES} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="actualFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%"  stopColor="#FF5800" stopOpacity={0.5} />
                  <stop offset="95%" stopColor="#FF5800" stopOpacity={0.03} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="#e5e7eb" strokeDasharray="3 3" />
              <XAxis dataKey="hour" stroke="#9EA3B0" tick={{ fontSize: 10, fill: '#555b6e' }} />
              <YAxis stroke="#5A5F6E" tickFormatter={(v) => compactFmt.format(v)} tick={{ fontSize: 10 }} />
              <Tooltip contentStyle={{ background: '#ffffff', border: '1px solid rgba(255,88,0,0.25)', borderRadius: 12, boxShadow: '0 4px 16px rgba(0,0,0,0.08)', color: '#1a1a1a' }}
                formatter={(v) => numberFmt.format(Number(v ?? 0))} />
              <Area type="monotone" dataKey="target" stroke="#5A5F6E" strokeDasharray="4 4" fill="none" name="Target" />
              <Area type="monotone" dataKey="actual" stroke="#FF5800" fill="url(#actualFill)" name="Actual" />
              <ReferenceLine x="01:00" stroke="#ef4444" strokeDasharray="3 3" strokeWidth={1.5} />
              <ReferenceLine x="04:00" stroke="#22c55e" strokeDasharray="3 3" strokeWidth={1.5} />
              <ReferenceLine x="12:00" stroke="#f59e0b" strokeDasharray="3 3" strokeWidth={1.5} />
              <ReferenceLine x="20:00" stroke="#FF8C42" strokeDasharray="3 3" strokeWidth={1.5} />
            </AreaChart>
          </ResponsiveContainer>
          <div className="pacing-events">
            <div className="pacing-event"><span className="pe-dot" style={{ background: '#ef4444' }} />01:00 — Campaign halted — audience sync failure</div>
            <div className="pacing-event"><span className="pe-dot" style={{ background: '#22c55e' }} />03:47 — AI fix deployed — delivery restored</div>
            <div className="pacing-event"><span className="pe-dot" style={{ background: '#f59e0b' }} />12:00 — FreeWheel timeout spike — 847 requests failed</div>
            <div className="pacing-event"><span className="pe-dot" style={{ background: '#FF8C42' }} />20:00 — Max prime-time surge</div>
          </div>
        </section>

        <section className="chart-card donut-card clickable" onClick={() => setDrilldown('fallout')}>
          <div className="chart-title-row">
            <h3>Fallout Reasons (24h)</h3>
            <span className="drill-hint-inline">Drill in <ChevronRight size={12} /></span>
          </div>
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie data={falloutSeries} innerRadius={60} outerRadius={90} dataKey="value" nameKey="name" paddingAngle={2}>
                {falloutSeries.map((e, i) => <Cell key={e.name} fill={PIE_COLORS[i % PIE_COLORS.length]} />)}
              </Pie>
              <Tooltip contentStyle={{ background: '#ffffff', border: '1px solid rgba(255,88,0,0.25)', borderRadius: 12, boxShadow: '0 4px 16px rgba(0,0,0,0.08)', color: '#1a1a1a' }}
                formatter={(v) => numberFmt.format(Number(v ?? 0))} />
            </PieChart>
          </ResponsiveContainer>
          <div className="legend-list">
            {falloutSeries.map((r, i) => (
              <div key={r.name}><span style={{ backgroundColor: PIE_COLORS[i % PIE_COLORS.length] }} />{r.name} ({r.value})</div>
            ))}
          </div>
        </section>

        {/* Campaigns at Risk */}
        <section className="chart-card at-risk-card">
          <div className="chart-title-row">
            <h3>Campaigns at Risk — Delivery Below Target</h3>
            <button type="button" className="see-all-btn" onClick={() => setActiveTab('health')}>View All <ChevronRight size={12} /></button>
          </div>
          <div className="at-risk-widgets">
            {campaignHealth.filter(h => h.deliveryRate < 90).slice(0, 4).map(h => {
              const riskAlert = enrichedAlerts.find(a => a.campaign_id === h.campaign.campaign_id)
              const riskColor = h.deliveryRate < 20 ? '#ef4444' : h.deliveryRate < 75 ? '#f59e0b' : '#FF5800'
              const riskLabel = h.deliveryRate < 20 ? 'Critical' : h.deliveryRate < 75 ? 'At Risk' : 'Below Target'
              const shortfall = h.recentRows.length > 0
                ? Math.max(0, h.recentRows.reduce((s, r) => {
                    const c = h.campaign; const exp = c.target_impressions / Math.max(1, flightHours(c))
                    return s + (exp - r.impressions_delivered)
                  }, 0))
                : 0
              const revenueAtRisk = Math.max(0, (h.campaign.target_impressions - h.recentRows.reduce((s, r) => s + r.impressions_delivered, 0)) * h.campaign.cpm_usd / 1000)
              return (
                <button
                  key={h.campaign.campaign_id}
                  type="button"
                  className="at-risk-widget"
                  style={{ '--risk-color': riskColor } as React.CSSProperties}
                  onClick={() => {
                    if (riskAlert) { setActiveTab('notifications'); setExpandedAlert(riskAlert.alert_id) }
                    else { setActiveTab('health') }
                  }}
                >
                  <div className="arw-top">
                    <span className="arw-risk-badge" style={{ background: `${riskColor}18`, color: riskColor, borderColor: `${riskColor}40` }}>{riskLabel}</span>
                    {h.alertCount > 0 && (
                      <span className={`severity-pill ${(h.topSeverity ?? '').toLowerCase()}`}>{h.alertCount} alert{h.alertCount !== 1 ? 's' : ''}</span>
                    )}
                  </div>
                  <div className="arw-name">{h.campaign.campaign_name}</div>
                  <div className="arw-meta">{h.platform_name} · {h.advertiser_name}</div>
                  <div className="arw-bar-row">
                    <div className="arw-bar-track">
                      <div className="arw-bar-fill" style={{ width: `${Math.max(2, h.deliveryRate)}%`, background: riskColor }} />
                    </div>
                    <span className="arw-pct" style={{ color: riskColor }}>{h.deliveryRate.toFixed(0)}%</span>
                  </div>
                  <div className="arw-risk-line">
                    {shortfall > 0
                      ? <><AlertTriangle size={10} /> {compactFmt.format(shortfall)} imp shortfall · {currencyFmt.format(revenueAtRisk)} at risk</>
                      : <><AlertTriangle size={10} /> {h.alertCount} active alert{h.alertCount !== 1 ? 's' : ''}</>
                    }
                  </div>
                  <div className="arw-cta">View in Notifications <ChevronRight size={10} /></div>
                </button>
              )
            })}
            {campaignHealth.filter(h => h.deliveryRate < 90).length === 0 && (
              <p style={{ textAlign: 'center', color: 'var(--text-muted)', padding: '20px 0', fontSize: '0.84rem' }}>All campaigns within pacing guardrails ✓</p>
            )}
          </div>
        </section>

        {/* Alert Type Revenue Breakdown */}
        <section className="chart-card alert-revenue-card clickable" onClick={() => setDrilldown('fallout')}>
          <div className="chart-title-row">
            <h3>Revenue Exposure by Alert Type</h3>
            <span className="drill-hint-inline">Drill in <ChevronRight size={12} /></span>
          </div>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart layout="vertical"
              data={alertTypeTotals.sort((a, b) => b.revenue - a.revenue).slice(0, 5)}
              margin={{ top: 4, right: 60, left: 130, bottom: 0 }}>
              <CartesianGrid stroke="#e5e7eb" strokeDasharray="3 3" horizontal={false} />
              <XAxis type="number" stroke="#9EA3B0" tickFormatter={(v) => `$${compactFmt.format(v)}`} tick={{ fontSize: 10, fill: '#555b6e' }} />
              <YAxis type="category" dataKey="type" stroke="#9EA3B0" tick={{ fontSize: 10, fill: '#555b6e' }} width={125} />
              <Tooltip contentStyle={{ background: '#ffffff', border: '1px solid rgba(255,88,0,0.25)', borderRadius: 10, boxShadow: '0 4px 16px rgba(0,0,0,0.08)', color: '#1a1a1a' }}
                formatter={(v) => currencyFmt.format(Number(v))} />
              <Bar dataKey="revenue" fill="#FF5800" radius={[0, 4, 4, 0]} name="Revenue at Risk" />
            </BarChart>
          </ResponsiveContainer>
        </section>
      </main>
      )}

      {/* ════════════════════════════════════════════════════════════════
          NOTIFICATIONS TAB (2nd) — Two-Pane Layout
      ════════════════════════════════════════════════════════════════ */}
      {activeTab === 'notifications' && (() => {
        const filteredAlerts = enrichedAlerts.filter((a) => notifFilter === 'All' || a.severity === notifFilter)
        const activeAlert = expandedAlert ? enrichedAlerts.find(a => a.alert_id === expandedAlert) ?? null : null
        return (
        <main key="notifications" className="notif-shell tab-content">
          {/* Header — spans both columns */}
          <div className="notif-header">
            <div className="notif-title">
              <Bell size={16} /> <strong>Alert Notifications</strong>
              <span>{enrichedAlerts.length} open · {Object.values(reviewDecisions).filter((d) => d === 'approved').length} deployed · {Object.values(reviewDecisions).filter((d) => d === 'rejected').length} rejected</span>
            </div>
            <div className="notif-filters">
              {['All', 'Critical', 'High', 'Medium', 'Warning'].map((f) => (
                <button key={f} className={`filter-btn ${notifFilter === f ? 'active' : ''}`} onClick={() => setNotifFilter(f)} type="button">{f}</button>
              ))}
            </div>
          </div>

          {/* Left pane — clean card list */}
          <div className="notif-left-pane">
            {filteredAlerts.map((alert) => {
              const dec = reviewDecisions[alert.alert_id]
              const isActive = expandedAlert === alert.alert_id
              const aState = agentState[alert.alert_id]
              return (
                <button
                  key={alert.alert_id}
                  type="button"
                  className={`notif-list-card ${dec ?? ''} ${isActive ? 'active-card' : ''}`}
                  onClick={() => setExpandedAlert(isActive ? null : alert.alert_id)}
                >
                  <div className="nlc-top">
                    <span className={`severity-pill ${alert.severity.toLowerCase()}`}>{alert.severity}</span>
                    <span className="nlc-revenue">{currencyFmt.format(Number(alert.revenue_impact_usd))}</span>
                  </div>
                  <div className="nlc-type">{alert.alert_type}</div>
                  <div className="nlc-campaign">{alert.campaign_name}</div>
                  {alert.description && <div className="nlc-description">{alert.description}</div>}
                  <div className="nlc-meta">{alert.platform_name} · {alert.advertiser_name}</div>
                  <div className="nlc-footer">
                    {dec === 'approved' && <span className="dec-badge approved"><CheckCircle2 size={10} /> Deployed</span>}
                    {dec === 'rejected' && <span className="dec-badge rejected"><XCircle size={10} /> Rejected</span>}
                    {!dec && aState === 'complete' && <span className="dec-badge pending"><CheckCircle2 size={10} /> Analyzed</span>}
                    {!dec && (!aState || aState === 'idle') && <span className="nlc-cta">Click to review →</span>}
                    {!dec && aState === 'running' && <span className="nlc-running"><span className="nlc-dot" />Analyzing…</span>}
                  </div>
                </button>
              )
            })}
          </div>

          {/* Right pane */}
          <div className="notif-right-pane">
            {!activeAlert ? (
              <div className="notif-right-empty">
                <Bot size={44} />
                <strong>Select an alert to begin</strong>
                <p>Click any notification on the left to view context and launch AI root cause analysis.</p>
              </div>
            ) : (() => {
              const alert = activeAlert
              const pb    = AGENT_PLAYBOOK[alert.alert_type]
              const dec   = reviewDecisions[alert.alert_id]
              const rca   = buildRca(alert)
              const aState = agentState[alert.alert_id]
              const aStep  = agentStep[alert.alert_id] ?? 0
              const hypotheses = ALERT_HYPOTHESES[alert.alert_type] ?? []

              // Shared header
              const header = (
                <div className="notif-analysis-header">
                  <div className="notif-analysis-meta">
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                      <span className={`severity-pill ${alert.severity.toLowerCase()}`}>{alert.severity}</span>
                      <strong style={{ fontSize: '1rem' }}>{alert.alert_type}</strong>
                    </div>
                    <span>{alert.campaign_name} · {alert.advertiser_name} · {alert.platform_name}</span>
                    <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{alert.alert_timestamp} · {alert.alert_id}</span>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 5, flexShrink: 0 }}>
                    <span style={{ fontSize: '1rem', color: '#ef4444', fontWeight: 700 }}>{currencyFmt.format(Number(alert.revenue_impact_usd))}</span>
                    <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>revenue at risk</span>
                    {dec === 'approved' && <span className="dec-badge approved"><CheckCircle2 size={11} /> Deployed</span>}
                    {dec === 'rejected' && <span className="dec-badge rejected"><XCircle size={11} /> Rejected</span>}
                    {!dec && <span className="dec-badge pending"><Bot size={11} /> Awaiting Review</span>}
                  </div>
                </div>
              )

              // State 1: No agent started yet — show context + hypotheses + Deep Dive button
              if (!aState || aState === 'idle') {
                return (
                  <div className="notif-analysis-pane">
                    {header}

                    {/* Alert context */}
                    <div className="notif-context-box">
                      <div className="nd-label"><AlertTriangle size={13} /> Alert Signal</div>
                      <p className="notif-trigger-text">{alert.trigger_value}</p>
                      <div className="notif-impact-row">
                        <div><span className="impact-label">Impression Shortfall</span><span className="impact-val">{numberFmt.format(alert.expected_impressions - alert.actual_impressions)}</span></div>
                        <div><span className="impact-label">Threshold</span><span className="impact-val">{alert.threshold}</span></div>
                        <div><span className="impact-label">Network</span><span className="impact-val">{alert.network ?? (platformMap[campaignMap[alert.campaign_id]?.platform_id ?? '']?.platform_name?.includes('TNT') || platformMap[campaignMap[alert.campaign_id]?.platform_id ?? '']?.platform_name?.includes('TBS') ? 'Linear' : 'Streaming')}</span></div>
                      </div>
                    </div>

                    {/* Hypotheses */}
                    {hypotheses.length > 0 && pb && (
                      <div className="notif-hypotheses">
                        <div className="nd-label"><Sparkles size={13} /> Potential Hypotheses</div>
                        <div className="hypothesis-list">
                          {hypotheses.map((h, i) => (
                            <div key={i} className="hypothesis-item">
                              <span className="hypothesis-num">{i + 1}</span>
                              <span>{h}</span>
                            </div>
                          ))}
                        </div>
                        <p className="hypothesis-note">AI agent will investigate and confirm which hypothesis is root cause.</p>
                      </div>
                    )}

                    {!dec && pb && (
                      <button
                        className="deep-dive-full-btn"
                        type="button"
                        onClick={() => triggerAgentAnalysis(alert.alert_id, pb)}
                      >
                        <Bot size={16} /> Launch Deep Dive &amp; Resolve
                      </button>
                    )}
                    {!pb && <div className="notif-right-empty" style={{ minHeight: 80 }}><p>No agent playbook for this alert type.</p></div>}
                  </div>
                )
              }

              // State 2: Agent running — live animation only
              if (aState === 'running') {
                return (
                  <div className="notif-analysis-pane">
                    {header}
                    <div className="agent-running-panel">
                      <div className="agent-running-head">
                        <div className="agent-spinner"><Bot size={16} /></div>
                        <div>
                          <strong>AI Agent Working</strong>
                          <span>Running root cause analysis — please wait…</span>
                        </div>
                      </div>
                      <div className="agent-steps-live">
                        {pb.steps.map((step, i) => (
                          <div key={step.tool} className={`agent-step-live ${i < aStep ? 'done' : i === aStep ? 'active' : 'pending'}`}>
                            <div className="asl-icon">
                              {i < aStep ? <CheckCircle2 size={11} /> : (
                                <>
                                  {step.icon === 'search'   && <Search   size={11} />}
                                  {step.icon === 'database' && <Database size={11} />}
                                  {step.icon === 'terminal' && <Terminal size={11} />}
                                  {step.icon === 'activity' && <Activity size={11} />}
                                  {step.icon === 'zap'      && <Zap      size={11} />}
                                </>
                              )}
                            </div>
                            <div className="asl-body">
                              <code>{step.tool}()</code>
                              <span>{step.desc}</span>
                              {i === aStep && step.thought && (
                                <span className="asl-thought"><Sparkles size={9} /> {step.thought}</span>
                              )}
                              {i < aStep && <span className="asl-result">↳ {step.result}</span>}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                )
              }

              // State 3: Complete — sequential reveal with animation delays
              return (
                <div className="notif-analysis-pane">
                  {header}

                  {/* Steps summary with thought process + Python query */}
                  <div className="nd-section fade-in-section" style={{ animationDelay: '0.1s' }}>
                    <div className="nd-label"><Bot size={13} /> Agent Steps Completed</div>
                    <div className="agent-steps-detailed">
                      {pb.steps.map((step, i) => (
                        <div className={`asd-step step-anim-${i}`} key={step.tool}>
                          <div className="asd-header">
                            <div className="asd-check"><CheckCircle2 size={11} /></div>
                            <div className="asd-main">
                              <div className="asd-tool-row">
                                <code className="step-tool">{step.tool}()</code>
                                <span className="asd-step-num">Step {i + 1}</span>
                              </div>
                              <span className="asd-desc">{step.desc}</span>
                              <span className="step-result">→ {step.result}</span>
                            </div>
                          </div>
                          {step.thought && (
                            <div className="asd-thought">
                              <span className="asd-thought-label"><Sparkles size={10} /> Agent Reasoning</span>
                              <p>{step.thought}</p>
                            </div>
                          )}
                          {step.query && (
                            <div className="asd-query">
                              <div className="asd-query-head"><Terminal size={10} /> Python Query</div>
                              <pre className="asd-code">{step.query}</pre>
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>

                  {/* Root cause */}
                  <div className="nd-section fade-in-section" style={{ animationDelay: '0.5s' }}>
                    <div className="nd-label"><Search size={13} /> Root Cause <span style={{ color: 'var(--green)', marginLeft: 4 }}>{rca.confidence}% confidence</span></div>
                    <div className="rca-text-box">
                      <p>{pb.rootCause}</p>
                    </div>
                  </div>

                  {/* System trace */}
                  <div className="nd-section fade-in-section" style={{ animationDelay: '1.0s' }}>
                    <div className="nd-label"><Terminal size={13} /> System Trace</div>
                    <pre className="notif-trace">{rca.trace.join('\n')}</pre>
                  </div>

                  {/* Fix artifact */}
                  <div className="nd-section fade-in-section" style={{ animationDelay: '1.5s' }}>
                    <div className="nd-label">
                      <Zap size={13} /> Proposed Fix
                      <span className="fix-type-badge">{pb.fixType}</span>
                      <span className="fix-recovery">{pb.recoveryTime}</span>
                    </div>
                    <pre className="fix-code">{pb.fixCode}</pre>
                  </div>

                  {/* Revenue impact */}
                  <div className="nd-section nd-impact fade-in-section" style={{ animationDelay: '2.0s' }}>
                    <div><span className="impact-label">Revenue at Risk</span><span className="impact-val">{currencyFmt.format(Number(alert.revenue_impact_usd))}</span></div>
                    <div><span className="impact-label">Impression Shortfall</span><span className="impact-val">{numberFmt.format(alert.expected_impressions - alert.actual_impressions)}</span></div>
                    <div><span className="impact-label">Alert ID</span><span className="impact-val code">{alert.alert_id}</span></div>
                  </div>

                  {/* Before / After delivery chart */}
                  <div className="nd-section fade-in-section" style={{ animationDelay: '2.2s' }}>
                    <div className="nd-label"><Activity size={13} /> Delivery Impact — Before &amp; After AI Fix</div>
                    <div className="before-after-panel">
                      <div className="ba-chart-box before">
                        <div className="ba-chart-title">Before</div>
                        <ResponsiveContainer width="100%" height={90}>
                          <AreaChart data={[{t:'0h',v:0},{t:'1h',v:0},{t:'2h',v:0},{t:'3h',v:0},{t:'4h',v:0}]}
                            margin={{ top: 4, right: 4, left: -28, bottom: 0 }}>
                            <defs>
                              <linearGradient id="beforeFill" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%"  stopColor="#ef4444" stopOpacity={0.4} />
                                <stop offset="95%" stopColor="#ef4444" stopOpacity={0.02} />
                              </linearGradient>
                            </defs>
                            <XAxis dataKey="t" stroke="#9EA3B0" tick={{ fontSize: 9, fill: '#9EA3B0' }} />
                            <YAxis domain={[0, 100]} stroke="#5A5F6E" tick={{ fontSize: 9 }} tickFormatter={(v) => `${v}%`} />
                            <Area type="monotone" dataKey="v" stroke="#ef4444" fill="url(#beforeFill)" name="Delivery %" />
                          </AreaChart>
                        </ResponsiveContainer>
                        <div className="ba-chart-label">4 hours — zero ads delivered — {currencyFmt.format(Number(alert.revenue_impact_usd))} lost</div>
                      </div>
                      <div className="ba-chart-box after">
                        <div className="ba-chart-title">After AI Fix</div>
                        <ResponsiveContainer width="100%" height={90}>
                          <AreaChart data={[{t:'Fix',v:0},{t:'+15m',v:28},{t:'+30m',v:64},{t:'+45m',v:88},{t:'+1h',v:94}]}
                            margin={{ top: 4, right: 4, left: -28, bottom: 0 }}>
                            <defs>
                              <linearGradient id="afterFill" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%"  stopColor="#22c55e" stopOpacity={0.4} />
                                <stop offset="95%" stopColor="#22c55e" stopOpacity={0.02} />
                              </linearGradient>
                            </defs>
                            <XAxis dataKey="t" stroke="#9EA3B0" tick={{ fontSize: 9, fill: '#9EA3B0' }} />
                            <YAxis domain={[0, 100]} stroke="#5A5F6E" tick={{ fontSize: 9 }} tickFormatter={(v) => `${v}%`} />
                            <Area type="monotone" dataKey="v" stroke="#22c55e" fill="url(#afterFill)" name="Delivery %" />
                          </AreaChart>
                        </ResponsiveContainer>
                        <div className="ba-chart-label">Delivery restored — {numberFmt.format(alert.expected_impressions)} impressions recovered</div>
                      </div>
                    </div>
                  </div>

                  {/* FreeWheel comparison */}
                  <div className="nd-section fade-in-section" style={{ animationDelay: '2.35s' }}>
                    <div className="freewheel-comparison">
                      <h4>Why wasn't this caught by existing tools?</h4>
                      <div className="fw-compare-grid">
                        <div className="fw-col-header left">What FreeWheel showed</div>
                        <div className="fw-col-header right">What AdOps Copilot found</div>
                        <div className="fw-cell left">"Delivery dropped — alert sent"</div>
                        <div className="fw-cell right">Audience sync failed 36 hours ago</div>
                        <div className="fw-cell left">No further detail</div>
                        <div className="fw-cell right">3 campaigns affected — all halted</div>
                        <div className="fw-cell left">Engineer manually assigned</div>
                        <div className="fw-cell right">Exact fix generated in 43 seconds</div>
                        <div className="fw-cell left">Problem fixed 2 hours later</div>
                        <div className="fw-cell right">Fix deployed — delivery fully recovered</div>
                      </div>
                    </div>
                  </div>

                  {/* Actions */}
                  {!dec && (
                    <div className="nd-actions fade-in-section" style={{ animationDelay: '2.4s' }}>
                      <button className="btn-approve" type="button"
                        onClick={() => setReviewDecisions((p) => ({ ...p, [alert.alert_id]: 'approved' }))}>
                        <CheckCircle2 size={14} /> Approve &amp; Deploy
                      </button>
                      <div className="reject-group">
                        <input
                          className="reject-reason-input"
                          placeholder="Rejection reason (optional)…"
                          value={rejectReason[alert.alert_id] ?? ''}
                          onChange={(e) => setRejectReason((p) => ({ ...p, [alert.alert_id]: e.target.value }))}
                        />
                        <button className="btn-reject" type="button"
                          onClick={() => setReviewDecisions((p) => ({ ...p, [alert.alert_id]: 'rejected' }))}>
                          <XCircle size={14} /> Reject
                        </button>
                      </div>
                    </div>
                  )}
                  {dec === 'approved' && <div className="nd-deployed fade-in-section"><CheckCircle2 size={15} /> Fix approved and queued for deployment.</div>}
                  {dec === 'rejected'  && <div className="nd-rejected fade-in-section"><XCircle size={15} /> Fix rejected.{rejectReason[alert.alert_id] ? ` Reason: ${rejectReason[alert.alert_id]}` : ''}</div>}
                </div>
              )
            })()}
          </div>
        </main>
        )
      })()}

      {/* ════════════════════════════════════════════════════════════════
          CAMPAIGN HEALTH TAB
      ════════════════════════════════════════════════════════════════ */}
      {activeTab === 'health' && (
      <main key="health" className="health-shell tab-content">
        <div className="health-summary">
          {[
            { label: 'Total Campaigns', count: `${Number(kpis.find((k) => k.kpi_name === 'Total Campaigns')?.value ?? 5247).toLocaleString()}+`, color: '#9EA3B0' },
            { label: 'Healthy (≥90%)',  count: campaignHealth.filter((h) => h.deliveryRate >= 90).length, color: '#22c55e' },
            { label: 'At Risk (75–89%)', count: campaignHealth.filter((h) => h.deliveryRate >= 75 && h.deliveryRate < 90).length, color: '#f59e0b' },
            { label: 'Critical (<75%)', count: campaignHealth.filter((h) => h.deliveryRate < 75).length, color: '#ef4444' },
            { label: 'With Open Alerts', count: enrichedAlerts.length, color: '#FF5800' },
          ].map((s) => (
            <div className="health-stat" key={s.label}>
              <span className="health-stat-num" style={{ color: s.color }}>{s.count}</span>
              <span className="health-stat-label">{s.label}</span>
            </div>
          ))}
        </div>

        <div className="health-layout">
          <div className="health-table-wrap">
            <table className="health-table">
              <thead>
                <tr>
                  {(['campaign','advertiser','platform'] as HealthSortCol[]).map(col => (
                    <th key={col} className="sortable" onClick={() => toggleHealthSort(col)}>
                      {col.charAt(0).toUpperCase()+col.slice(1)}
                      {healthSortCol === col ? (healthSortDir === 'asc' ? ' ↑' : ' ↓') : ' ↕'}
                    </th>
                  ))}
                  {([['delivery','Delivery %'],['fill','Fill %'],['vcr','VCR %'],['error','Error %'],['alerts','Alerts']] as [HealthSortCol,string][]).map(([col,label]) => (
                    <th key={col} className="sortable" onClick={() => toggleHealthSort(col)}>
                      {label}
                      {healthSortCol === col ? (healthSortDir === 'asc' ? ' ↑' : ' ↓') : ' ↕'}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {campaignHealth.filter((h) => h.recentRows.length > 0 || h.alertCount > 0).sort((a, b) => {
                  let cmp = 0
                  if (healthSortCol === 'alerts') {
                    cmp = severityWeight(a) - severityWeight(b)
                  } else if (healthSortCol === 'campaign') {
                    cmp = a.campaign.campaign_name.localeCompare(b.campaign.campaign_name)
                  } else if (healthSortCol === 'advertiser') {
                    cmp = (a.advertiser_name ?? '').localeCompare(b.advertiser_name ?? '')
                  } else if (healthSortCol === 'platform') {
                    cmp = (a.platform_name ?? '').localeCompare(b.platform_name ?? '')
                  } else if (healthSortCol === 'delivery') {
                    cmp = a.deliveryRate - b.deliveryRate
                  } else if (healthSortCol === 'fill') {
                    cmp = a.fillRate - b.fillRate
                  } else if (healthSortCol === 'vcr') {
                    cmp = a.vcr - b.vcr
                  } else if (healthSortCol === 'error') {
                    cmp = a.errorRate - b.errorRate
                  }
                  return healthSortDir === 'asc' ? cmp : -cmp
                }).map((h) => (
                  <tr key={h.campaign.campaign_id}
                    className={`health-row ${selectedCampaignId === h.campaign.campaign_id ? 'selected' : ''}`}
                    onClick={() => setSelectedCampaignId(h.campaign.campaign_id === selectedCampaignId ? null : h.campaign.campaign_id)}>
                    <td className="hcell-name">
                      <span>{h.campaign.campaign_name}</span>
                      <code>{h.campaign.campaign_id}</code>
                    </td>
                    <td>{h.advertiser_name}</td>
                    <td>{h.platform_name}</td>
                    <td>
                      <div className="health-bar-wrap">
                        <div className="health-bar" style={{ width: `${Math.min(100, h.deliveryRate)}%`, background: healthColor(h.deliveryRate) }} />
                        <span style={{ color: healthColor(h.deliveryRate) }}>{h.deliveryRate.toFixed(1)}%</span>
                      </div>
                    </td>
                    <td style={{ color: h.fillRate >= 95 ? '#22c55e' : h.fillRate >= 80 ? '#f59e0b' : '#ef4444' }}>{h.fillRate.toFixed(1)}%</td>
                    <td style={{ color: h.vcr >= 70 ? '#22c55e' : h.vcr >= 50 ? '#f59e0b' : '#ef4444' }}>{h.vcr.toFixed(1)}%</td>
                    <td style={{ color: h.errorRate < 3 ? '#22c55e' : h.errorRate < 8 ? '#f59e0b' : '#ef4444' }}>{h.errorRate.toFixed(2)}%</td>
                    <td>
                      {h.alertCount > 0
                        ? <span className={`severity-pill ${(h.topSeverity ?? '').toLowerCase()}`}>{h.alertCount} {h.topSeverity}</span>
                        : h.deliveryRate < 10
                          ? <span className="severity-pill critical">Critical</span>
                          : h.deliveryRate < 85
                            ? <span className="severity-pill high">High</span>
                            : <span className="no-alerts">&#10003; Clear</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {selectedCampaignId && (() => {
            const h = campaignHealth.find((x) => x.campaign.campaign_id === selectedCampaignId)
            if (!h) return null
            const chartData = h.recentRows.map((r) => ({
              hour: r.log_hour.slice(11, 16),
              delivered: r.impressions_delivered,
              requests: r.vast_requests,
            }))
            const campAlerts = enrichedAlerts.filter((a) => a.campaign_id === selectedCampaignId)
            return (
              <aside className="health-detail">
                <div className="hd-head">
                  <div>
                    <strong>{h.campaign.campaign_name}</strong>
                    <span>{h.advertiser_name} · {h.platform_name}</span>
                  </div>
                  <button type="button" onClick={() => setSelectedCampaignId(null)}><X size={16} /></button>
                </div>
                <div className="hd-meta">
                  <span>CPM: <strong>{currencyFmt.format(h.campaign.cpm_usd)}</strong></span>
                  <span>Target: <strong>{compactFmt.format(h.campaign.target_impressions)} imps</strong></span>
                  <span>{h.campaign.start_date} → {h.campaign.end_date}</span>
                </div>
                <div className="hd-kpis">
                  {[
                    { label: 'Delivery', val: `${h.deliveryRate.toFixed(1)}%`, color: healthColor(h.deliveryRate) },
                    { label: 'Fill',     val: `${h.fillRate.toFixed(1)}%`,     color: h.fillRate >= 95 ? '#22c55e' : '#f59e0b' },
                    { label: 'VCR',      val: `${h.vcr.toFixed(1)}%`,          color: h.vcr >= 70 ? '#22c55e' : '#f59e0b' },
                    { label: 'Errors',   val: `${h.errorRate.toFixed(2)}%`,    color: h.errorRate < 3 ? '#22c55e' : '#ef4444' },
                  ].map((k) => (
                    <div className="hd-kpi" key={k.label}>
                      <span style={{ color: k.color }}>{k.val}</span>
                      <label>{k.label}</label>
                    </div>
                  ))}
                </div>
                <h4>Hourly Delivery (recent)</h4>
                <ResponsiveContainer width="100%" height={160}>
                  <AreaChart data={chartData} margin={{ top: 4, right: 4, left: -20, bottom: 0 }}>
                    <defs>
                      <linearGradient id="hdFill" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%"  stopColor="#FF5800" stopOpacity={0.5} />
                        <stop offset="95%" stopColor="#FF5800" stopOpacity={0.03} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid stroke="#e5e7eb" strokeDasharray="3 3" />
                    <XAxis dataKey="hour" stroke="#9EA3B0" tick={{ fontSize: 9, fill: '#555b6e' }} />
                    <YAxis stroke="#5A5F6E" tickFormatter={(v) => compactFmt.format(v)} tick={{ fontSize: 9 }} />
                    <Tooltip contentStyle={{ background: '#ffffff', border: '1px solid rgba(255,88,0,0.25)', borderRadius: 8, fontSize: 11, boxShadow: '0 4px 16px rgba(0,0,0,0.08)', color: '#1a1a1a' }}
                      formatter={(v) => numberFmt.format(Number(v ?? 0))} />
                    <Area type="monotone" dataKey="delivered" stroke="#FF5800" fill="url(#hdFill)" name="Delivered" />
                  </AreaChart>
                </ResponsiveContainer>
                {campAlerts.length > 0 && (
                  <>
                    <h4>Active Alerts ({campAlerts.length})</h4>
                    <div className="hd-alerts">
                      {campAlerts.map((a) => (
                        <div className="hd-alert-row" key={a.alert_id}>
                          <span className={`severity-pill ${a.severity.toLowerCase()}`}>{a.severity}</span>
                          <div>
                            <strong>{a.alert_type}</strong>
                            <p>{a.trigger_value}</p>
                          </div>
                          <span className="hd-revenue">{currencyFmt.format(Number(a.revenue_impact_usd))}</span>
                        </div>
                      ))}
                    </div>
                  </>
                )}

                {/* AI Agent Analysis Section */}
                <div className="hd-agent-section">
                  {(() => {
                    const cAgentState = campaignAgentState[selectedCampaignId!]
                    const cAgentStep  = campaignAgentStep[selectedCampaignId!] ?? 0
                    const rca = getCampaignRca(h)

                    if (!cAgentState || cAgentState === 'idle') {
                      return (
                        <button
                          className="hd-agent-trigger"
                          type="button"
                          onClick={() => triggerCampaignAnalysis(selectedCampaignId!)}
                        >
                          <Bot size={14} /> Analyze with AI Agent
                        </button>
                      )
                    }

                    if (cAgentState === 'running') {
                      return (
                        <div className="hd-agent-working">
                          <div className="hd-agent-working-head">
                            <div className="agent-spinner" style={{ width: 20, height: 20, flexShrink: 0 }}><Bot size={11} /></div>
                            <span>AI agent analyzing campaign…</span>
                          </div>
                          <div className="hd-agent-steps">
                            {CAMPAIGN_HEALTH_STEPS.map((step, i) => (
                              <div key={step.tool} className={`hd-agent-step ${i < cAgentStep ? 'done' : i === cAgentStep ? 'active' : 'pending'}`}>
                                <div className="hd-step-icon">
                                  {step.icon === 'search'   && <Search   size={10} />}
                                  {step.icon === 'database' && <Database size={10} />}
                                  {step.icon === 'terminal' && <Terminal size={10} />}
                                  {step.icon === 'activity' && <Activity size={10} />}
                                  {step.icon === 'zap'      && <Zap      size={10} />}
                                </div>
                                <span>{step.desc}</span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )
                    }

                    // Complete
                    return (
                      <div className="hd-agent-complete fade-in-section">
                        <div className="hd-rca-box">
                          <div className="hd-rca-label"><Bot size={11} /> Root Cause</div>
                          <p>{rca.rootCause}</p>
                        </div>
                        <div className="hd-rec-box">
                          <div className="hd-rec-label"><Zap size={11} /> Recommendation</div>
                          <p>{rca.recommendation}</p>
                        </div>
                        <button
                          type="button"
                          style={{ marginTop: 4, padding: '5px 10px', border: '1px solid rgba(0,0,0,0.1)', borderRadius: 6, background: 'transparent', fontSize: '0.72rem', color: 'var(--text-muted)', cursor: 'pointer', fontFamily: 'var(--font-sans)' }}
                          onClick={() => { setCampaignAgentState(p => ({ ...p, [selectedCampaignId!]: 'idle' })); setCampaignAgentStep(p => ({ ...p, [selectedCampaignId!]: 0 })) }}
                        >
                          Re-run analysis
                        </button>
                      </div>
                    )
                  })()}
                </div>
              </aside>
            )
          })()}
        </div>
      </main>
      )}

      {/* ════════════════════════════════════════════════════════════════
          INTELLIGENCE FEED TAB
      ════════════════════════════════════════════════════════════════ */}
      {activeTab === 'intel' && (
      <main key="intel" className="intel-shell tab-content">

        {/* Board header */}
        <div className="board-header">
          <div className="board-header-left">
            <Sparkles size={16} className="board-header-icon" />
            <div>
              <h2 className="board-title">Strategic Intelligence Board</h2>
              <p className="board-sub">AI-curated delivery analytics, engagement trends &amp; spend efficiency — last 24 hours</p>
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <div className="board-refresh-badge"><Activity size={11} /> Live · auto-refreshes</div>
            <button
              type="button"
              className="board-pdf-btn"
              onClick={() => window.print()}
            >
              <Database size={12} /> Export PDF
            </button>
          </div>
        </div>

        {/* Platform tabs */}
        <div className="board-platform-tabs">
          {intelPlatformNames.map(name => (
            <button
              key={name}
              type="button"
              className={`board-plt-tab ${intelPlatform === name ? 'active' : ''}`}
              onClick={() => setIntelPlatform(name)}
            >
              {name}
            </button>
          ))}
        </div>

        {/* AI Platform Summary */}
        <div className="board-ai-summary">
          <div className="board-ai-icon"><Sparkles size={13} /></div>
          <p>{intelPlatformSummary}</p>
        </div>

        {/* KPI strip — platform-scoped */}
        <div className="board-kpi-strip">
          {[
            {
              label: 'Avg Fill Rate',
              value: `${intelPlatformKpis.fillRate.toFixed(1)}%`,
              sub: intelPlatformKpis.fillRate >= 85 ? 'On target (≥85%)' : 'Below target',
              status: intelPlatformKpis.fillRate >= 85 ? 'good' : 'warn',
              icon: <TrendingUp size={14} />,
            },
            {
              label: 'Video Completion Rate',
              value: `${intelPlatformKpis.vcr.toFixed(1)}%`,
              sub: intelPlatformKpis.vcr >= 70 ? 'Healthy engagement' : 'Needs attention',
              status: intelPlatformKpis.vcr >= 70 ? 'good' : 'warn',
              icon: <MonitorPlay size={14} />,
            },
            {
              label: 'VAST SLA Compliance',
              value: `${intelPlatformKpis.slaComp.toFixed(1)}%`,
              sub: 'Latency < 1800ms threshold',
              status: intelPlatformKpis.slaComp >= 90 ? 'good' : intelPlatformKpis.slaComp >= 70 ? 'warn' : 'bad',
              icon: <Zap size={14} />,
            },
            {
              label: 'Revenue at Risk',
              value: currencyFmt.format(intelPlatformKpis.atRisk),
              sub: `${intelPlatformKpis.alertCount} open alert${intelPlatformKpis.alertCount !== 1 ? 's' : ''}`,
              status: intelPlatformKpis.atRisk === 0 ? 'good' : intelPlatformKpis.atRisk < 50000 ? 'warn' : 'bad',
              icon: <CircleDollarSign size={14} />,
            },
          ].map((kpi) => (
            <div key={kpi.label} className={`board-kpi-tile board-kpi-${kpi.status}`}>
              <div className="board-kpi-top">{kpi.icon}<span className="board-kpi-label">{kpi.label}</span></div>
              <div className="board-kpi-value">{kpi.value}</div>
              <div className="board-kpi-sub">{kpi.sub}</div>
            </div>
          ))}
        </div>

        {/* Charts row */}
        <div className="board-charts-row">
          {/* Fill rate + VCR over 24h */}
          <div className="board-chart-card board-chart-wide">
            <div className="board-chart-head">
              <span className="board-chart-title"><Activity size={13} /> Fill Rate &amp; Completion Rate — 24h Trend</span>
              <div className="board-legend">
                <span className="legend-dot" style={{ background: '#FF5800' }} /> Fill Rate
                <span className="legend-dot" style={{ background: '#22c55e' }} /> VCR
              </div>
            </div>
            <ResponsiveContainer width="100%" height={200}>
              <AreaChart data={intelPlatformEngagement} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
                <defs>
                  <linearGradient id="gradFill" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%"  stopColor="#FF5800" stopOpacity={0.18} />
                    <stop offset="95%" stopColor="#FF5800" stopOpacity={0} />
                  </linearGradient>
                  <linearGradient id="gradVcr" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%"  stopColor="#22c55e" stopOpacity={0.18} />
                    <stop offset="95%" stopColor="#22c55e" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(0,0,0,0.06)" />
                <XAxis dataKey="hour" tick={{ fontSize: 10, fill: '#9EA3B0' }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
                <YAxis tick={{ fontSize: 10, fill: '#9EA3B0' }} tickLine={false} axisLine={false} domain={[0, 100]} unit="%" />
                <Tooltip
                  contentStyle={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 8, fontSize: 12 }}
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  formatter={((val: number | string, name: string) => [`${Number(val).toFixed(1)}%`, name === 'fillRate' ? 'Fill Rate' : 'VCR']) as any}
                />
                <Area type="monotone" dataKey="fillRate" stroke="#FF5800" strokeWidth={2} fill="url(#gradFill)" dot={false} />
                <Area type="monotone" dataKey="vcr"      stroke="#22c55e" strokeWidth={2} fill="url(#gradVcr)"  dot={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          {/* Platform fill rate comparison */}
          <div className="board-chart-card board-chart-narrow">
            <div className="board-chart-head">
              <span className="board-chart-title"><MonitorPlay size={13} /> Platform Fill Rate</span>
            </div>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={platformPerformance} layout="vertical" margin={{ top: 4, right: 20, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(0,0,0,0.06)" horizontal={false} />
                <XAxis type="number" tick={{ fontSize: 10, fill: '#9EA3B0' }} tickLine={false} axisLine={false} unit="%" domain={[0, 100]} />
                <YAxis type="category" dataKey="name" tick={{ fontSize: 10, fill: '#9EA3B0' }} tickLine={false} axisLine={false} width={80} />
                <Tooltip
                  contentStyle={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 8, fontSize: 12 }}
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  formatter={((val: number | string) => [`${Number(val).toFixed(1)}%`, 'Fill Rate']) as any}
                />
                <Bar dataKey="fillRate" radius={[0, 4, 4, 0]}>
                  {platformPerformance.map((_, i) => (
                    <Cell key={i} fill={i === 0 ? '#FF5800' : i === 1 ? '#FF8C42' : '#FFB347'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* AI Insight cards — platform scoped */}
        <div className="board-insights-row">
          {[
            {
              icon: <TrendingUp size={15} />,
              title: 'Delivery Momentum',
              color: '#FF5800',
              body: (() => {
                const avgFill = intelPlatformKpis.fillRate
                const trend   = intelPlatformEngagement.length >= 4
                  ? intelPlatformEngagement.slice(-4).reduce((s, r) => s + r.fillRate, 0) / 4
                  : avgFill
                const dir = trend > avgFill ? 'improving' : trend < avgFill - 3 ? 'declining' : 'stable'
                return `Fill rate is ${dir} over the last 4 hours (${trend.toFixed(1)}% vs ${avgFill.toFixed(1)}% 24h avg). ${avgFill >= 85 ? 'Delivery is on pace with no action required.' : 'Below the 85% target — consider inventory reallocation or frequency cap adjustment.'}`
              })(),
              stat: `${intelPlatformKpis.fillRate.toFixed(1)}% fill`,
            },
            {
              icon: <MonitorPlay size={15} />,
              title: 'Engagement Health',
              color: '#22c55e',
              body: (() => {
                const vcr = intelPlatformKpis.vcr
                const top = intelFilteredHealth.filter(c => c.vcr > 0).sort((a,b) => b.vcr - a.vcr)[0]
                if (!top) return 'Insufficient VCR data for this period.'
                return `Video completion rate is ${vcr.toFixed(1)}%${intelPlatform !== 'All' ? ` on ${intelPlatform}` : ''}. Top performer is ${top.campaign.campaign_name} at ${top.vcr.toFixed(1)}% VCR. ${vcr >= 70 ? 'Audience engagement is healthy — creatives are resonating.' : 'VCR is below benchmark (70%). Review creative length and targeting to improve completions.'}`
              })(),
              stat: `${intelPlatformKpis.vcr.toFixed(1)}% VCR`,
            },
            {
              icon: <CircleDollarSign size={15} />,
              title: 'Revenue Exposure',
              color: '#6366f1',
              body: (() => {
                const risk = intelPlatformKpis.atRisk
                const cnt  = intelPlatformKpis.alertCount
                const scope = intelPlatform === 'All' ? 'across the network' : `on ${intelPlatform}`
                return `${currencyFmt.format(risk)} is at risk ${scope} from ${cnt} open alert${cnt !== 1 ? 's' : ''}. ${risk === 0 ? 'No revenue exposure — all campaigns healthy.' : risk < 50000 ? 'Exposure is contained. Monitor active alerts to prevent escalation.' : 'Significant exposure detected. Prioritize resolving Critical and High severity alerts first to protect committed revenue.'}`
              })(),
              stat: currencyFmt.format(intelPlatformKpis.atRisk),
            },
          ].map((card) => (
            <div key={card.title} className="board-insight-card">
              <div className="board-insight-head" style={{ color: card.color }}>
                {card.icon}
                <strong>{card.title}</strong>
                <span className="board-insight-stat" style={{ background: `${card.color}18`, color: card.color }}>{card.stat}</span>
              </div>
              <p className="board-insight-body">{card.body}</p>
            </div>
          ))}
        </div>

        {/* Top campaigns by VCR table */}
        <div className="board-table-card">
          <div className="board-chart-head">
            <span className="board-chart-title"><Table2 size={13} /> Top Campaigns by Video Completion Rate</span>
            <span className="board-table-sub">Ranked by VCR · {intelPlatform === 'All' ? 'All platforms' : intelPlatform}</span>
          </div>
          <table className="board-perf-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Campaign</th>
                <th>Platform</th>
                <th>Fill Rate</th>
                <th>VCR</th>
                <th>Delivery</th>
                <th>Error Rate</th>
              </tr>
            </thead>
            <tbody>
              {intelFilteredHealth.filter(c => c.vcr > 0).sort((a,b) => b.vcr - a.vcr).slice(0, 8).map((row, i) => (
                <tr key={row.campaign.campaign_id}>
                  <td className="board-rank">{i + 1}</td>
                  <td>
                    <div className="board-camp-name">{row.campaign.campaign_name}</div>
                    <div className="board-camp-adv">{row.advertiser_name}</div>
                  </td>
                  <td><span className="board-platform-tag">{row.platform_name}</span></td>
                  <td>
                    <div className="board-meter-wrap">
                      <div className="board-meter-bar" style={{ width: `${Math.min(row.fillRate, 100)}%`, background: row.fillRate >= 85 ? '#22c55e' : row.fillRate >= 70 ? '#FF8C42' : '#ef4444' }} />
                      <span>{row.fillRate.toFixed(1)}%</span>
                    </div>
                  </td>
                  <td>
                    <div className="board-meter-wrap">
                      <div className="board-meter-bar" style={{ width: `${Math.min(row.vcr, 100)}%`, background: '#6366f1' }} />
                      <span>{row.vcr.toFixed(1)}%</span>
                    </div>
                  </td>
                  <td>
                    <span className={`board-rate-chip ${row.deliveryRate >= 90 ? 'good' : row.deliveryRate >= 70 ? 'warn' : 'bad'}`}>
                      {row.deliveryRate.toFixed(0)}%
                    </span>
                  </td>
                  <td className="board-error-cell">
                    {row.errorRate > 2
                      ? <span className="board-rate-chip bad">{row.errorRate.toFixed(1)}%</span>
                      : <span className="board-rate-chip good">{row.errorRate.toFixed(1)}%</span>
                    }
                  </td>
                </tr>
              ))}
              {intelFilteredHealth.filter(c => c.vcr > 0).length === 0 && (
                <tr><td colSpan={7} className="board-empty-row">No active campaign performance data for {intelPlatform}</td></tr>
              )}
            </tbody>
          </table>
        </div>

      </main>
      )}

      {/* ════════════════════════════════════════════════════════════════
          DATA EXPLORER TAB
      ════════════════════════════════════════════════════════════════ */}
      {activeTab === 'explorer' && (
      <main key="explorer" className="explorer-shell tab-content">

        {/* Explorer header with PDF download */}
        <div className="ex-toolbar">
          <div className="ex-toolbar-left">
            <Database size={15} />
            <span>Data Explorer</span>
            <span className="ex-toolbar-sub">Schema · ER Diagram · KPI Formulas</span>
          </div>
          <button type="button" className="board-pdf-btn" onClick={() => window.print()}>
            <Database size={12} /> Export PDF
          </button>
        </div>

        <section className="ex-section">
          <div className="ex-section-head"><Table2 size={17} /><h2>Data Catalog</h2><span className="ex-badge">9 tables</span></div>
          <div className="ex-table-grid">
            {TABLE_CATALOG.map((t) => (
              <div className="ex-table-card" key={t.name} style={{ borderColor: t.color }}>
                <div className="ex-table-header" style={{ background: `${t.color}18` }}>
                  <span className="ex-table-name" style={{ color: t.color }}>{t.name}</span>
                  <span className="ex-file-badge">{t.file}</span>
                </div>
                <p className="ex-table-desc">{t.description}</p>
                <div className="ex-col-list">
                  {t.columns.map((col) => (
                    <div className="ex-col-row" key={col.name}>
                      <div className="ex-col-flags">
                        {col.pk && <span className="flag pk">PK</span>}
                        {col.fk && <span className="flag fk">FK</span>}
                      </div>
                      <span className="ex-col-name">{col.name}</span>
                      <span className="ex-col-type">{col.type}</span>
                    </div>
                  ))}
                </div>
                <div className="ex-card-footer">
                  <button
                    className="ex-btn-preview"
                    type="button"
                    onClick={() => setPreviewTable(t.name)}
                  >
                    <Search size={12} /> Preview
                  </button>
                  <button
                    className="ex-btn-download"
                    type="button"
                    onClick={() => downloadCsv(t.file)}
                  >
                    <Database size={12} /> Download CSV
                  </button>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section className="ex-section">
          <div className="ex-section-head"><Database size={17} /><h2>Entity Relationship Diagram</h2><span className="ex-badge">FK → PK · dashed = standalone</span></div>
          <div className="ex-er-wrap">
            <svg viewBox="0 0 920 500" className="ex-er-svg">
              <defs>
                <marker id="fk-arrow" markerWidth="7" markerHeight="7" refX="6" refY="3" orient="auto">
                  <path d="M0,0 L0,6 L7,3 z" fill="rgba(255,88,0,0.6)" />
                </marker>
              </defs>
              <g stroke="rgba(255,88,0,0.3)" strokeWidth="1.5" fill="none" markerEnd="url(#fk-arrow)">
                {ER_LINES.map((l, i) => <path key={i} d={l.d} />)}
              </g>
              <g fill="#9EA3B0" fontSize="9" fontFamily="'JetBrains Mono','SFMono-Regular',monospace">
                {ER_LINES.map((l, i) => <text key={i} x={l.lx} y={l.ly}>{l.label}</text>)}
              </g>
              {ER_NODES.map(({ name, cx, cy, color, cols, standalone }) => {
                const w = 185, headerH = 28, rowH = 18
                const h = headerH + cols.length * rowH + 8
                const x = cx - w / 2, y = cy - h / 2
                return (
                  <g key={name}>
                    <rect x={x} y={y} width={w} height={h} rx={7} fill="#ffffff" stroke={color} strokeWidth={1.5} strokeDasharray={standalone ? '5 3' : undefined} />
                    <rect x={x} y={y} width={w} height={headerH} rx={7} fill={color + '18'} />
                    <rect x={x} y={y + headerH - 2} width={w} height={2} fill={color + '33'} />
                    <text x={cx} y={y + 17} textAnchor="middle" fill={color} fontSize={11} fontWeight="bold" fontFamily="'JetBrains Mono','SFMono-Regular',monospace">{name}</text>
                    {cols.map(([flag, colName], i) => {
                      const rowY = y + headerH + i * rowH + 13
                      return (
                        <g key={colName}>
                          {flag === 'PK' && <text x={x + 6} y={rowY} fill="#d97706" fontSize={8} fontWeight="bold">PK</text>}
                          {flag === 'FK' && <text x={x + 6} y={rowY} fill="#FF5800" fontSize={8} fontWeight="bold">FK</text>}
                          <text x={flag ? x + 22 : x + 8} y={rowY}
                            fill={flag === 'PK' ? '#92400e' : flag === 'FK' ? '#c2440c' : '#9EA3B0'}
                            fontSize={10} fontFamily="'JetBrains Mono','SFMono-Regular',monospace">{colName}</text>
                        </g>
                      )
                    })}
                  </g>
                )
              })}
            </svg>
          </div>
        </section>

        <section className="ex-section">
          <div className="ex-section-head"><TrendingUp size={17} /><h2>KPI Formulas</h2><span className="ex-badge">{KPI_FORMULAS.length} metrics</span></div>
          <div className="kpi-grid">
            {KPI_FORMULAS.map((k) => (
              <div className="kpi-card" key={k.name}>
                <div className="kpi-card-head"><strong>{k.name}</strong><span className="kpi-source-tag">{k.source}</span></div>
                <code className="kpi-formula">{k.formula}</code>
                <p className="kpi-desc">{k.description}</p>
                <div className="kpi-thresholds">
                  {k.good !== '—' && <span className="thr-good">&#10003; {k.good}</span>}
                  {k.bad  !== '—' && <span className="thr-bad">&#10007; {k.bad}</span>}
                </div>
              </div>
            ))}
          </div>
        </section>
      </main>
      )}

      {/* ════════════════════════════════════════════════════════════════
          OVERVIEW DRILL-DOWN MODAL
      ════════════════════════════════════════════════════════════════ */}
      {drilldown && (
        <div className="modal-backdrop" onClick={() => setDrilldown(null)}>
          <div className="modal-box" onClick={(e) => e.stopPropagation()}>
            <div className="modal-head">
              <h3>
                {drilldown === 'campaigns' && 'Active Campaigns — Breakdown'}
                {drilldown === 'delivery'  && 'Delivery Rate — Under-Performing Campaigns'}
                {drilldown === 'alerts'    && 'Active Alerts — By Type & Severity'}
                {drilldown === 'revenue'   && 'Revenue at Risk — Top Impacted Campaigns'}
                {drilldown === 'pacing'    && 'Network Delivery Pacing — Expanded'}
                {drilldown === 'fallout'   && 'Fallout Reasons — Detailed Breakdown'}
              </h3>
              <button type="button" onClick={() => setDrilldown(null)}><X size={18} /></button>
            </div>

            {drilldown === 'campaigns' && (
              <div className="modal-body">
                <div className="dd-stat-row">
                  <div className="dd-stat"><span>{streamingCount}</span><label>Streaming</label></div>
                  <div className="dd-stat"><span>{linearCount}</span><label>Linear</label></div>
                  <div className="dd-stat"><span>{campaigns.filter((c) => c.status === 'Paused').length}</span><label>Paused</label></div>
                  <div className="dd-stat"><span>{campaigns.filter((c) => c.status === 'Completed').length}</span><label>Completed</label></div>
                </div>
                <h4>Active Campaigns by Advertiser Tier</h4>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={tierBreakdown} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
                    <CartesianGrid stroke="#e5e7eb" strokeDasharray="3 3" />
                    <XAxis dataKey="name" stroke="#9EA3B0" tick={{ fontSize: 12, fill: '#555b6e' }} />
                    <YAxis stroke="#9EA3B0" tick={{ fontSize: 12, fill: '#555b6e' }} />
                    <Tooltip contentStyle={{ background: '#ffffff', border: '1px solid rgba(255,88,0,0.25)', borderRadius: 10, boxShadow: '0 4px 16px rgba(0,0,0,0.08)', color: '#1a1a1a' }} />
                    <Bar dataKey="value" name="Campaigns" radius={[5, 5, 0, 0]}>
                      {tierBreakdown.map((_, i) => <Cell key={i} fill={['#fbbf24', '#9ca3af', '#cd7c3a'][i]} />)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
                <h4>Platform Breakdown</h4>
                <ResponsiveContainer width="100%" height={180}>
                  <BarChart layout="vertical"
                    data={[...new Map(activeByCampaigns.map((c) => [platformMap[c.platform_id]?.platform_name ?? '?', 0])).keys()]
                      .map((name) => ({ name, count: activeByCampaigns.filter((c) => platformMap[c.platform_id]?.platform_name === name).length }))
                      .sort((a, b) => b.count - a.count).slice(0, 8)}
                    margin={{ top: 4, right: 40, left: 90, bottom: 0 }}>
                    <CartesianGrid stroke="#e5e7eb" strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" stroke="#9EA3B0" tick={{ fontSize: 11, fill: '#555b6e' }} />
                    <YAxis type="category" dataKey="name" stroke="#9EA3B0" tick={{ fontSize: 11, fill: '#555b6e' }} width={85} />
                    <Tooltip contentStyle={{ background: '#ffffff', border: '1px solid rgba(255,88,0,0.25)', borderRadius: 10, boxShadow: '0 4px 16px rgba(0,0,0,0.08)', color: '#1a1a1a' }} />
                    <Bar dataKey="count" fill="#FF5800" radius={[0, 4, 4, 0]} name="Campaigns" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}

            {drilldown === 'delivery' && (
              <div className="modal-body">
                <p className="modal-sub">{worstDelivery.length} campaigns with delivery below 100% · {worstDelivery.filter(h => h.deliveryRate === 0).length} at zero delivery</p>
                <table className="dd-table">
                  <thead><tr><th>Campaign</th><th>Platform</th><th>Delivery %</th><th>Gap</th><th>Revenue at Risk</th><th>Alerts</th></tr></thead>
                  <tbody>
                    {worstDelivery.map((h) => (
                      <tr key={h.campaign.campaign_id}>
                        <td>
                          <strong>{h.campaign.campaign_name}</strong>
                          <br /><code>{h.campaign.campaign_id}</code>
                          {h.deliveryRate === 0 && (
                            <span style={{ marginLeft: 6, fontSize: '0.65rem', background: 'rgba(239,68,68,0.12)', color: '#ef4444', padding: '1px 6px', borderRadius: 999, border: '1px solid rgba(239,68,68,0.3)', fontWeight: 700 }}>Zero Delivery</span>
                          )}
                        </td>
                        <td>{h.platform_name}</td>
                        <td style={{ color: healthColor(h.deliveryRate) }}><strong>{h.deliveryRate.toFixed(1)}%</strong></td>
                        <td style={{ color: '#ef4444' }}>{(100 - h.deliveryRate).toFixed(1)}% below</td>
                        <td style={{ color: '#ef4444', fontWeight: 600 }}>{h.revenueAtRisk > 0 ? currencyFmt.format(h.revenueAtRisk) : '—'}</td>
                        <td>{h.alertCount > 0 ? <span className={`severity-pill ${(h.topSeverity ?? '').toLowerCase()}`}>{h.topSeverity}</span> : '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {drilldown === 'alerts' && (
              <div className="modal-body">
                <div className="dd-stat-row">
                  {(['Critical', 'High', 'Medium', 'Warning'] as const).map((s) => {
                    const count = enrichedAlerts.filter((a) => a.severity === s).length
                    return <div className={`dd-stat sev-${s.toLowerCase()}`} key={s}><span>{count}</span><label>{s}</label></div>
                  })}
                </div>
                <h4>By Alert Type</h4>
                <table className="dd-table">
                  <thead><tr><th>Alert Type</th><th>Count</th><th>Revenue at Risk</th></tr></thead>
                  <tbody>
                    {alertTypeTotals.sort((a, b) => b.revenue - a.revenue).map((t) => (
                      <tr key={t.type}>
                        <td><strong>{t.type}</strong></td>
                        <td>{t.count}</td>
                        <td style={{ color: '#ef4444' }}>{currencyFmt.format(t.revenue)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {drilldown === 'revenue' && (
              <div className="modal-body">
                <p className="modal-sub">Top campaigns by estimated revenue impact</p>
                <table className="dd-table">
                  <thead><tr><th>Campaign</th><th>Alert Type</th><th>Severity</th><th>Revenue at Risk</th></tr></thead>
                  <tbody>
                    {topRevenueAlerts.map((a) => (
                      <tr key={a.alert_id}>
                        <td><strong>{a.campaign_name}</strong><br /><span style={{ color: '#5A5F6E', fontSize: '0.76rem' }}>{a.advertiser_name}</span></td>
                        <td>{a.alert_type}</td>
                        <td><span className={`severity-pill ${a.severity.toLowerCase()}`}>{a.severity}</span></td>
                        <td style={{ color: '#ef4444' }}><strong>{currencyFmt.format(Number(a.revenue_impact_usd))}</strong></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {drilldown === 'pacing' && (
              <div className="modal-body">
                <h4>Cumulative Delivery vs Target (Last 24h)</h4>
                <ResponsiveContainer width="100%" height={280}>
                  <AreaChart data={pacingSeries} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
                    <defs>
                      <linearGradient id="ddFill" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%"  stopColor="#FF5800" stopOpacity={0.5} />
                        <stop offset="95%" stopColor="#FF5800" stopOpacity={0.03} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid stroke="#e5e7eb" strokeDasharray="3 3" />
                    <XAxis dataKey="hour" stroke="#9EA3B0" tick={{ fontSize: 11, fill: '#555b6e' }} />
                    <YAxis stroke="#5A5F6E" tickFormatter={(v) => compactFmt.format(v)} tick={{ fontSize: 11 }} />
                    <Tooltip contentStyle={{ background: '#ffffff', border: '1px solid rgba(255,88,0,0.25)', borderRadius: 12, boxShadow: '0 4px 16px rgba(0,0,0,0.08)', color: '#1a1a1a' }}
                      formatter={(v) => numberFmt.format(Number(v ?? 0))} />
                    <Area type="monotone" dataKey="target" stroke="#5A5F6E" strokeDasharray="4 4" fill="none" name="Target" />
                    <Area type="monotone" dataKey="actual" stroke="#FF5800" fill="url(#ddFill)" name="Actual" />
                  </AreaChart>
                </ResponsiveContainer>
                <h4>Bottom 5 Under-Delivering Campaigns</h4>
                <table className="dd-table">
                  <thead><tr><th>Campaign</th><th>Platform</th><th>Delivery %</th></tr></thead>
                  <tbody>
                    {campaignHealth.slice(0, 5).map((h) => (
                      <tr key={h.campaign.campaign_id}>
                        <td>{h.campaign.campaign_name}</td>
                        <td>{h.platform_name}</td>
                        <td style={{ color: healthColor(h.deliveryRate) }}>{h.deliveryRate.toFixed(1)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {drilldown === 'fallout' && (
              <div className="modal-body">
                <h4>Fallout by Reason — Count &amp; Revenue Impact</h4>
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart layout="vertical"
                    data={alertTypeTotals.sort((a, b) => b.revenue - a.revenue)}
                    margin={{ top: 4, right: 80, left: 150, bottom: 0 }}>
                    <CartesianGrid stroke="#e5e7eb" strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" stroke="#5A5F6E" tickFormatter={(v) => compactFmt.format(v)} tick={{ fontSize: 10 }} />
                    <YAxis type="category" dataKey="type" stroke="#9EA3B0" tick={{ fontSize: 10, fill: '#555b6e' }} width={145} />
                    <Tooltip contentStyle={{ background: '#ffffff', border: '1px solid rgba(255,88,0,0.25)', borderRadius: 10, boxShadow: '0 4px 16px rgba(0,0,0,0.08)', color: '#1a1a1a' }}
                      formatter={(v, n) => n === 'revenue' ? currencyFmt.format(Number(v)) : v} />
                    <Bar dataKey="revenue" name="revenue" radius={[0, 4, 4, 0]} fill="#FF5800" />
                  </BarChart>
                </ResponsiveContainer>
                <table className="dd-table">
                  <thead><tr><th>Alert Type</th><th>Count</th><th>Revenue at Risk</th><th>Agent Fix Available</th></tr></thead>
                  <tbody>
                    {alertTypeTotals.sort((a, b) => b.revenue - a.revenue).map((t) => (
                      <tr key={t.type}>
                        <td><strong>{t.type}</strong></td>
                        <td>{t.count}</td>
                        <td style={{ color: '#ef4444' }}>{currencyFmt.format(t.revenue)}</td>
                        <td>{AGENT_PLAYBOOK[t.type] ? <span style={{ color: '#22c55e' }}>&#10003; Yes</span> : <span style={{ color: '#5A5F6E' }}>—</span>}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}

      {/* ════════════════════════════════════════════════════════════════
          DATA PREVIEW MODAL
      ════════════════════════════════════════════════════════════════ */}
      {previewTable && (() => {
        const previewData = getPreviewData(previewTable)
        const tableEntry = TABLE_CATALOG.find(t => t.name === previewTable)
        const columns = previewData.length > 0 ? Object.keys(previewData[0]) : []
        return (
          <div className="modal-backdrop" onClick={() => setPreviewTable(null)}>
            <div className="modal-box modal-box-wide" onClick={(e) => e.stopPropagation()}>
              <div className="modal-head">
                <h3>Preview: <code>{previewTable}</code></h3>
                <div className="modal-head-actions">
                  <button
                    type="button"
                    className="ex-btn-download"
                    onClick={() => tableEntry && downloadCsv(tableEntry.file)}
                  >
                    <Database size={13} /> Download CSV
                  </button>
                  <button type="button" onClick={() => setPreviewTable(null)}><X size={18} /></button>
                </div>
              </div>
              <div className="modal-body">
                {previewData.length > 0 ? (
                  <>
                    <p className="modal-sub">Showing first {previewData.length} rows</p>
                    <div className="preview-table-wrap">
                      <table className="preview-table">
                        <thead>
                          <tr>{columns.map(c => <th key={c}>{c}</th>)}</tr>
                        </thead>
                        <tbody>
                          {previewData.map((row, i) => (
                            <tr key={i}>
                              {columns.map(c => <td key={c}>{String(row[c] ?? '')}</td>)}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                ) : (
                  <div className="preview-empty">
                    <p>Data not loaded in memory — download to view</p>
                    {tableEntry && (
                      <button
                        type="button"
                        className="ex-btn-download"
                        onClick={() => downloadCsv(tableEntry.file)}
                      >
                        <Database size={13} /> Download CSV
                      </button>
                    )}
                  </div>
                )}
              </div>
            </div>
          </div>
        )
      })()}

      {/* ── RCA Side Panel (from overview alert click) ── */}
      {selectedAlert && (
        <aside className="rca-panel">
          <div className="rca-head">
            <h3>AI Root Cause Analysis</h3>
            <button type="button" onClick={() => setSelectedAlert(null)}><X size={18} /></button>
          </div>
          <div className="rca-section">
            <h4>Observed Symptom</h4>
            <p>{selectedAlert.trigger_value}</p>
          </div>
          <div className="rca-section">
            <h4>AI Analysis</h4>
            <p>{selectedAlert.alert_type} is causing delivery instability on {selectedAlert.platform_name}. The issue is tied to data-path failures between advertiser definitions and ad-serving eligibility checks.</p>
            <div className="confidence">Confidence Score: <strong>{buildRca(selectedAlert).confidence}%</strong></div>
          </div>
          <div className="rca-section">
            <h4>System Trace Logs</h4>
            <pre>{buildRca(selectedAlert).trace.join('\n')}</pre>
          </div>
          <div className="rca-section">
            <h4>Recommended Resolution</h4>
            <p>{buildRca(selectedAlert).recommendation}</p>
            <button className="autofix-btn" type="button"
              onClick={() => { setReviewDecisions((p) => ({ ...p, [selectedAlert.alert_id]: 'approved' })); setSelectedAlert(null); setActiveTab('notifications') }}>
              <Wrench size={15} /> Review Full Agent Analysis
            </button>
          </div>
        </aside>
      )}

      {/* ── CHAT WIDGET ── */}
      <div className={`chat-widget ${chatOpen ? 'open' : ''}`}>
        {chatOpen && (
          <div className="chat-box">
            <div className="chat-head">
              <div className="chat-head-left"><Bot size={14} /> <strong>Campaign Health Assistant</strong></div>
              <button type="button" onClick={() => setChatOpen(false)}><X size={15} /></button>
            </div>
            <div className="chat-messages">
              {chatMessages.map((m, i) => (
                <div key={i} className={`chat-msg ${m.role}`}>
                  {m.role === 'ai' && <div className="chat-msg-icon"><Bot size={11} /></div>}
                  <div className="chat-msg-text">{m.text}</div>
                </div>
              ))}
            </div>
            <form
              className="chat-input-row"
              onSubmit={(e) => {
                e.preventDefault()
                const q = chatInput.trim()
                if (!q) return
                const answer = answerChat(q)
                setChatMessages(prev => [...prev, { role: 'user', text: q }, { role: 'ai', text: answer }])
                setChatInput('')
              }}
            >
              <input
                className="chat-input"
                placeholder="Ask about delivery, alerts, platforms…"
                value={chatInput}
                onChange={(e) => setChatInput(e.target.value)}
              />
              <button type="submit" className="chat-send-btn"><Zap size={14} /></button>
            </form>
          </div>
        )}
        <button
          type="button"
          className="chat-fab"
          onClick={() => setChatOpen(o => !o)}
          title="Campaign Health Assistant"
        >
          {chatOpen ? <X size={20} /> : <Bot size={20} />}
        </button>
      </div>

    </div>
  )
}

export default App
