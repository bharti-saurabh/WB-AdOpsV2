import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import Papa from 'papaparse'
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, Cell,
  Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
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
  revenue_impact_usd: number; status: string
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

// ─── AGENT PLAYBOOK ─────────────────────────────────────────────────────────

const AGENT_PLAYBOOK: Record<string, {
  steps: { tool: string; icon: string; desc: string; result: string }[]
  rootCause: string; fixType: string; fixCode: string; recoveryTime: string
}> = {
  'Empty Audience Segment': {
    steps: [
      { tool: 'query_performance_log',    icon: 'search',   desc: 'Scanning delivery metrics (last 6h)',           result: 'impressions_delivered: 0 for 4 consecutive hours' },
      { tool: 'query_audience_segments',  icon: 'database', desc: 'Checking segment sync status: SEG-001',         result: 'sync_status: Failed — last sync 36h ago' },
      { tool: 'query_dmp_connector_logs', icon: 'terminal', desc: 'Fetching DMP connector error telemetry',        result: '404_SEGMENT_NOT_FOUND in MAX_IDENTITY_GRAPH' },
      { tool: 'correlate_campaigns',      icon: 'activity', desc: 'Identifying all campaigns sharing this segment', result: '3 campaigns affected — all delivery halted' },
      { tool: 'generate_resolution',      icon: 'zap',      desc: 'Generating fix artifact',                       result: 'Re-map segment + force sync + replay 4h window' },
    ],
    rootCause: 'DMP segment SEG-001 failed identity graph sync 36 hours ago. MAX_IDENTITY_GRAPH returned 404_SEGMENT_NOT_FOUND, causing 0 eligible users across all campaigns using this segment. Auto-retry exhausted after 5 attempts with no recovery.',
    fixType: 'API Call',
    fixCode: `# Step 1: Re-map segment to v2 identity graph
curl -X POST https://dmp.warnermedia.internal/v2/segments/remap \\
  -H "Authorization: Bearer $DMP_TOKEN" \\
  -d '{
    "segment_id": "SEG-001",
    "target_graph": "MAX_IDENTITY_GRAPH_V2",
    "force_sync": true,
    "notify_on_complete": "adops-oncall@warnermedia.com"
  }'

# Step 2: Replay queued impressions (4h window)
curl -X POST https://adserver.warnermedia.internal/v1/delivery/replay \\
  -H "Authorization: Bearer $ADSERVER_TOKEN" \\
  -d '{"filter":{"segment_id":"SEG-001"},"window_hours":4}'`,
    recoveryTime: '~25 min',
  },
  'DMP Segment Sync Failure': {
    steps: [
      { tool: 'query_dmp_connector',   icon: 'terminal', desc: 'Fetching DMP ingest batch logs',                result: '502_UPSTREAM_TIMEOUT — taxonomy mismatch detected' },
      { tool: 'query_segment_mapping', icon: 'database', desc: 'Checking external key taxonomy table',          result: '"Action_Enthusiasts" → unmapped in taxonomy v3.1' },
      { tool: 'query_eligibility',     icon: 'search',   desc: 'Checking campaign targeting eligibility',       result: '0 eligible impressions in queue' },
      { tool: 'query_snapshot',        icon: 'activity', desc: 'Finding last known-good sync snapshot',         result: 'Snapshot 2024-07-14 08:00 UTC available' },
      { tool: 'generate_resolution',   icon: 'zap',      desc: 'Generating config patch + re-ingest job',       result: 'Taxonomy patch + connector retry with fallback' },
    ],
    rootCause: 'DMP connector taxonomy mapping is mismatched — v3.1 vs expected v3.2. External key "Action_Enthusiasts" resolves to null in the current mapping table, causing 502 upstream timeouts during ingest batch processing. 5 retries exhausted.',
    fixType: 'Config Patch',
    fixCode: `# Patch taxonomy mapping
kubectl patch configmap dmp-taxonomy-mapping -n adops \\
  --patch '{"data":{"Action_Enthusiasts":"action_sports_v32","taxonomy_version":"3.2"}}'

# Retry ingest with fallback snapshot
kubectl create job --from=cronjob/dmp-ingest-batch dmp-ingest-retry-$(date +%s) \\
  -n adops -- \\
  --segment=SEG-001 \\
  --use-fallback-snapshot=2024-07-14T08:00:00Z \\
  --force-remap=true`,
    recoveryTime: '~18 min',
  },
  'VAST Timeout': {
    steps: [
      { tool: 'query_fill_rate',      icon: 'search',   desc: 'Analyzing VAST request/response ratios',       result: 'Fill rate: 94% → 61% over last 2h' },
      { tool: 'query_latency',        icon: 'activity', desc: 'Checking decision engine response times',       result: 'p99 latency: 2840ms  (SLA: 1800ms)' },
      { tool: 'query_cdn_health',     icon: 'terminal', desc: 'Inspecting CDN node health telemetry',          result: 'CDN node us-east-2b: 43% packet loss' },
      { tool: 'identify_fallback',    icon: 'database', desc: 'Locating backup ad decision endpoint',          result: 'adserver-2.warnermedia.internal — healthy' },
      { tool: 'generate_resolution',  icon: 'zap',      desc: 'Generating failover config patch',              result: 'Timeout reduction + CDN failover routing' },
    ],
    rootCause: 'CDN node us-east-2b is degraded (43% packet loss), pushing VAST response latency above the 1800ms SLA. Primary decision engine times out before returning valid ad pods, resulting in slate insertion across affected campaigns.',
    fixType: 'Config Update',
    fixCode: `cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: adserver-routing-config
  namespace: adops
data:
  vast_timeout_ms: "1800"
  primary_endpoint: "adserver-1.warnermedia.internal"
  failover_endpoint: "adserver-2.warnermedia.internal"
  auto_failover_threshold_pct: "20"
  cdn_health_check_interval: "30s"
EOF`,
    recoveryTime: '~8 min',
  },
  'Roku Transcode Risk': {
    steps: [
      { tool: 'query_creative_specs',  icon: 'search',   desc: 'Fetching creative bitrate profiles',           result: 'CMP creative: 18,500 kbps — Roku max: 15,000 kbps' },
      { tool: 'query_device_cohort',   icon: 'database', desc: 'Checking Roku household cohort size',           result: '~340K Roku devices targeted in this flight' },
      { tool: 'query_asset_store',     icon: 'terminal', desc: 'Checking transcoder pipeline for renditions',   result: 'No Roku-compatible rendition found in asset store' },
      { tool: 'query_similar_assets',  icon: 'activity', desc: 'Finding compliant creatives in same family',    result: 'Creative 1 (VOD 12,147 kbps) — compatible' },
      { tool: 'generate_resolution',   icon: 'zap',      desc: 'Generating transcode job + ingest rule',        result: 'Transcode to ≤15 Mbps + preflight enforcement' },
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
      { tool: 'query_pacing_metrics',    icon: 'search',   desc: 'Computing delivery gap vs pacing target',     result: 'Actual delivery 34% below target (last 6h)' },
      { tool: 'query_inventory_avails',  icon: 'database', desc: 'Checking available inventory on platform',    result: 'Prime-time sports break pods: 0 avails' },
      { tool: 'query_cross_platform',    icon: 'activity', desc: 'Checking MAX/TBS/CNN+ inventory',             result: 'MAX premium: 28% available · TBS: 41% available' },
      { tool: 'compute_budget_shift',    icon: 'terminal', desc: 'Calculating budget reallocation to recover',  result: 'Shift 12% budget → MAX + loosen freq cap by 1' },
      { tool: 'generate_resolution',     icon: 'zap',      desc: 'Generating trafficking system update',        result: 'Cross-platform reallocation + make-good schedule' },
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
      { tool: 'query_linear_schedule',  icon: 'search',   desc: 'Checking live event schedule for overruns',   result: 'NBA playoff overran 38 min — 6 ad pods consumed' },
      { tool: 'query_make_good_queue',  icon: 'database', desc: 'Checking make-good queue depth',               result: '42 spots queued · late-prime windows available' },
      { tool: 'query_streaming_avails', icon: 'activity', desc: 'Checking streaming inventory for spillover',   result: 'MAX: 65K+ CPM-compatible avails in late-prime' },
      { tool: 'compute_recovery',       icon: 'terminal', desc: 'Generating make-good schedule',                result: 'Distribute 42 spots: late-prime + MAX streaming' },
      { tool: 'generate_resolution',    icon: 'zap',      desc: 'Generating make-good + spillover config',      result: 'Automated insertion + streaming overflow routing' },
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
    'DMP segment failed to sync with the identity graph (last sync >36h ago)',
    'Audience cardinality near-zero due to over-restrictive targeting parameters',
    'External key mapping changed in the latest taxonomy update, breaking lookup',
  ],
  'DMP Segment Sync Failure': [
    'Upstream DMP connector returning 5xx errors during batch ingest',
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
  { name: 'audience_segments', file: 'audience_segments.csv', color: '#FF5800',
    description: 'DMP-sourced audience targeting segments used in campaign targeting.',
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
  { name: 'audience_segments', cx: 130, cy: 430, color: '#FF5800', cols: [['PK','segment_id'],['','segment_name'],['','provider'],['','sync_status']] },
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
    'Empty Audience Segment': 96, 'DMP Segment Sync Failure': 93,
    'Delivery Drop': 88, 'VAST Timeout': 91, 'Roku Transcode Risk': 90, 'Prime-Time Underdelivery': 84,
  }
  const traceMap: Record<string, string[]> = {
    'Empty Audience Segment': [
      '[SEGMENT-GRAPH] INFO  Segment lookup: SEG-001',
      '[SEGMENT-GRAPH] ERROR 404_SEGMENT_NOT_FOUND in MAX_IDENTITY_GRAPH',
      '[ELIGIBILITY]   WARN  0 eligible users for campaign targeting',
      '[DELIVERY]      CRIT  campaign halted — empty audience cardinality',
    ],
    'DMP Segment Sync Failure': [
      '[DMP-CONNECTOR] INFO  ingest batch started for SEG-001',
      '[DMP-CONNECTOR] ERROR 502_UPSTREAM_TIMEOUT resolving taxonomy IDs',
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
    'Empty Audience Segment': 'Re-map SEG-001 to MAX_IDENTITY_GRAPH_V2, force sync, and replay 4h impression queue.',
    'DMP Segment Sync Failure': 'Patch taxonomy mapping to v3.2 and re-run DMP connector with prior-day fallback snapshot.',
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
      fixes.push('Check DMP segment sync status; if targeting a specific audience, the segment may have failed to load. Expand targeting or remove constraints temporarily to restore delivery.')
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

  // Data explorer preview
  const [previewTable, setPreviewTable] = useState<string | null>(null)
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

  // ── Fallout donut
  const falloutSeries = useMemo(() => {
    if (!alerts.length) return []
    const maxT = alerts.reduce((l, a) => { const t = parseDate(a.alert_timestamp); return !l || t > l ? t : l }, null as Date | null)
    if (!maxT) return []
    const from = new Date(maxT.getTime() - 24 * 3600 * 1000)
    const m = new Map<string, number>()
    alerts.filter((a) => parseDate(a.alert_timestamp) >= from && a.status.toLowerCase() === 'open')
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

  // ── Agent sessions (top 6 alerts by revenue impact, one per alert_type)
  const agentSessions = useMemo(() => {
    const seen = new Set<string>()
    return enrichedAlerts
      .filter((a) => { if (seen.has(a.alert_type) || !AGENT_PLAYBOOK[a.alert_type]) return false; seen.add(a.alert_type); return true })
      .sort((a, b) => Number(b.revenue_impact_usd) - Number(a.revenue_impact_usd))
      .slice(0, 6)
  }, [enrichedAlerts])


  // ── Intelligence Board: hourly fill rate + completion rate over last 24h
  const hourlyEngagement = useMemo(() => {
    if (!last24Rows.length) return []
    const byHour = new Map<string, { req: number; resp: number; compl: number; del: number; err: number }>()
    for (const row of last24Rows) {
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
        fillRate:   r.req  > 0 ? Math.round((r.resp  / r.req) * 1000) / 10 : 0,
        vcr:        r.del  > 0 ? Math.round((r.compl / r.del) * 1000) / 10 : 0,
        errorRate:  r.req  > 0 ? Math.round((r.err   / r.req) * 1000) / 10 : 0,
      }
    })
  }, [last24Rows])

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

  // ── Intelligence Board: top campaigns by VCR for table
  const topCampaignsByVcr = useMemo(() => {
    return campaignHealth
      .filter((c) => c.vcr > 0)
      .sort((a, b) => b.vcr - a.vcr)
      .slice(0, 8)
  }, [campaignHealth])

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

  // Count-up animated values — must be before any early return (Rules of Hooks)
  const countCampaigns = useCountUp(loading ? 0 : topMetrics.activeCampaigns)
  const countAlerts    = useCountUp(loading ? 0 : topMetrics.activeAlerts)
  const countRevenue   = useCountUp(loading ? 0 : topMetrics.revenueAtRisk)

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
            <h2>{numberFmt.format(countCampaigns)}</h2>
            <p className="positive">Live across Streaming + Linear</p>
            <span className="drill-hint">Explore breakdown <ChevronRight size={11} /></span>
          </article>
          <article className="metric-card clickable" style={{ '--accent': '#FF8C42' } as React.CSSProperties} onClick={() => setDrilldown('delivery')}>
            <div className="metric-head">
              <span>Average Delivery Rate</span>
              <div className="metric-icon" style={{ background: 'rgba(255,140,66,0.10)', color: '#FF8C42' }}><MonitorPlay size={15} /></div>
            </div>
            <h2>{topMetrics.avgDeliveryRate.toFixed(1)}<span className="metric-unit">%</span></h2>
            <p className={topMetrics.avgDeliveryRate >= 90 ? 'positive' : 'negative'}>
              {topMetrics.avgDeliveryRate >= 90 ? 'Within pacing guardrails' : 'Below pacing target'}
            </p>
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
            <h2>{currencyFmt.format(countRevenue)}</h2>
            <p className="negative">Potentially lost from unresolved fallout</p>
            <span className="drill-hint">Explore breakdown <ChevronRight size={11} /></span>
          </article>
        </section>

        <section className="insight-banner">
          <Sparkles size={18} />
          <div className="insight-content">
            <strong>AI Network Forecast — Next 6 Hours</strong>
            <p>Cross-platform pacing analysis predicts <span className="insight-highlight">elevated underdelivery risk on linear live events</span>. Prime-time sports windows are forecasted near-zero avails. Recommend activating streaming spillover on {campaigns.filter(c => c.status === 'Active').length > 0 ? Math.min(3, Math.ceil(enrichedAlerts.length / 4)) : 2} campaigns to maintain pacing targets.</p>
          </div>
          <div className="insight-actions">
            <button type="button" className="lightweight-toggle" onClick={() => setIsLightweightMode((p) => !p)}>
              <span className="lt-label">{isLightweightMode ? '⚡ Fast Mode' : '📊 Full Data'}</span>
              <span className="lt-desc">{isLightweightMode ? 'Sampling 1 in 8 rows — faster load' : 'Loading all performance rows'}</span>
            </button>
          </div>
        </section>

        <section className="chart-card pacing-card clickable" onClick={() => setDrilldown('pacing')}>
          <div className="chart-title-row">
            <h3>Network Delivery Pacing (Last 24h)</h3>
            <span className="drill-hint-inline">Drill in <ChevronRight size={12} /></span>
          </div>
          <ResponsiveContainer width="100%" height={260}>
            <AreaChart data={pacingSeries} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="actualFill" x1="0" y1="0" x2="0" y2="1">
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
              <Area type="monotone" dataKey="actual" stroke="#FF5800" fill="url(#actualFill)" name="Actual" />
            </AreaChart>
          </ResponsiveContainer>
          <p className="chart-footnote">AI Insight: Delivery drops concentrated in linear prime-time windows; recovery expected after streaming spillover.</p>
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
          <div className="at-risk-list">
            {campaignHealth.filter(h => h.deliveryRate < 90).slice(0, 6).map(h => (
              <div key={h.campaign.campaign_id} className="at-risk-row">
                <div className="at-risk-meta">
                  <span className="at-risk-name">{h.campaign.campaign_name}</span>
                  <span className="at-risk-platform">{h.platform_name} · {h.advertiser_name}</span>
                </div>
                <div className="at-risk-bar-wrap">
                  <div className="at-risk-bar" style={{ width: `${Math.max(2, h.deliveryRate)}%`, background: h.deliveryRate < 20 ? '#ef4444' : h.deliveryRate < 75 ? '#f59e0b' : '#FF5800' }} />
                  <span style={{ color: h.deliveryRate < 20 ? '#ef4444' : h.deliveryRate < 75 ? '#f59e0b' : '#FF5800', fontSize: '0.78rem', fontWeight: 700 }}>{h.deliveryRate.toFixed(0)}%</span>
                </div>
                {h.alertCount > 0 && <span className={`severity-pill ${(h.topSeverity ?? '').toLowerCase()}`}>{h.topSeverity}</span>}
              </div>
            ))}
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
                              {step.icon === 'search'   && <Search   size={11} />}
                              {step.icon === 'database' && <Database size={11} />}
                              {step.icon === 'terminal' && <Terminal size={11} />}
                              {step.icon === 'activity' && <Activity size={11} />}
                              {step.icon === 'zap'      && <Zap      size={11} />}
                            </div>
                            <div className="asl-body">
                              <code>{step.tool}()</code>
                              <span>{step.desc}</span>
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

                  {/* Steps summary */}
                  <div className="nd-section fade-in-section" style={{ animationDelay: '0.1s' }}>
                    <div className="nd-label"><Bot size={13} /> Agent Steps Completed</div>
                    <div className="agent-steps compact">
                      {pb.steps.map((step, i) => (
                        <div className={`agent-step step-anim-${i}`} key={step.tool}>
                          <div className="step-icon"><CheckCircle2 size={11} /></div>
                          <div className="step-body">
                            <code className="step-tool">{step.tool}()</code>
                            <span className="step-result" style={{ color: 'var(--text-secondary)' }}>→ {step.result}</span>
                          </div>
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
            { label: 'Healthy (≥90%)',  count: campaignHealth.filter((h) => h.deliveryRate >= 90).length, color: '#22c55e' },
            { label: 'At Risk (75–89%)', count: campaignHealth.filter((h) => h.deliveryRate >= 75 && h.deliveryRate < 90).length, color: '#f59e0b' },
            { label: 'Critical (<75%)', count: campaignHealth.filter((h) => h.deliveryRate < 75).length, color: '#ef4444' },
            { label: 'With Open Alerts', count: campaignHealth.filter((h) => h.alertCount > 0).length, color: '#FF5800' },
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
                  <th>Campaign</th><th>Advertiser</th><th>Platform</th>
                  <th>Delivery %</th><th>Fill %</th><th>VCR %</th><th>Error %</th><th>Alerts</th>
                </tr>
              </thead>
              <tbody>
                {campaignHealth.map((h) => (
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
          <div className="board-refresh-badge"><Activity size={11} /> Live · auto-refreshes</div>
        </div>

        {/* KPI strip */}
        <div className="board-kpi-strip">
          {[
            {
              label: 'Avg Fill Rate',
              value: `${intelKpis.fillRate.toFixed(1)}%`,
              sub: intelKpis.fillRate >= 85 ? 'On target (≥85%)' : 'Below target',
              status: intelKpis.fillRate >= 85 ? 'good' : 'warn',
              icon: <TrendingUp size={14} />,
            },
            {
              label: 'Video Completion Rate',
              value: `${intelKpis.vcr.toFixed(1)}%`,
              sub: intelKpis.vcr >= 70 ? 'Healthy engagement' : 'Needs attention',
              status: intelKpis.vcr >= 70 ? 'good' : 'warn',
              icon: <MonitorPlay size={14} />,
            },
            {
              label: 'VAST SLA Compliance',
              value: `${intelKpis.slaComp.toFixed(1)}%`,
              sub: 'Latency < 1800ms threshold',
              status: intelKpis.slaComp >= 90 ? 'good' : intelKpis.slaComp >= 70 ? 'warn' : 'bad',
              icon: <Zap size={14} />,
            },
            {
              label: 'Revenue Efficiency',
              value: `${intelKpis.revEff.toFixed(1)}%`,
              sub: `${currencyFmt.format(intelKpis.atRisk)} at risk of ${currencyFmt.format(intelKpis.totalRev)}`,
              status: intelKpis.revEff >= 90 ? 'good' : intelKpis.revEff >= 75 ? 'warn' : 'bad',
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
              <AreaChart data={hourlyEngagement} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
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
                  formatter={(val: number, name: string) => [`${val.toFixed(1)}%`, name === 'fillRate' ? 'Fill Rate' : 'VCR']}
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
                  formatter={(val: number) => [`${val.toFixed(1)}%`, 'Fill Rate']}
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

        {/* AI Insight cards */}
        <div className="board-insights-row">
          {[
            {
              icon: <TrendingUp size={15} />,
              title: 'Delivery Momentum',
              color: '#FF5800',
              body: (() => {
                const avgFill = intelKpis.fillRate
                const trend   = hourlyEngagement.length >= 4
                  ? hourlyEngagement.slice(-4).reduce((s, r) => s + r.fillRate, 0) / 4
                  : avgFill
                const dir = trend > avgFill ? 'improving' : trend < avgFill - 3 ? 'declining' : 'stable'
                return `Fill rate is ${dir} over the last 4 hours (${trend.toFixed(1)}% vs ${avgFill.toFixed(1)}% 24h avg). ${avgFill >= 85 ? 'Delivery is on pace with no action required.' : 'Below the 85% target — consider inventory reallocation or frequency cap adjustment.'}`
              })(),
              stat: `${intelKpis.fillRate.toFixed(1)}% fill`,
            },
            {
              icon: <MonitorPlay size={15} />,
              title: 'Engagement Health',
              color: '#22c55e',
              body: (() => {
                const vcr = intelKpis.vcr
                const top = topCampaignsByVcr[0]
                if (!top) return 'Insufficient VCR data for this period.'
                return `Video completion rate is ${vcr.toFixed(1)}% across all active campaigns. Top performer is ${top.campaign.campaign_name} at ${top.vcr.toFixed(1)}% VCR. ${vcr >= 70 ? 'Audience engagement is healthy — creatives are resonating.' : 'VCR is below benchmark (70%). Review creative length and targeting to improve completions.'}`
              })(),
              stat: `${intelKpis.vcr.toFixed(1)}% VCR`,
            },
            {
              icon: <CircleDollarSign size={15} />,
              title: 'Spend Efficiency',
              color: '#6366f1',
              body: (() => {
                const eff  = intelKpis.revEff
                const risk = intelKpis.atRisk
                const total = intelKpis.totalRev
                return `${currencyFmt.format(risk)} of ${currencyFmt.format(total)} total committed revenue is at risk — a ${(100 - eff).toFixed(1)}% exposure. ${eff >= 90 ? 'Spend efficiency is strong. Monitor underperforming segments to maintain trajectory.' : 'Efficiency below 90% threshold. Prioritize reallocating budget away from underdelivering line items to protected inventory.'}`
              })(),
              stat: `${intelKpis.revEff.toFixed(1)}% efficient`,
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
            <span className="board-table-sub">Ranked by VCR · Active campaigns only</span>
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
              {topCampaignsByVcr.map((row, i) => (
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
              {topCampaignsByVcr.length === 0 && (
                <tr><td colSpan={7} className="board-empty-row">No active campaign performance data loaded</td></tr>
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

    </div>
  )
}

export default App
