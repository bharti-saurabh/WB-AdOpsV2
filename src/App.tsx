import { useEffect, useMemo, useState } from 'react'
import Papa from 'papaparse'
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, Cell,
  Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts'
import {
  Activity, AlertTriangle, Bell, Bot, CheckCircle2, ChevronDown,
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

const PIE_COLORS = ['#1bc7ff', '#6f84ff', '#ff5d96', '#ffc156', '#55e4a8']
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

// ─── TABLE CATALOG ──────────────────────────────────────────────────────────

const TABLE_CATALOG = [
  { name: 'advertisers', file: 'advertisers.csv', color: '#4472ff',
    description: 'Brand accounts purchasing ad inventory across Warner networks.',
    columns: [
      { name: 'advertiser_id', type: 'STRING', pk: true,  fk: false },
      { name: 'name',          type: 'STRING', pk: false, fk: false },
      { name: 'industry',      type: 'STRING', pk: false, fk: false },
      { name: 'tier',          type: 'STRING', pk: false, fk: false },
    ]},
  { name: 'agencies', file: 'agencies.csv', color: '#4472ff',
    description: 'Media buying agencies placing orders on behalf of advertisers.',
    columns: [
      { name: 'agency_id',     type: 'STRING', pk: true,  fk: false },
      { name: 'name',          type: 'STRING', pk: false, fk: false },
      { name: 'contact_email', type: 'STRING', pk: false, fk: false },
    ]},
  { name: 'platforms', file: 'platforms.csv', color: '#4472ff',
    description: 'Ad delivery platforms — Streaming (HBO Max, CNN+) and Linear (TNT, TBS, truTV).',
    columns: [
      { name: 'platform_id',   type: 'STRING', pk: true,  fk: false },
      { name: 'platform_name', type: 'STRING', pk: false, fk: false },
      { name: 'platform_type', type: 'STRING', pk: false, fk: false },
    ]},
  { name: 'audience_segments', file: 'audience_segments.csv', color: '#4472ff',
    description: 'DMP-sourced audience targeting segments used in campaign targeting.',
    columns: [
      { name: 'segment_id',   type: 'STRING', pk: true,  fk: false },
      { name: 'segment_name', type: 'STRING', pk: false, fk: false },
      { name: 'provider',     type: 'STRING', pk: false, fk: false },
      { name: 'sync_status',  type: 'STRING', pk: false, fk: false },
    ]},
  { name: 'campaigns', file: 'campaigns.csv', color: '#9333ea',
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
  { name: 'kpi_summary', file: 'kpi_summary.csv', color: '#06b6d4',
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
  { name: 'advertisers',       cx: 130, cy: 70,  color: '#4472ff', cols: [['PK','advertiser_id'],['','name'],['','industry'],['','tier']] },
  { name: 'agencies',          cx: 130, cy: 250, color: '#4472ff', cols: [['PK','agency_id'],['','name'],['','contact_email']] },
  { name: 'audience_segments', cx: 130, cy: 430, color: '#4472ff', cols: [['PK','segment_id'],['','segment_name'],['','provider'],['','sync_status']] },
  { name: 'kpi_summary',       cx: 460, cy: 70,  color: '#06b6d4', standalone: true, cols: [['','kpi_name'],['','value'],['','unit'],['','as_of']] },
  { name: 'campaigns',         cx: 460, cy: 255, color: '#9333ea', cols: [['PK','campaign_id'],['FK','advertiser_id'],['FK','agency_id'],['FK','platform_id'],['FK','segment_id'],['','status'],['','cpm_usd']] },
  { name: 'alerts',            cx: 460, cy: 440, color: '#ef4444', cols: [['PK','alert_id'],['FK','campaign_id'],['','severity'],['','alert_type'],['','revenue_impact_usd']] },
  { name: 'platforms',         cx: 790, cy: 70,  color: '#4472ff', cols: [['PK','platform_id'],['','platform_name'],['','platform_type']] },
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
  if (rate >= 90) return '#38d9b2'
  if (rate >= 75) return '#ffc156'
  return '#ff7a8a'
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

  if (loading) {
    return (
      <div className="loading-shell">
        <div className="loading-card"><MonitorPlay size={22} /><p>Building AdOps Intelligence Workspace...</p></div>
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
  const worstDelivery = campaignHealth.filter((h) => h.deliveryRate < 90).slice(0, 10)
  const alertTypeTotals = [...new Map(enrichedAlerts.map((a) => [a.alert_type, { type: a.alert_type, count: 0, revenue: 0 }])).values()]
  enrichedAlerts.forEach((a) => { const t = alertTypeTotals.find((x) => x.type === a.alert_type); if (t) { t.count++; t.revenue += Number(a.revenue_impact_usd) } })
  const topRevenueAlerts = [...enrichedAlerts].sort((a, b) => Number(b.revenue_impact_usd) - Number(a.revenue_impact_usd)).slice(0, 8)

  return (
    <div className="app-shell">

      {/* ── TOP NAV ── */}
      <header className="top-nav">
        <div className="brand">
          <div className="brand-mark">AO</div>
          <div><strong>AdOps Copilot</strong><span>Warner Network Control Plane</span></div>
        </div>
        <nav className="tabs">
          <button className={`tab ${activeTab === 'overview' ? 'active' : ''}`}      onClick={() => setActiveTab('overview')}><LayoutDashboard size={15} /> Overview</button>
          <button className={`tab ${activeTab === 'health' ? 'active' : ''}`}        onClick={() => setActiveTab('health')}><Activity size={15} /> Campaign Health</button>
          <button className={`tab ${activeTab === 'intel' ? 'active' : ''}`}         onClick={() => setActiveTab('intel')}><Bot size={15} /> Intelligence Feed</button>
          <button className={`tab ${activeTab === 'notifications' ? 'active' : ''}`} onClick={() => setActiveTab('notifications')}>
            <Bell size={15} /> Notifications
            {enrichedAlerts.filter((a) => !reviewDecisions[a.alert_id]).length > 0 && (
              <span className="notif-badge">{enrichedAlerts.filter((a) => !reviewDecisions[a.alert_id]).length}</span>
            )}
          </button>
          <button className={`tab ${activeTab === 'explorer' ? 'active' : ''}`}      onClick={() => setActiveTab('explorer')}><Database size={15} /> Data Explorer</button>
        </nav>
        <div className="status-pill">System Status: <strong>Degraded</strong></div>
      </header>

      {/* ════════════════════════════════════════════════════════════════
          OVERVIEW TAB
      ════════════════════════════════════════════════════════════════ */}
      {activeTab === 'overview' && (
      <main className="content-grid">
        <section className="metric-grid">
          <article className="metric-card clickable" onClick={() => setDrilldown('campaigns')}>
            <div className="metric-head"><span>Active Campaigns</span><TrendingUp size={16} /></div>
            <h2>{numberFmt.format(topMetrics.activeCampaigns)}</h2>
            <p className="positive">Live across Streaming + Linear</p>
            <span className="drill-hint">Click to explore <ChevronRight size={12} /></span>
          </article>
          <article className="metric-card clickable" onClick={() => setDrilldown('delivery')}>
            <div className="metric-head"><span>Average Delivery Rate</span><MonitorPlay size={16} /></div>
            <h2>{topMetrics.avgDeliveryRate.toFixed(1)}%</h2>
            <p className={topMetrics.avgDeliveryRate >= 90 ? 'positive' : 'negative'}>
              {topMetrics.avgDeliveryRate >= 90 ? 'Within pacing guardrails' : 'Below pacing target'}
            </p>
            <span className="drill-hint">Click to explore <ChevronRight size={12} /></span>
          </article>
          <article className="metric-card clickable" onClick={() => setDrilldown('alerts')}>
            <div className="metric-head"><span>Active Alerts</span><ShieldAlert size={16} /></div>
            <h2>{numberFmt.format(topMetrics.activeAlerts)}</h2>
            <p className="negative">Critical issues requiring intervention</p>
            <span className="drill-hint">Click to explore <ChevronRight size={12} /></span>
          </article>
          <article className="metric-card clickable" onClick={() => setDrilldown('revenue')}>
            <div className="metric-head"><span>Revenue at Risk</span><CircleDollarSign size={16} /></div>
            <h2>{currencyFmt.format(topMetrics.revenueAtRisk)}</h2>
            <p className="negative">Potentially lost from unresolved fallout</p>
            <span className="drill-hint">Click to explore <ChevronRight size={12} /></span>
          </article>
        </section>

        <section className="insight-banner">
          <Sparkles size={18} />
          <div>
            <strong>AI Predictive Insight</strong>
            <p>Cross-platform pacing suggests elevated underdelivery risk on linear live events in the next 6 hours.</p>
          </div>
          <button type="button" className="lightweight-toggle" onClick={() => setIsLightweightMode((p) => !p)}>
            {isLightweightMode ? 'Lightweight Mode: ON (sampled)' : 'Lightweight Mode: OFF (full)'}
          </button>
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
                  <stop offset="5%"  stopColor="#6180ff" stopOpacity={0.5} />
                  <stop offset="95%" stopColor="#6180ff" stopOpacity={0.03} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="#243057" strokeDasharray="3 3" />
              <XAxis dataKey="hour" stroke="#7f93c8" tick={{ fontSize: 11 }} />
              <YAxis stroke="#7f93c8" tickFormatter={(v) => compactFmt.format(v)} tick={{ fontSize: 11 }} />
              <Tooltip contentStyle={{ background: '#08122f', border: '1px solid #1f2c57', borderRadius: 12 }}
                formatter={(v) => numberFmt.format(Number(v ?? 0))} />
              <Area type="monotone" dataKey="target" stroke="#7f91b9" strokeDasharray="4 4" fill="none" name="Target" />
              <Area type="monotone" dataKey="actual" stroke="#6583ff" fill="url(#actualFill)" name="Actual" />
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
              <Tooltip contentStyle={{ background: '#08122f', border: '1px solid #1f2c57', borderRadius: 12 }}
                formatter={(v) => numberFmt.format(Number(v ?? 0))} />
            </PieChart>
          </ResponsiveContainer>
          <div className="legend-list">
            {falloutSeries.map((r, i) => (
              <div key={r.name}><span style={{ backgroundColor: PIE_COLORS[i % PIE_COLORS.length] }} />{r.name} ({r.value})</div>
            ))}
          </div>
        </section>

        <section className="alerts-card">
          <h3>Real-Time Alerts Feed</h3>
          <div className="alerts-list">
            {enrichedAlerts.map((alert) => (
              <button key={alert.alert_id} className="alert-row" onClick={() => setSelectedAlert(alert)} type="button">
                <div className={`severity-pill ${alert.severity.toLowerCase()}`}>{alert.severity}</div>
                <div className="alert-main">
                  <strong>{alert.campaign_name}</strong>
                  <span>{alert.platform_name} ({alert.network}) • {alert.advertiser_name}</span>
                  <p>{alert.trigger_value}</p>
                </div>
                <AlertTriangle size={16} />
              </button>
            ))}
          </div>
        </section>
      </main>
      )}

      {/* ════════════════════════════════════════════════════════════════
          CAMPAIGN HEALTH TAB
      ════════════════════════════════════════════════════════════════ */}
      {activeTab === 'health' && (
      <main className="health-shell">
        <div className="health-summary">
          {[
            { label: 'Healthy (≥90%)',  count: campaignHealth.filter((h) => h.deliveryRate >= 90).length, color: '#38d9b2' },
            { label: 'At Risk (75–89%)', count: campaignHealth.filter((h) => h.deliveryRate >= 75 && h.deliveryRate < 90).length, color: '#ffc156' },
            { label: 'Critical (<75%)', count: campaignHealth.filter((h) => h.deliveryRate < 75).length, color: '#ff7a8a' },
            { label: 'With Open Alerts', count: campaignHealth.filter((h) => h.alertCount > 0).length, color: '#6f84ff' },
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
                    <td style={{ color: h.fillRate >= 95 ? '#38d9b2' : h.fillRate >= 80 ? '#ffc156' : '#ff7a8a' }}>{h.fillRate.toFixed(1)}%</td>
                    <td style={{ color: h.vcr >= 70 ? '#38d9b2' : h.vcr >= 50 ? '#ffc156' : '#ff7a8a' }}>{h.vcr.toFixed(1)}%</td>
                    <td style={{ color: h.errorRate < 3 ? '#38d9b2' : h.errorRate < 8 ? '#ffc156' : '#ff7a8a' }}>{h.errorRate.toFixed(2)}%</td>
                    <td>
                      {h.alertCount > 0
                        ? <span className={`severity-pill ${(h.topSeverity ?? '').toLowerCase()}`}>{h.alertCount} {h.topSeverity}</span>
                        : <span className="no-alerts">✓ Clear</span>}
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
                    { label: 'Fill',     val: `${h.fillRate.toFixed(1)}%`,     color: h.fillRate >= 95 ? '#38d9b2' : '#ffc156' },
                    { label: 'VCR',      val: `${h.vcr.toFixed(1)}%`,          color: h.vcr >= 70 ? '#38d9b2' : '#ffc156' },
                    { label: 'Errors',   val: `${h.errorRate.toFixed(2)}%`,    color: h.errorRate < 3 ? '#38d9b2' : '#ff7a8a' },
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
                        <stop offset="5%"  stopColor="#6180ff" stopOpacity={0.5} />
                        <stop offset="95%" stopColor="#6180ff" stopOpacity={0.03} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid stroke="#1a2848" strokeDasharray="3 3" />
                    <XAxis dataKey="hour" stroke="#4a6494" tick={{ fontSize: 9 }} />
                    <YAxis stroke="#4a6494" tickFormatter={(v) => compactFmt.format(v)} tick={{ fontSize: 9 }} />
                    <Tooltip contentStyle={{ background: '#08122f', border: '1px solid #1f2c57', borderRadius: 8, fontSize: 11 }}
                      formatter={(v) => numberFmt.format(Number(v ?? 0))} />
                    <Area type="monotone" dataKey="delivered" stroke="#6583ff" fill="url(#hdFill)" name="Delivered" />
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
      <main className="intel-shell">

        {/* Agent pipeline visual */}
        <section className="pipeline-banner">
          <div className="pipeline-title"><Bot size={16} /> <strong>Agentic RCA Pipeline</strong> <span>— autonomous root cause analysis &amp; fix generation</span></div>
          <div className="pipeline-steps">
            {[
              { icon: <ShieldAlert size={14} />, label: 'Signal Detected',  desc: 'Anomaly threshold crossed' },
              { icon: <Search size={14} />,       label: 'Agent Dispatched', desc: 'RCA agent initialized' },
              { icon: <Database size={14} />,     label: 'Data Correlation', desc: 'Multi-source tool calls' },
              { icon: <Terminal size={14} />,     label: 'Root Cause ID',   desc: 'Hypothesis ranked by confidence' },
              { icon: <Zap size={14} />,          label: 'Fix Generated',   desc: 'Deployment artifact created' },
              { icon: <CheckCircle2 size={14} />, label: 'Human Review',    desc: 'Approve or reject' },
            ].map((s, i, arr) => (
              <div className="pipe-step-wrap" key={s.label}>
                <div className="pipe-step">
                  {s.icon}
                  <strong>{s.label}</strong>
                  <span>{s.desc}</span>
                </div>
                {i < arr.length - 1 && <ChevronRight size={14} className="pipe-arrow" />}
              </div>
            ))}
          </div>
        </section>

        {/* Agent sessions */}
        <div className="intel-sessions">
          {agentSessions.map((alert) => {
            const pb    = AGENT_PLAYBOOK[alert.alert_type]
            const dec   = reviewDecisions[alert.alert_id]
            if (!pb) return null
            return (
              <div className={`session-card ${dec ?? ''}`} key={alert.alert_id}>
                <div className="session-head">
                  <div className="session-meta">
                    <span className={`severity-pill ${alert.severity.toLowerCase()}`}>{alert.severity}</span>
                    <strong>{alert.alert_type}</strong>
                    <span className="session-sub">{alert.campaign_name} · {alert.platform_name}</span>
                  </div>
                  <div className="session-right">
                    <span className="session-revenue">{currencyFmt.format(Number(alert.revenue_impact_usd))} at risk</span>
                    {dec === 'approved' && <span className="dec-badge approved"><CheckCircle2 size={12} /> Approved &amp; Deployed</span>}
                    {dec === 'rejected' && <span className="dec-badge rejected"><XCircle size={12} /> Rejected</span>}
                    {!dec              && <span className="dec-badge pending"><Bot size={12} /> Awaiting Review</span>}
                  </div>
                </div>

                {/* Agent steps */}
                <div className="agent-steps">
                  {pb.steps.map((step, i) => (
                    <div className={`agent-step step-anim-${i}`} key={step.tool}>
                      <div className="step-icon">
                        {step.icon === 'search'   && <Search   size={12} />}
                        {step.icon === 'database' && <Database size={12} />}
                        {step.icon === 'terminal' && <Terminal size={12} />}
                        {step.icon === 'activity' && <Activity size={12} />}
                        {step.icon === 'zap'      && <Zap      size={12} />}
                      </div>
                      <div className="step-body">
                        <code className="step-tool">{step.tool}()</code>
                        <span className="step-desc">{step.desc}</span>
                        <span className="step-result">→ {step.result}</span>
                      </div>
                    </div>
                  ))}
                </div>

                {/* Root cause */}
                <div className="session-rca">
                  <div className="rca-label"><Bot size={12} /> Root Cause</div>
                  <p>{pb.rootCause}</p>
                  <div className="confidence">Confidence: <strong>{buildRca(alert).confidence}%</strong></div>
                </div>

                {/* Fix artifact */}
                <div className="session-fix">
                  <div className="fix-head">
                    <Terminal size={12} /> <span>Proposed Fix</span>
                    <span className="fix-type-badge">{pb.fixType}</span>
                    <span className="fix-recovery">Est. recovery: {pb.recoveryTime}</span>
                  </div>
                  <pre className="fix-code">{pb.fixCode}</pre>
                </div>

                {/* Actions */}
                {!dec && (
                  <div className="session-actions">
                    <button className="btn-approve" type="button"
                      onClick={() => setReviewDecisions((p) => ({ ...p, [alert.alert_id]: 'approved' }))}>
                      <CheckCircle2 size={14} /> Approve &amp; Deploy
                    </button>
                    <button className="btn-reject" type="button"
                      onClick={() => setReviewDecisions((p) => ({ ...p, [alert.alert_id]: 'rejected' }))}>
                      <XCircle size={14} /> Reject
                    </button>
                  </div>
                )}
              </div>
            )
          })}
        </div>
      </main>
      )}

      {/* ════════════════════════════════════════════════════════════════
          NOTIFICATIONS TAB
      ════════════════════════════════════════════════════════════════ */}
      {activeTab === 'notifications' && (
      <main className="notif-shell">
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

        <div className="notif-list">
          {enrichedAlerts
            .filter((a) => notifFilter === 'All' || a.severity === notifFilter)
            .map((alert) => {
              const pb  = AGENT_PLAYBOOK[alert.alert_type]
              const dec = reviewDecisions[alert.alert_id]
              const isOpen = expandedAlert === alert.alert_id
              const rca = buildRca(alert)
              return (
                <div className={`notif-card ${dec ?? 'pending'}`} key={alert.alert_id}>
                  <button className="notif-row" type="button" onClick={() => setExpandedAlert(isOpen ? null : alert.alert_id)}>
                    <span className={`severity-pill ${alert.severity.toLowerCase()}`}>{alert.severity}</span>
                    <div className="notif-info">
                      <strong>{alert.alert_type}</strong>
                      <span>{alert.campaign_name} · {alert.platform_name} · {alert.advertiser_name}</span>
                      <p>{alert.trigger_value}</p>
                    </div>
                    <div className="notif-right">
                      <span className="notif-revenue">{currencyFmt.format(Number(alert.revenue_impact_usd))}</span>
                      {dec === 'approved' && <span className="dec-badge approved"><CheckCircle2 size={11} /> Deployed</span>}
                      {dec === 'rejected' && <span className="dec-badge rejected"><XCircle size={11} /> Rejected</span>}
                      {!dec              && <span className="dec-badge pending"><Bot size={11} /> Pending</span>}
                      {isOpen ? <ChevronDown size={14} className="notif-chevron open" /> : <ChevronRight size={14} className="notif-chevron" />}
                    </div>
                  </button>

                  {isOpen && (
                    <div className="notif-detail">
                      {/* Agent steps */}
                      {pb && (
                        <div className="nd-section">
                          <div className="nd-label"><Bot size={13} /> Agent Analysis Steps</div>
                          <div className="agent-steps compact">
                            {pb.steps.map((step, i) => (
                              <div className={`agent-step step-anim-${i}`} key={step.tool}>
                                <div className="step-icon">
                                  {step.icon === 'search'   && <Search   size={11} />}
                                  {step.icon === 'database' && <Database size={11} />}
                                  {step.icon === 'terminal' && <Terminal size={11} />}
                                  {step.icon === 'activity' && <Activity size={11} />}
                                  {step.icon === 'zap'      && <Zap      size={11} />}
                                </div>
                                <div className="step-body">
                                  <code className="step-tool">{step.tool}()</code>
                                  <span className="step-desc">{step.desc}</span>
                                  <span className="step-result">→ {step.result}</span>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Root cause */}
                      <div className="nd-section">
                        <div className="nd-label"><Search size={13} /> Root Cause ({rca.confidence}% confidence)</div>
                        <p>{pb?.rootCause ?? rca.recommendation}</p>
                      </div>

                      {/* System trace */}
                      <div className="nd-section">
                        <div className="nd-label"><Terminal size={13} /> System Trace</div>
                        <pre className="notif-trace">{rca.trace.join('\n')}</pre>
                      </div>

                      {/* Fix artifact */}
                      {pb && (
                        <div className="nd-section">
                          <div className="nd-label">
                            <Zap size={13} /> Proposed Fix
                            <span className="fix-type-badge">{pb.fixType}</span>
                            <span className="fix-recovery">{pb.recoveryTime}</span>
                          </div>
                          <pre className="fix-code">{pb.fixCode}</pre>
                        </div>
                      )}

                      {/* Revenue impact */}
                      <div className="nd-section nd-impact">
                        <div><span className="impact-label">Revenue at Risk</span><span className="impact-val">{currencyFmt.format(Number(alert.revenue_impact_usd))}</span></div>
                        <div><span className="impact-label">Impression Shortfall</span><span className="impact-val">{numberFmt.format(alert.expected_impressions - alert.actual_impressions)}</span></div>
                        <div><span className="impact-label">Alert ID</span><span className="impact-val code">{alert.alert_id}</span></div>
                      </div>

                      {/* Actions */}
                      {!dec && (
                        <div className="nd-actions">
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
                      {dec === 'approved' && (
                        <div className="nd-deployed">
                          <CheckCircle2 size={15} /> Fix approved and queued for deployment.
                        </div>
                      )}
                      {dec === 'rejected' && (
                        <div className="nd-rejected">
                          <XCircle size={15} /> Fix rejected.{rejectReason[alert.alert_id] ? ` Reason: ${rejectReason[alert.alert_id]}` : ''}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )
            })}
        </div>
      </main>
      )}

      {/* ════════════════════════════════════════════════════════════════
          DATA EXPLORER TAB
      ════════════════════════════════════════════════════════════════ */}
      {activeTab === 'explorer' && (
      <main className="explorer-shell">
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
                  <path d="M0,0 L0,6 L7,3 z" fill="#3a5880" />
                </marker>
              </defs>
              <g stroke="#2c4878" strokeWidth="1.5" fill="none" markerEnd="url(#fk-arrow)">
                {ER_LINES.map((l, i) => <path key={i} d={l.d} />)}
              </g>
              <g fill="#3d5a88" fontSize="9" fontFamily="'JetBrains Mono','SFMono-Regular',monospace">
                {ER_LINES.map((l, i) => <text key={i} x={l.lx} y={l.ly}>{l.label}</text>)}
              </g>
              {ER_NODES.map(({ name, cx, cy, color, cols, standalone }) => {
                const w = 185, headerH = 28, rowH = 18
                const h = headerH + cols.length * rowH + 8
                const x = cx - w / 2, y = cy - h / 2
                return (
                  <g key={name}>
                    <rect x={x} y={y} width={w} height={h} rx={7} fill="#07112a" stroke={color} strokeWidth={1.5} strokeDasharray={standalone ? '5 3' : undefined} />
                    <rect x={x} y={y} width={w} height={headerH} rx={7} fill={color + '22'} />
                    <rect x={x} y={y + headerH - 2} width={w} height={2} fill={color + '44'} />
                    <text x={cx} y={y + 17} textAnchor="middle" fill={color} fontSize={11} fontWeight="bold" fontFamily="'JetBrains Mono','SFMono-Regular',monospace">{name}</text>
                    {cols.map(([flag, colName], i) => {
                      const rowY = y + headerH + i * rowH + 13
                      return (
                        <g key={colName}>
                          {flag === 'PK' && <text x={x + 6} y={rowY} fill="#fbbf24" fontSize={8} fontWeight="bold">PK</text>}
                          {flag === 'FK' && <text x={x + 6} y={rowY} fill="#7da8ff" fontSize={8} fontWeight="bold">FK</text>}
                          <text x={flag ? x + 22 : x + 8} y={rowY}
                            fill={flag === 'PK' ? '#fde68a' : flag === 'FK' ? '#bcd0ff' : '#6a85b0'}
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
                  {k.good !== '—' && <span className="thr-good">✓ {k.good}</span>}
                  {k.bad  !== '—' && <span className="thr-bad">✗ {k.bad}</span>}
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
                    <CartesianGrid stroke="#1a2848" strokeDasharray="3 3" />
                    <XAxis dataKey="name" stroke="#7f93c8" tick={{ fontSize: 12 }} />
                    <YAxis stroke="#7f93c8" tick={{ fontSize: 12 }} />
                    <Tooltip contentStyle={{ background: '#08122f', border: '1px solid #1f2c57', borderRadius: 10 }} />
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
                    <CartesianGrid stroke="#1a2848" strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" stroke="#7f93c8" tick={{ fontSize: 11 }} />
                    <YAxis type="category" dataKey="name" stroke="#7f93c8" tick={{ fontSize: 11 }} width={85} />
                    <Tooltip contentStyle={{ background: '#08122f', border: '1px solid #1f2c57', borderRadius: 10 }} />
                    <Bar dataKey="count" fill="#6583ff" radius={[0, 4, 4, 0]} name="Campaigns" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}

            {drilldown === 'delivery' && (
              <div className="modal-body">
                <p className="modal-sub">{worstDelivery.length} campaigns currently below 90% delivery target</p>
                <table className="dd-table">
                  <thead><tr><th>Campaign</th><th>Platform</th><th>Delivery %</th><th>Gap</th><th>Alerts</th></tr></thead>
                  <tbody>
                    {worstDelivery.map((h) => (
                      <tr key={h.campaign.campaign_id}>
                        <td><strong>{h.campaign.campaign_name}</strong><br /><code>{h.campaign.campaign_id}</code></td>
                        <td>{h.platform_name}</td>
                        <td style={{ color: healthColor(h.deliveryRate) }}><strong>{h.deliveryRate.toFixed(1)}%</strong></td>
                        <td style={{ color: '#ff7a8a' }}>{(100 - h.deliveryRate).toFixed(1)}% below</td>
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
                        <td style={{ color: '#ff7a8a' }}>{currencyFmt.format(t.revenue)}</td>
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
                        <td><strong>{a.campaign_name}</strong><br /><span style={{ color: '#4a6494', fontSize: '0.76rem' }}>{a.advertiser_name}</span></td>
                        <td>{a.alert_type}</td>
                        <td><span className={`severity-pill ${a.severity.toLowerCase()}`}>{a.severity}</span></td>
                        <td style={{ color: '#ff7a8a' }}><strong>{currencyFmt.format(Number(a.revenue_impact_usd))}</strong></td>
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
                        <stop offset="5%"  stopColor="#6180ff" stopOpacity={0.5} />
                        <stop offset="95%" stopColor="#6180ff" stopOpacity={0.03} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid stroke="#1a2848" strokeDasharray="3 3" />
                    <XAxis dataKey="hour" stroke="#7f93c8" tick={{ fontSize: 11 }} />
                    <YAxis stroke="#7f93c8" tickFormatter={(v) => compactFmt.format(v)} tick={{ fontSize: 11 }} />
                    <Tooltip contentStyle={{ background: '#08122f', border: '1px solid #1f2c57', borderRadius: 12 }}
                      formatter={(v) => numberFmt.format(Number(v ?? 0))} />
                    <Area type="monotone" dataKey="target" stroke="#7f91b9" strokeDasharray="4 4" fill="none" name="Target" />
                    <Area type="monotone" dataKey="actual" stroke="#6583ff" fill="url(#ddFill)" name="Actual" />
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
                    <CartesianGrid stroke="#1a2848" strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" stroke="#7f93c8" tickFormatter={(v) => compactFmt.format(v)} tick={{ fontSize: 10 }} />
                    <YAxis type="category" dataKey="type" stroke="#7f93c8" tick={{ fontSize: 10 }} width={145} />
                    <Tooltip contentStyle={{ background: '#08122f', border: '1px solid #1f2c57', borderRadius: 10 }}
                      formatter={(v, n) => n === 'revenue' ? currencyFmt.format(Number(v)) : v} />
                    <Bar dataKey="revenue" name="revenue" radius={[0, 4, 4, 0]} fill="#ff5d96" />
                  </BarChart>
                </ResponsiveContainer>
                <table className="dd-table">
                  <thead><tr><th>Alert Type</th><th>Count</th><th>Revenue at Risk</th><th>Agent Fix Available</th></tr></thead>
                  <tbody>
                    {alertTypeTotals.sort((a, b) => b.revenue - a.revenue).map((t) => (
                      <tr key={t.type}>
                        <td><strong>{t.type}</strong></td>
                        <td>{t.count}</td>
                        <td style={{ color: '#ff7a8a' }}>{currencyFmt.format(t.revenue)}</td>
                        <td>{AGENT_PLAYBOOK[t.type] ? <span style={{ color: '#38d9b2' }}>✓ Yes</span> : <span style={{ color: '#6a85b0' }}>—</span>}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}

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
