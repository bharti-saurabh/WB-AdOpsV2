import { useEffect, useMemo, useState } from 'react'
import Papa from 'papaparse'
import {
  Area,
  AreaChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  AlertTriangle,
  Bell,
  CircleDollarSign,
  Database,
  LayoutDashboard,
  MonitorPlay,
  ShieldAlert,
  Sparkles,
  Table2,
  TrendingUp,
  Wrench,
  X,
} from 'lucide-react'
import './App.css'

type Advertiser = {
  advertiser_id: string
  name: string
  industry: string
  tier: string
}

type Platform = {
  platform_id: string
  platform_name: string
  platform_type: string
}

type Campaign = {
  campaign_id: string
  campaign_name: string
  advertiser_id: string
  agency_id: string
  platform_id: string
  segment_id: string
  status: 'Active' | 'Paused' | 'Completed'
  start_date: string
  end_date: string
  target_impressions: number
  cpm_usd: number
}

type PerformanceRow = {
  log_hour: string
  campaign_id: string
  impressions_delivered: number
  vast_requests: number
  vast_responses: number
  avg_latency_ms: number
  error_count: number
  video_completes: number
}

type AlertRow = {
  alert_id: string
  alert_timestamp: string
  severity: 'Critical' | 'High' | 'Medium' | 'Warning'
  alert_type: string
  campaign_id: string
  trigger_value: string
  threshold: string
  expected_impressions: number
  actual_impressions: number
  revenue_impact_usd: number
  status: string
}

type KpiRow = {
  kpi_name: string
  value: string
  unit: string
  as_of: string
}

type EnrichedAlert = AlertRow & {
  campaign_name: string
  advertiser_name: string
  platform_name: string
  network: 'Streaming' | 'Linear'
}

const PIE_COLORS = ['#1bc7ff', '#6f84ff', '#ff5d96', '#ffc156', '#55e4a8']

const numberFmt = new Intl.NumberFormat('en-US')
const compactFmt = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 })
const currencyFmt = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 })

function parseDate(dateString: string): Date {
  return new Date(dateString.replace(' ', 'T'))
}

function flightHours(c: Campaign): number {
  const start = new Date(`${c.start_date}T00:00:00`)
  const end = new Date(`${c.end_date}T00:00:00`)
  const days = Math.max(1, Math.round((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24)) + 1)
  return days * 24
}

function getNetwork(platformName: string): 'Streaming' | 'Linear' {
  return ['TNT', 'TBS', 'truTV'].includes(platformName) ? 'Linear' : 'Streaming'
}

function getSeverityWeight(sev: string): number {
  if (sev === 'Critical') return 0
  if (sev === 'High') return 1
  if (sev === 'Warning') return 2
  return 3
}

function loadCsv<T>(url: string): Promise<T[]> {
  return new Promise((resolve, reject) => {
    Papa.parse<T>(url, {
      download: true,
      header: true,
      dynamicTyping: true,
      skipEmptyLines: true,
      complete: (result: Papa.ParseResult<T>) => resolve(result.data),
      error: (err: Error) => reject(err),
    })
  })
}

function loadPerformanceSample(
  url: string,
  sampleEveryNRows: number,
  includeCampaignIds: Set<string>,
): Promise<PerformanceRow[]> {
  return new Promise((resolve, reject) => {
    const sampled: PerformanceRow[] = []
    let rowIndex = 0
    Papa.parse<PerformanceRow>(url, {
      download: true,
      header: true,
      dynamicTyping: true,
      skipEmptyLines: true,
      step: (result: Papa.ParseStepResult<PerformanceRow>) => {
        const row = result.data
        const keepBySample = rowIndex % sampleEveryNRows === 0
        const keepByCampaign = includeCampaignIds.has(String(row.campaign_id))
        if (keepBySample || keepByCampaign) {
          sampled.push(row)
        }
        rowIndex += 1
      },
      complete: () => resolve(sampled),
      error: (err: Error) => reject(err),
    })
  })
}

function buildRca(alert: EnrichedAlert) {
  const confidenceMap: Record<string, number> = {
    'Empty Audience Segment': 96,
    'DMP Segment Sync Failure': 93,
    'Delivery Drop': 88,
    'VAST Timeout': 91,
    'Roku Transcode Risk': 90,
    'Prime-Time Underdelivery': 84,
  }

  const traceMap: Record<string, string[]> = {
    'Empty Audience Segment': [
      '[SEGMENT-GRAPH] INFO  Segment lookup request: SEG-001',
      '[SEGMENT-GRAPH] ERROR 404_SEGMENT_NOT_FOUND in MAX_IDENTITY_GRAPH',
      '[ELIGIBILITY] WARN 0 eligible users returned for campaign targeting expression',
      '[DELIVERY] CRITICAL campaign halted due to empty audience cardinality',
    ],
    'DMP Segment Sync Failure': [
      '[DMP-CONNECTOR] INFO ingest batch started for SEG-001',
      '[DMP-CONNECTOR] ERROR 502_UPSTREAM_TIMEOUT while resolving advertiser taxonomy IDs',
      '[MAPPING] ERROR 404_SEGMENT_NOT_FOUND for external key Action_Enthusiasts',
      '[RECOVERY] WARN auto-retry exhausted after 5 attempts',
    ],
    'VAST Timeout': [
      '[AD-SERVER] INFO request fanout to decision engine',
      '[VAST] ERROR 504_TIMEOUT from creative decision endpoint',
      '[VAST] ERROR no fallback ad pod available',
      '[PLAYER] WARN response payload empty, defaulting to slate',
    ],
    'Roku Transcode Risk': [
      '[TRANSCODE] INFO profile match: Roku_AVC_Level_4_1',
      '[TRANSCODE] ERROR bitrate 18500kbps exceeds device threshold 15000kbps',
      '[PACKAGER] WARN fallback rendition missing for 6s GOP',
      '[DELIVERY] WARN increased startup failure risk for Roku household cohort',
    ],
    'Delivery Drop': [
      '[PACING] INFO expected delivery drift crossed 25% threshold',
      '[FORECAST] WARN underdelivery concentrated in prime-time slots',
      '[INVENTORY] WARN constrained avails on premium sports break pods',
      '[ROUTER] INFO realloc recommendation generated for cross-network balancing',
    ],
    'Prime-Time Underdelivery': [
      '[LINEAR-SCHED] WARN live sports overrun consumed planned ad pods',
      '[TRAFFICKER] WARN make-good queue depth increased to 42 spots',
      '[PACING] WARN campaign behind target in 18:00-21:00 window',
      '[OPTIMIZER] INFO recommending spillover to streaming inventory',
    ],
  }

  const fixMap: Record<string, string> = {
    'Empty Audience Segment': 'Re-map SEG-001 to valid first-party graph key, trigger identity sync, and replay queued placements for last 4 hours.',
    'DMP Segment Sync Failure': 'Re-run DMP connector job with corrected taxonomy mapping and enable temporary fallback to prior-day successful segment snapshot.',
    'VAST Timeout': 'Fail over to backup ad decision endpoint, lower timeout ceiling to 1800ms, and deploy fallback creatives for affected device classes.',
    'Roku Transcode Risk': 'Regenerate creative renditions under 15,000 kbps and enforce preflight validation in ingest pipeline for Roku-targeted flights.',
    'Delivery Drop': 'Shift 12% budget weight to available MAX/TBS inventory, loosen frequency cap by 1 impression, and prioritize high-fill pods.',
    'Prime-Time Underdelivery': 'Insert make-good spots across late-prime windows and rebalance remaining goal to streaming placements automatically.',
  }

  return {
    confidence: confidenceMap[alert.alert_type] ?? 82,
    trace: traceMap[alert.alert_type] ?? ['[SYSTEM] No detailed trace available.'],
    recommendation:
      fixMap[alert.alert_type] ?? 'Escalate to AdOps engineering and manually validate taxonomy, inventory, and creative eligibility constraints.',
  }
}

const TABLE_CATALOG = [
  {
    name: 'advertisers', file: 'advertisers.csv', color: '#4472ff',
    description: 'Brand accounts purchasing ad inventory across Warner networks.',
    columns: [
      { name: 'advertiser_id', type: 'STRING', pk: true, fk: false },
      { name: 'name', type: 'STRING', pk: false, fk: false },
      { name: 'industry', type: 'STRING', pk: false, fk: false },
      { name: 'tier', type: 'STRING', pk: false, fk: false },
    ],
  },
  {
    name: 'agencies', file: 'agencies.csv', color: '#4472ff',
    description: 'Media buying agencies placing orders on behalf of advertisers.',
    columns: [
      { name: 'agency_id', type: 'STRING', pk: true, fk: false },
      { name: 'name', type: 'STRING', pk: false, fk: false },
      { name: 'contact_email', type: 'STRING', pk: false, fk: false },
    ],
  },
  {
    name: 'platforms', file: 'platforms.csv', color: '#4472ff',
    description: 'Ad delivery platforms — Streaming (HBO Max, CNN+) and Linear (TNT, TBS, truTV).',
    columns: [
      { name: 'platform_id', type: 'STRING', pk: true, fk: false },
      { name: 'platform_name', type: 'STRING', pk: false, fk: false },
      { name: 'platform_type', type: 'STRING', pk: false, fk: false },
    ],
  },
  {
    name: 'audience_segments', file: 'audience_segments.csv', color: '#4472ff',
    description: 'DMP-sourced audience targeting segments used in campaign targeting expressions.',
    columns: [
      { name: 'segment_id', type: 'STRING', pk: true, fk: false },
      { name: 'segment_name', type: 'STRING', pk: false, fk: false },
      { name: 'provider', type: 'STRING', pk: false, fk: false },
      { name: 'sync_status', type: 'STRING', pk: false, fk: false },
    ],
  },
  {
    name: 'campaigns', file: 'campaigns.csv', color: '#9333ea',
    description: 'Core campaign records. Central fact table linking advertisers, agencies, platforms, and segments.',
    columns: [
      { name: 'campaign_id', type: 'STRING', pk: true, fk: false },
      { name: 'campaign_name', type: 'STRING', pk: false, fk: false },
      { name: 'advertiser_id', type: 'STRING', pk: false, fk: true },
      { name: 'agency_id', type: 'STRING', pk: false, fk: true },
      { name: 'platform_id', type: 'STRING', pk: false, fk: true },
      { name: 'segment_id', type: 'STRING', pk: false, fk: true },
      { name: 'status', type: 'ENUM', pk: false, fk: false },
      { name: 'start_date', type: 'DATE', pk: false, fk: false },
      { name: 'end_date', type: 'DATE', pk: false, fk: false },
      { name: 'target_impressions', type: 'INT', pk: false, fk: false },
      { name: 'cpm_usd', type: 'FLOAT', pk: false, fk: false },
    ],
  },
  {
    name: 'creatives', file: 'creatives.csv', color: '#f59e0b',
    description: 'Ad creative assets (video/display) associated with campaign flights.',
    columns: [
      { name: 'creative_id', type: 'STRING', pk: true, fk: false },
      { name: 'campaign_id', type: 'STRING', pk: false, fk: true },
      { name: 'creative_name', type: 'STRING', pk: false, fk: false },
      { name: 'format', type: 'STRING', pk: false, fk: false },
      { name: 'bitrate_kbps', type: 'INT', pk: false, fk: false },
      { name: 'duration_sec', type: 'INT', pk: false, fk: false },
    ],
  },
  {
    name: 'performance_log', file: 'performance_log.csv', color: '#22c55e',
    description: 'Hourly delivery metrics per campaign. Primary time-series fact table (~155K rows, 9.4 MB).',
    columns: [
      { name: 'log_hour', type: 'DATETIME', pk: false, fk: false },
      { name: 'campaign_id', type: 'STRING', pk: false, fk: true },
      { name: 'impressions_delivered', type: 'INT', pk: false, fk: false },
      { name: 'vast_requests', type: 'INT', pk: false, fk: false },
      { name: 'vast_responses', type: 'INT', pk: false, fk: false },
      { name: 'avg_latency_ms', type: 'INT', pk: false, fk: false },
      { name: 'error_count', type: 'INT', pk: false, fk: false },
      { name: 'video_completes', type: 'INT', pk: false, fk: false },
    ],
  },
  {
    name: 'alerts', file: 'alerts.csv', color: '#ef4444',
    description: 'System-generated delivery and performance alerts with revenue impact estimates.',
    columns: [
      { name: 'alert_id', type: 'STRING', pk: true, fk: false },
      { name: 'alert_timestamp', type: 'DATETIME', pk: false, fk: false },
      { name: 'severity', type: 'ENUM', pk: false, fk: false },
      { name: 'alert_type', type: 'STRING', pk: false, fk: false },
      { name: 'campaign_id', type: 'STRING', pk: false, fk: true },
      { name: 'trigger_value', type: 'STRING', pk: false, fk: false },
      { name: 'threshold', type: 'STRING', pk: false, fk: false },
      { name: 'expected_impressions', type: 'INT', pk: false, fk: false },
      { name: 'actual_impressions', type: 'INT', pk: false, fk: false },
      { name: 'revenue_impact_usd', type: 'FLOAT', pk: false, fk: false },
      { name: 'status', type: 'STRING', pk: false, fk: false },
    ],
  },
  {
    name: 'kpi_summary', file: 'kpi_summary.csv', color: '#06b6d4',
    description: 'Pre-aggregated KPI snapshot. Materialized view of key system metrics for dashboard tiles.',
    columns: [
      { name: 'kpi_name', type: 'STRING', pk: false, fk: false },
      { name: 'value', type: 'STRING', pk: false, fk: false },
      { name: 'unit', type: 'STRING', pk: false, fk: false },
      { name: 'as_of', type: 'DATETIME', pk: false, fk: false },
    ],
  },
] as const

const KPI_FORMULAS = [
  {
    name: 'Delivery Rate',
    formula: 'impressions_delivered ÷ (target_impressions ÷ flight_hours) × 100',
    unit: '%', source: 'performance_log + campaigns',
    description: "Measures hourly pacing against the campaign's target rate. Below 90% triggers a pacing alert.",
    good: '≥ 90%', bad: '< 85%',
  },
  {
    name: 'Fill Rate',
    formula: 'vast_responses ÷ vast_requests × 100',
    unit: '%', source: 'performance_log',
    description: 'Percentage of ad requests that returned a valid VAST response from the ad decision engine.',
    good: '≥ 95%', bad: '< 80%',
  },
  {
    name: 'Video Completion Rate (VCR)',
    formula: 'video_completes ÷ impressions_delivered × 100',
    unit: '%', source: 'performance_log',
    description: 'Ratio of viewers who watched the full ad. Key quality signal for premium video inventory.',
    good: '≥ 70%', bad: '< 50%',
  },
  {
    name: 'VAST Error Rate',
    formula: 'error_count ÷ vast_requests × 100',
    unit: '%', source: 'performance_log',
    description: 'Proportion of ad requests resulting in VAST errors (timeouts, empty pods, malformed responses).',
    good: '< 3%', bad: '> 8%',
  },
  {
    name: 'Revenue at Risk',
    formula: "Σ revenue_impact_usd  WHERE  status = 'Open'",
    unit: 'USD', source: 'alerts',
    description: 'Total estimated revenue exposure from all unresolved alerts. Primary urgency signal for AdOps triage.',
    good: '< $10K', bad: '> $50K',
  },
  {
    name: 'Effective CPM',
    formula: '(campaign_budget ÷ impressions_delivered) × 1,000',
    unit: 'USD/M', source: 'campaigns + performance_log',
    description: 'Actual cost per 1,000 delivered impressions vs. contracted cpm_usd. Deviation flags billing risk.',
    good: 'Within ±10% of cpm_usd', bad: '> 15% above cpm_usd',
  },
  {
    name: 'Impression Shortfall',
    formula: 'expected_impressions − actual_impressions',
    unit: 'imps', source: 'alerts',
    description: 'Absolute delivery gap at alert time. Used by traffickers to calculate make-good commitments.',
    good: '< 5% of target', bad: '> 15% of target',
  },
  {
    name: 'Flight Hours',
    formula: '(end_date − start_date + 1 day) × 24',
    unit: 'hours', source: 'campaigns',
    description: 'Total flight duration in hours. Denominator for per-hour target impressions in pacing calculations.',
    good: '—', bad: '—',
  },
] as const

type ErNode = { name: string; cx: number; cy: number; color: string; standalone?: boolean; cols: [string, string][] }
type ErLine = { d: string; label: string; lx: number; ly: number }

const ER_NODES: ErNode[] = [
  { name: 'advertisers',      cx: 130, cy: 70,  color: '#4472ff', cols: [['PK','advertiser_id'],['','name'],['','industry'],['','tier']] },
  { name: 'agencies',         cx: 130, cy: 250, color: '#4472ff', cols: [['PK','agency_id'],['','name'],['','contact_email']] },
  { name: 'audience_segments',cx: 130, cy: 430, color: '#4472ff', cols: [['PK','segment_id'],['','segment_name'],['','provider'],['','sync_status']] },
  { name: 'kpi_summary',      cx: 460, cy: 70,  color: '#06b6d4', standalone: true, cols: [['','kpi_name'],['','value'],['','unit'],['','as_of']] },
  { name: 'campaigns',        cx: 460, cy: 255, color: '#9333ea', cols: [['PK','campaign_id'],['FK','advertiser_id'],['FK','agency_id'],['FK','platform_id'],['FK','segment_id'],['','status'],['','cpm_usd']] },
  { name: 'alerts',           cx: 460, cy: 440, color: '#ef4444', cols: [['PK','alert_id'],['FK','campaign_id'],['','severity'],['','alert_type'],['','revenue_impact_usd']] },
  { name: 'platforms',        cx: 790, cy: 70,  color: '#4472ff', cols: [['PK','platform_id'],['','platform_name'],['','platform_type']] },
  { name: 'creatives',        cx: 790, cy: 250, color: '#f59e0b', cols: [['PK','creative_id'],['FK','campaign_id'],['','creative_name'],['','format'],['','bitrate_kbps']] },
  { name: 'performance_log',  cx: 790, cy: 430, color: '#22c55e', cols: [['FK','campaign_id'],['','log_hour'],['','impressions_delivered'],['','vast_requests'],['','video_completes']] },
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

function App() {
  const [advertisers, setAdvertisers] = useState<Advertiser[]>([])
  const [platforms, setPlatforms] = useState<Platform[]>([])
  const [campaigns, setCampaigns] = useState<Campaign[]>([])
  const [performance, setPerformance] = useState<PerformanceRow[]>([])
  const [alerts, setAlerts] = useState<AlertRow[]>([])
  const [kpis, setKpis] = useState<KpiRow[]>([])
  const [selectedAlert, setSelectedAlert] = useState<EnrichedAlert | null>(null)
  const [autofixDone, setAutofixDone] = useState<Record<string, boolean>>({})
  const [loading, setLoading] = useState(true)
  const [isLightweightMode, setIsLightweightMode] = useState(true)
  const [activeTab, setActiveTab] = useState<'overview' | 'explorer'>('overview')

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

      const alertCampaignIds = new Set(al.map((row) => row.campaign_id))
      const perf = isLightweightMode
        ? await loadPerformanceSample(`${import.meta.env.BASE_URL}data/performance_log.csv`, 8, alertCampaignIds)
        : await loadCsv<PerformanceRow>(`${import.meta.env.BASE_URL}data/performance_log.csv`)

      setAdvertisers(a)
      setPlatforms(p)
      setCampaigns(c)
      setPerformance(perf)
      setAlerts(al)
      setKpis(k)
      setLoading(false)
    }

    load().catch((err) => {
      console.error('Failed to load dashboard data', err)
      setLoading(false)
    })
  }, [isLightweightMode])

  const advertiserMap = useMemo(() => Object.fromEntries(advertisers.map((a) => [a.advertiser_id, a])), [advertisers])
  const platformMap = useMemo(() => Object.fromEntries(platforms.map((p) => [p.platform_id, p])), [platforms])
  const campaignMap = useMemo(() => Object.fromEntries(campaigns.map((c) => [c.campaign_id, c])), [campaigns])

  const maxPerfTime = useMemo(() => {
    if (!performance.length) return null
    return performance.reduce((latest, row) => {
      const t = parseDate(row.log_hour)
      return !latest || t > latest ? t : latest
    }, null as Date | null)
  }, [performance])

  const last24Rows = useMemo(() => {
    if (!maxPerfTime) return []
    const from = new Date(maxPerfTime.getTime() - 23 * 60 * 60 * 1000)
    return performance.filter((r) => parseDate(r.log_hour) >= from)
  }, [maxPerfTime, performance])

  const topMetrics = useMemo(() => {
    const activeCampaigns = campaigns.filter((c) => c.status === 'Active').length

    let expected = 0
    let actual = 0
    for (const row of last24Rows) {
      const campaign = campaignMap[row.campaign_id]
      if (!campaign) continue
      expected += campaign.target_impressions / flightHours(campaign)
      actual += row.impressions_delivered
    }
    const avgDeliveryRate = expected > 0 ? (actual / expected) * 100 : 0

    const activeAlerts = alerts.filter((a) => a.status.toLowerCase() === 'open').length
    const revenueAtRisk =
      Number(kpis.find((k) => k.kpi_name === 'Revenue at Risk')?.value) ||
      alerts.reduce((sum, a) => sum + Number(a.revenue_impact_usd || 0), 0)

    return { activeCampaigns, avgDeliveryRate, activeAlerts, revenueAtRisk }
  }, [campaigns, last24Rows, campaignMap, alerts, kpis])

  const pacingSeries = useMemo(() => {
    if (!last24Rows.length) return []
    const byHour = new Map<string, { expected: number; actual: number }>()

    for (const row of last24Rows) {
      const c = campaignMap[row.campaign_id]
      if (!c) continue
      const expected = c.target_impressions / flightHours(c)
      const slot = row.log_hour.slice(11, 16)
      const prev = byHour.get(slot) ?? { expected: 0, actual: 0 }
      prev.expected += expected
      prev.actual += row.impressions_delivered
      byHour.set(slot, prev)
    }

    const hours = [...byHour.keys()].sort()
    let cumulativeExpected = 0
    let cumulativeActual = 0

    return hours.map((hour) => {
      const row = byHour.get(hour)!
      cumulativeExpected += row.expected
      cumulativeActual += row.actual
      return {
        hour,
        target: Math.round(cumulativeExpected),
        actual: Math.round(cumulativeActual),
      }
    })
  }, [last24Rows, campaignMap])

  const falloutSeries = useMemo(() => {
    if (!alerts.length) return []
    const maxAlertTime = alerts.reduce((latest, a) => {
      const t = parseDate(a.alert_timestamp)
      return !latest || t > latest ? t : latest
    }, null as Date | null)
    if (!maxAlertTime) return []

    const from = new Date(maxAlertTime.getTime() - 24 * 60 * 60 * 1000)
    const windowAlerts = alerts.filter((a) => parseDate(a.alert_timestamp) >= from && a.status.toLowerCase() === 'open')

    const reasonMap = new Map<string, number>()
    for (const a of windowAlerts) {
      reasonMap.set(a.alert_type, (reasonMap.get(a.alert_type) ?? 0) + 1)
    }

    return [...reasonMap.entries()].map(([name, value]) => ({ name, value }))
  }, [alerts])

  const enrichedAlerts = useMemo<EnrichedAlert[]>(() => {
    return alerts
      .filter((a) => a.status.toLowerCase() === 'open')
      .map((a) => {
        const campaign = campaignMap[a.campaign_id]
        const advertiser = campaign ? advertiserMap[campaign.advertiser_id] : undefined
        const platform = campaign ? platformMap[campaign.platform_id] : undefined
        const platformName = platform?.platform_name ?? 'Unknown Platform'

        return {
          ...a,
          campaign_name: campaign?.campaign_name ?? a.campaign_id,
          advertiser_name: advertiser?.name ?? 'Unknown Advertiser',
          platform_name: platformName,
          network: getNetwork(platformName),
        }
      })
      .sort((a, b) => {
        const severitySort = getSeverityWeight(a.severity) - getSeverityWeight(b.severity)
        if (severitySort !== 0) return severitySort
        return parseDate(b.alert_timestamp).getTime() - parseDate(a.alert_timestamp).getTime()
      })
  }, [alerts, campaignMap, advertiserMap, platformMap])

  if (loading) {
    return (
      <div className="loading-shell">
        <div className="loading-card">
          <MonitorPlay size={22} />
          <p>Building AdOps Intelligence Workspace...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="app-shell">
      <header className="top-nav">
        <div className="brand">
          <div className="brand-mark">AO</div>
          <div>
            <strong>AdOps Copilot</strong>
            <span>Warner Network Control Plane</span>
          </div>
        </div>

        <nav className="tabs">
          <button className={`tab ${activeTab === 'overview' ? 'active' : ''}`} onClick={() => setActiveTab('overview')}><LayoutDashboard size={15} /> Overview</button>
          <button className="tab">Campaign Health</button>
          <button className="tab">Intelligence Feed</button>
          <button className="tab"><Bell size={15} /> Notifications</button>
          <button className={`tab ${activeTab === 'explorer' ? 'active' : ''}`} onClick={() => setActiveTab('explorer')}><Database size={15} /> Data Explorer</button>
        </nav>

        <div className="status-pill">System Status: <strong>Degraded</strong></div>
      </header>

      {activeTab === 'overview' ? (
      <main className="content-grid">
        <section className="metric-grid">
          <article className="metric-card">
            <div className="metric-head"><span>Active Campaigns</span><TrendingUp size={16} /></div>
            <h2>{numberFmt.format(topMetrics.activeCampaigns)}</h2>
            <p className="positive">Live across Streaming + Linear</p>
          </article>

          <article className="metric-card">
            <div className="metric-head"><span>Average Delivery Rate</span><MonitorPlay size={16} /></div>
            <h2>{topMetrics.avgDeliveryRate.toFixed(1)}%</h2>
            <p className={topMetrics.avgDeliveryRate >= 90 ? 'positive' : 'negative'}>
              {topMetrics.avgDeliveryRate >= 90 ? 'Within pacing guardrails' : 'Below pacing target'}
            </p>
          </article>

          <article className="metric-card">
            <div className="metric-head"><span>Active Alerts</span><ShieldAlert size={16} /></div>
            <h2>{numberFmt.format(topMetrics.activeAlerts)}</h2>
            <p className="negative">Critical issues requiring intervention</p>
          </article>

          <article className="metric-card">
            <div className="metric-head"><span>Revenue at Risk</span><CircleDollarSign size={16} /></div>
            <h2>{currencyFmt.format(topMetrics.revenueAtRisk)}</h2>
            <p className="negative">Potentially lost from unresolved fallout</p>
          </article>
        </section>

        <section className="insight-banner">
          <Sparkles size={18} />
          <div>
            <strong>AI Predictive Insight</strong>
            <p>Cross-platform pacing suggests elevated underdelivery risk on linear live events in the next 6 hours.</p>
          </div>
          <button
            type="button"
            className="lightweight-toggle"
            onClick={() => setIsLightweightMode((prev) => !prev)}
          >
            {isLightweightMode ? 'Lightweight Mode: ON (sampled)' : 'Lightweight Mode: OFF (full)'}
          </button>
        </section>

        <section className="chart-card pacing-card">
          <h3>Network Delivery Pacing (Last 24h)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={pacingSeries} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="actualFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#6180ff" stopOpacity={0.5} />
                  <stop offset="95%" stopColor="#6180ff" stopOpacity={0.03} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="#243057" strokeDasharray="3 3" />
              <XAxis dataKey="hour" stroke="#7f93c8" />
              <YAxis stroke="#7f93c8" tickFormatter={(v) => compactFmt.format(v)} />
              <Tooltip
                contentStyle={{ background: '#08122f', border: '1px solid #1f2c57', borderRadius: 12 }}
                formatter={(value) => numberFmt.format(Number(value ?? 0))}
              />
              <Area type="monotone" dataKey="target" stroke="#7f91b9" strokeDasharray="4 4" fill="none" name="Target" />
              <Area type="monotone" dataKey="actual" stroke="#6583ff" fill="url(#actualFill)" name="Actual" />
            </AreaChart>
          </ResponsiveContainer>
          <p className="chart-footnote">
            AI Insight: Delivery drops are concentrated in linear prime-time windows, with recovery likely after streaming spillover.
          </p>
        </section>

        <section className="chart-card donut-card">
          <h3>Fallout Reasons (24h)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie data={falloutSeries} innerRadius={74} outerRadius={106} dataKey="value" nameKey="name" paddingAngle={2}>
                {falloutSeries.map((entry, index) => (
                  <Cell key={entry.name} fill={PIE_COLORS[index % PIE_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{ background: '#08122f', border: '1px solid #1f2c57', borderRadius: 12 }}
                formatter={(value) => numberFmt.format(Number(value ?? 0))}
              />
            </PieChart>
          </ResponsiveContainer>
          <div className="legend-list">
            {falloutSeries.map((reason, idx) => (
              <div key={reason.name}>
                <span style={{ backgroundColor: PIE_COLORS[idx % PIE_COLORS.length] }} />
                {reason.name} ({reason.value})
              </div>
            ))}
          </div>
        </section>

        <section className="alerts-card">
          <h3>Real-Time Alerts Feed</h3>
          <div className="alerts-list">
            {enrichedAlerts.map((alert) => (
              <button
                key={alert.alert_id}
                className="alert-row"
                onClick={() => setSelectedAlert(alert)}
                type="button"
              >
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
      ) : (
      <main className="explorer-shell">

        {/* ── SECTION 1: TABLE CATALOG ── */}
        <section className="ex-section">
          <div className="ex-section-head">
            <Table2 size={17} />
            <h2>Data Catalog</h2>
            <span className="ex-badge">9 tables · 6 CSV sources loaded</span>
          </div>
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

        {/* ── SECTION 2: ER DIAGRAM ── */}
        <section className="ex-section">
          <div className="ex-section-head">
            <Database size={17} />
            <h2>Entity Relationship Diagram</h2>
            <span className="ex-badge">FK → PK relationships · dashed = no FK</span>
          </div>
          <div className="ex-er-wrap">
            <svg viewBox="0 0 920 500" className="ex-er-svg">
              <defs>
                <marker id="fk-arrow" markerWidth="7" markerHeight="7" refX="6" refY="3" orient="auto">
                  <path d="M0,0 L0,6 L7,3 z" fill="#3a5880" />
                </marker>
              </defs>

              {/* Relationship lines */}
              <g stroke="#2c4878" strokeWidth="1.5" fill="none" markerEnd="url(#fk-arrow)">
                {ER_LINES.map((l, i) => <path key={i} d={l.d} />)}
              </g>

              {/* FK column labels on lines */}
              <g fill="#3d5a88" fontSize="9" fontFamily="'JetBrains Mono','SFMono-Regular',monospace">
                {ER_LINES.map((l, i) => <text key={i} x={l.lx} y={l.ly}>{l.label}</text>)}
              </g>

              {/* Table boxes */}
              {ER_NODES.map(({ name, cx, cy, color, cols, standalone }) => {
                const w = 185
                const headerH = 28
                const rowH = 18
                const h = headerH + cols.length * rowH + 8
                const x = cx - w / 2
                const y = cy - h / 2
                return (
                  <g key={name}>
                    <rect x={x} y={y} width={w} height={h} rx={7}
                      fill="#07112a" stroke={color} strokeWidth={1.5}
                      strokeDasharray={standalone ? '5 3' : undefined}
                    />
                    <rect x={x} y={y} width={w} height={headerH} rx={7} fill={color + '22'} />
                    <rect x={x} y={y + headerH - 2} width={w} height={2} fill={color + '44'} />
                    <text x={cx} y={y + 17} textAnchor="middle" fill={color}
                      fontSize={11} fontWeight="bold"
                      fontFamily="'JetBrains Mono','SFMono-Regular',monospace">
                      {name}
                    </text>
                    {cols.map(([flag, colName], i) => {
                      const rowY = y + headerH + i * rowH + 13
                      return (
                        <g key={colName}>
                          {flag === 'PK' && <text x={x + 6} y={rowY} fill="#fbbf24" fontSize={8} fontWeight="bold">PK</text>}
                          {flag === 'FK' && <text x={x + 6} y={rowY} fill="#7da8ff" fontSize={8} fontWeight="bold">FK</text>}
                          <text x={flag ? x + 22 : x + 8} y={rowY}
                            fill={flag === 'PK' ? '#fde68a' : flag === 'FK' ? '#bcd0ff' : '#6a85b0'}
                            fontSize={10}
                            fontFamily="'JetBrains Mono','SFMono-Regular',monospace">
                            {colName}
                          </text>
                        </g>
                      )
                    })}
                  </g>
                )
              })}
            </svg>
          </div>
        </section>

        {/* ── SECTION 3: KPI FORMULAS ── */}
        <section className="ex-section">
          <div className="ex-section-head">
            <TrendingUp size={17} />
            <h2>KPI Formulas</h2>
            <span className="ex-badge">{KPI_FORMULAS.length} metrics defined</span>
          </div>
          <div className="kpi-grid">
            {KPI_FORMULAS.map((k) => (
              <div className="kpi-card" key={k.name}>
                <div className="kpi-card-head">
                  <strong>{k.name}</strong>
                  <span className="kpi-source-tag">{k.source}</span>
                </div>
                <code className="kpi-formula">{k.formula}</code>
                <p className="kpi-desc">{k.description}</p>
                <div className="kpi-thresholds">
                  {k.good !== '—' && <span className="thr-good">✓ {k.good}</span>}
                  {k.bad !== '—' && <span className="thr-bad">✗ {k.bad}</span>}
                </div>
              </div>
            ))}
          </div>
        </section>

      </main>
      )}

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
            <p>
              {selectedAlert.alert_type} is causing delivery instability on {selectedAlert.platform_name}. The issue is tied to data-path failures between advertiser definitions and ad-serving eligibility checks.
            </p>
            <div className="confidence">Confidence Score: <strong>{buildRca(selectedAlert).confidence}%</strong></div>
          </div>

          <div className="rca-section">
            <h4>System Trace Logs</h4>
            <pre>
              {buildRca(selectedAlert).trace.join('\n')}
            </pre>
          </div>

          <div className="rca-section">
            <h4>Recommended Resolution</h4>
            <p>{buildRca(selectedAlert).recommendation}</p>
            <button
              className="autofix-btn"
              type="button"
              onClick={() => setAutofixDone((prev) => ({ ...prev, [selectedAlert.alert_id]: true }))}
              disabled={Boolean(autofixDone[selectedAlert.alert_id])}
            >
              <Wrench size={15} /> {autofixDone[selectedAlert.alert_id] ? 'Auto-Fix Queued' : 'Execute Auto-Fix'}
            </button>
          </div>
        </aside>
      )}
    </div>
  )
}

export default App
