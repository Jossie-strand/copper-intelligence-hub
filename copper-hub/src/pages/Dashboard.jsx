import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
} from 'recharts'
import { useNavigate } from 'react-router-dom'
import { useDashboardData } from '../data/hooks/useDashboardData'
import { useInventoryData } from '../data/hooks/useInventoryData'
import { usePricingData } from '../data/hooks/usePricingData'
import { useDisruptionsData } from '../data/hooks/useDisruptionsData'
import KPICard from '../components/ui/KPICard'
import SignalBadge from '../components/ui/SignalBadge'
import SectionHeader from '../components/ui/SectionHeader'
import DataTable from '../components/ui/DataTable'

function fmt(n) { return n?.toLocaleString() ?? '—' }
function fmtChange(n) {
  if (n === null || n === undefined) return '—'
  return (n > 0 ? '+' : '') + n.toLocaleString()
}

const CHART_TOOLTIP_STYLE = {
  backgroundColor: '#0C1220',
  border: '1px solid #1A2332',
  borderRadius: 6,
  color: '#E8E4DC',
  fontFamily: 'JetBrains Mono',
  fontSize: 12,
}

function SparklineCard({ title, data, dataKey, color, latest, change, onClick }) {
  // Use last 30 rows for the sparkline display
  const displayData = data.slice(-30)
  return (
    <div
      className="bg-bg-card border border-[#1A2332] rounded-lg p-4 group cursor-pointer hover:border-[#C87941]/50 transition-colors"
      onClick={onClick}
    >
      <div className="flex items-start justify-between mb-3">
        <div>
          <p className="text-txt-secondary text-xs font-body uppercase tracking-wider">{title}</p>
          <p className="text-txt-primary text-xl font-mono font-semibold mt-1">{fmt(latest)} <span className="text-sm text-txt-secondary">mt</span></p>
        </div>
        <div className="flex items-center gap-2">
          <span className={`text-xs font-mono ${change < 0 ? 'text-signal-bear' : change > 0 ? 'text-signal-bull' : 'text-txt-secondary'}`}>
            {fmtChange(change)} mt
          </span>
          <span className="text-txt-secondary text-xs opacity-0 group-hover:opacity-100 transition-opacity">↗</span>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={60}>
        <LineChart data={displayData} margin={{ top: 2, right: 2, left: 2, bottom: 2 }}>
          <Line type="monotone" dataKey={dataKey} stroke={color} strokeWidth={1.5} dot={false} />
          <Tooltip
            contentStyle={CHART_TOOLTIP_STYLE}
            formatter={(v) => [fmt(v) + ' mt', title]}
            labelFormatter={(l) => l}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

export default function Dashboard() {
  const navigate = useNavigate()
  const { data: rows, latest } = useDashboardData()
  const { comexHistory, lmeHistory, shfeHistory } = useInventoryData()
  const { benchmarks, tcrcHistory } = usePricingData()
  const { summary: disruptSummary } = useDisruptionsData()

  const latestTcrc = tcrcHistory[tcrcHistory.length - 1]
  const recent10 = rows.slice(-10).reverse()

  // Signal logic (thresholds from CLAUDE.md)
  const lmeCancelledSignal  = latest.lmeCancelledPct > 30 ? 'bull' : 'neutral'
  const combinedStocksSignal = latest.combinedTotal < 200000 ? 'bull' : latest.combinedTotal < 300000 ? 'neutral' : 'neutral'
  const tcrcSignal          = latestTcrc.tc < 20 ? 'bull' : 'neutral'
  const disruptionSignal    = disruptSummary.totalKtAtRisk > 100 ? 'bull' : 'neutral'

  const recentColumns = [
    { key: 'date',          label: 'Date',              align: 'left' },
    { key: 'comexTotal',    label: 'COMEX (mt)',         align: 'right', render: v => fmt(v) },
    { key: 'comexChange',   label: 'COMEX Δ',            align: 'right', render: v => <span className={v < 0 ? 'text-signal-bear' : v > 0 ? 'text-signal-bull' : ''}>{fmtChange(v)}</span> },
    { key: 'lmeTotal',      label: 'LME (mt)',           align: 'right', render: v => fmt(v) },
    { key: 'lmeChange',     label: 'LME Δ',              align: 'right', render: v => <span className={v < 0 ? 'text-signal-bear' : v > 0 ? 'text-signal-bull' : ''}>{fmtChange(v)}</span> },
    { key: 'shfeTotal',     label: 'SHFE (mt)',          align: 'right', render: v => fmt(v) },
    { key: 'shfeChange',    label: 'SHFE Δ',             align: 'right', render: v => <span className={v < 0 ? 'text-signal-bear' : v > 0 ? 'text-signal-bull' : ''}>{fmtChange(v)}</span> },
    { key: 'combinedTotal', label: 'Combined (mt)',      align: 'right', render: v => fmt(v) },
    { key: 'wowChange',     label: 'WoW Δ',              align: 'right', render: v => <span className={v < 0 ? 'text-signal-bear' : v > 0 ? 'text-signal-bull' : ''}>{fmtChange(v)}</span> },
  ]

  return (
    <div className="space-y-6">
      {/* Page title */}
      <div>
        <h1 className="text-txt-primary font-display font-bold text-2xl">Dashboard</h1>
        <p className="text-txt-secondary text-sm font-body mt-1">As of {latest.date} — static placeholder data</p>
      </div>

      {/* Row 1: KPI cards */}
      <div className="grid grid-cols-6 gap-4">
        <KPICard
          label="LME 3-Month"
          value={`$${benchmarks.lme3Month.toLocaleString()}`}
          unit="/mt"
          subtext="Static placeholder"
          onClick={() => navigate('/pricing')}
        />
        <KPICard
          label="COMEX Front"
          value={`$${benchmarks.comexFront.toFixed(3)}`}
          unit="/lb"
          subtext="Static placeholder"
          onClick={() => navigate('/pricing')}
        />
        <KPICard
          label="SHFE Front"
          value={`¥${benchmarks.shfeFront.toLocaleString()}`}
          unit="CNY/mt"
          subtext="Static placeholder"
          onClick={() => navigate('/pricing')}
        />
        <KPICard
          label="Combined Inventory"
          value={fmt(latest.combinedTotal)}
          unit="mt"
          changeValue={latest.combinedChange}
          changeUnit="mt"
        />
        <KPICard
          label="LME Cancelled %"
          value={`${latest.lmeCancelledPct.toFixed(1)}%`}
          changeValue={null}
          signal={lmeCancelledSignal}
          subtext={`${fmt(latest.lmeCancelledWarrants)} mt cancelled`}
        />
        <KPICard
          label="TC/RC Spot"
          value={`$${latestTcrc.tc.toFixed(1)}`}
          unit="/t"
          signal={tcrcSignal}
          subtext={`RC: ${latestTcrc.rc.toFixed(2)} ¢/lb`}
        />
      </div>

      {/* Row 2: Signal panel */}
      <div>
        <SectionHeader title="Market Signals" subtitle="Based on defined thresholds" />
        <div className="grid grid-cols-2 gap-3">
          <SignalBadge
            signal={lmeCancelledSignal}
            label={lmeCancelledSignal === 'bull' ? 'BULL' : 'NEUTRAL'}
            description={`LME cancelled warrants at ${latest.lmeCancelledPct.toFixed(1)}% (threshold: >30%). Physical metal being drawn from warehouses.`}
          />
          <SignalBadge
            signal={combinedStocksSignal}
            label={combinedStocksSignal === 'bull' ? 'BULL' : 'NEUTRAL'}
            description={`Combined 3-exchange stocks at ${fmt(latest.combinedTotal)} mt. Tight threshold: <200,000 mt.`}
          />
          <SignalBadge
            signal={tcrcSignal}
            label={tcrcSignal === 'bull' ? 'BULL' : 'NEUTRAL'}
            description={`TC/RC at $${latestTcrc.tc.toFixed(1)}/t — well below $20/t threshold. Concentrate shortage / refining bottleneck.`}
          />
          <SignalBadge
            signal={disruptionSignal}
            label={disruptionSignal === 'bull' ? 'BULL' : 'NEUTRAL'}
            description={`${fmt(disruptSummary.totalKtAtRisk)} kt annualized at risk from active disruptions (threshold: >100 kt).`}
          />
        </div>
      </div>

      {/* Row 3: Sparkline charts */}
      <div>
        <SectionHeader title="30-Day Inventory Trends" subtitle="Click a chart to view detailed inventory data" />
        <div className="grid grid-cols-3 gap-4">
          <SparklineCard
            title="COMEX"
            data={comexHistory}
            dataKey="total"
            color="#C87941"
            latest={latest.comexTotal}
            change={latest.comexChange}
            onClick={() => navigate('/inventories?tab=comex')}
          />
          <SparklineCard
            title="LME"
            data={lmeHistory}
            dataKey="total"
            color="#E8A76C"
            latest={latest.lmeTotal}
            change={latest.lmeChange}
            onClick={() => navigate('/inventories?tab=lme')}
          />
          <SparklineCard
            title="SHFE"
            data={shfeHistory}
            dataKey="total"
            color="#8B5A2B"
            latest={latest.shfeTotal}
            change={latest.shfeChange}
            onClick={() => navigate('/inventories?tab=shfe')}
          />
        </div>
      </div>

      {/* Row 4: Recent data table */}
      <div>
        <SectionHeader title="Recent Daily Data" subtitle="Last 10 trading days" />
        <DataTable columns={recentColumns} rows={recent10} maxHeight="360px" />
      </div>
    </div>
  )
}
