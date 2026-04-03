import {
  AreaChart, Area, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, Tooltip, ResponsiveContainer,
} from 'recharts'
import { useNavigate, Link } from 'react-router-dom'
import { useDashboardData } from '../data/hooks/useDashboardData'
import KPICard from '../components/ui/KPICard'
import SignalBadge from '../components/ui/SignalBadge'
import SectionHeader from '../components/ui/SectionHeader'

function fmt(n) { return n?.toLocaleString() ?? '—' }
function fmtB(n) {
  if (n == null) return '—'
  const abs = Math.abs(n)
  if (abs >= 1e12) return `$${(n / 1e12).toFixed(1)}T`
  if (abs >= 1e9)  return `$${(n / 1e9).toFixed(1)}B`
  if (abs >= 1e6)  return `$${(n / 1e6).toFixed(0)}M`
  return `$${Math.round(n).toLocaleString()}`
}

const TT = {
  backgroundColor: '#0C1220', border: '1px solid #1A2332',
  borderRadius: 6, color: '#E8E4DC', fontFamily: 'JetBrains Mono', fontSize: 11,
}

const DONUT_COLORS = ['#C87941', '#E8A76C', '#8B5A2B', '#38BDF8', '#22C55E', '#1A2332']

// ── Section 1: Inventory KPI Cards ────────────────────────────────────────
function InventoryKPIs({ inv, signal, navigate }) {
  if (!inv) {
    return (
      <div className="grid grid-cols-5 gap-4">
        {[...Array(5)].map((_, i) => (
          <div key={i} className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
            <p className="text-txt-secondary text-xs font-mono animate-pulse">Loading…</p>
          </div>
        ))}
      </div>
    )
  }

  const combined = inv.combinedTotal || 0
  const invSignal = combined < 150000 ? 'bear' : combined < 200000 ? 'neutral' : null
  const invLabel  = combined < 150000 ? 'VERY TIGHT' : combined < 200000 ? 'TIGHT' : 'NORMAL'
  const lmeSig    = (inv.lmeCancelledPct ?? 0) > 30 ? 'bull' : null

  return (
    <div className="grid grid-cols-5 gap-4">
      <KPICard
        label="COMEX"
        value={inv.comexTotal != null ? fmt(inv.comexTotal) : '—'}
        unit="mt"
        changeValue={inv.comexChange}
        changeUnit="mt"
        subtext={inv.comexTotal == null ? 'Awaiting data' : ''}
        onClick={() => navigate('/inventories?tab=comex')}
      />
      <KPICard
        label="LME"
        value={inv.lmeTotal != null ? fmt(inv.lmeTotal) : '—'}
        unit="mt"
        changeValue={inv.lmeChange}
        changeUnit="mt"
        subtext={inv.lmeTotal == null ? 'Awaiting data' : ''}
        onClick={() => navigate('/inventories?tab=lme')}
      />
      <KPICard
        label="SHFE"
        value={inv.shfeTotal != null ? fmt(inv.shfeTotal) : '—'}
        unit="mt"
        changeValue={inv.shfeChange}
        changeUnit="mt"
        subtext={inv.shfeTotal == null ? 'Awaiting data' : ''}
        onClick={() => navigate('/inventories?tab=shfe')}
      />
      <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4 cursor-pointer hover:border-[#C87941]/50 transition-colors" onClick={() => navigate('/inventories')}>
        <p className="text-txt-secondary text-xs font-body uppercase tracking-wider mb-2">Combined Total</p>
        <div className="flex items-end gap-2">
          <span className="text-txt-primary text-2xl font-mono font-semibold leading-none">{combined > 0 ? fmt(combined) : '—'}</span>
          <span className="text-txt-secondary text-sm font-mono mb-0.5">mt</span>
        </div>
        {combined > 0 && (
          <div className="mt-2">
            <SignalBadge signal={invSignal || 'neutral'} label={invLabel} size="sm" />
          </div>
        )}
      </div>
      <KPICard
        label="LME Cancelled %"
        value={inv.lmeCancelledPct != null ? `${inv.lmeCancelledPct.toFixed(1)}%` : '—'}
        signal={lmeSig}
        subtext={lmeSig ? 'Physical tightness' : inv.lmeCancelledPct != null ? '' : 'Awaiting data'}
      />
    </div>
  )
}

// ── Section 2: Inventory Trend ────────────────────────────────────────────
function InventoryTrend({ summary }) {
  const chartData = summary.slice(-30).map(r => ({
    date: r.data_date,
    comex: Number(r.comex_total) || 0,
    lme:   Number(r.lme_total)   || 0,
    shfe:  Number(r.shfe_total)  || 0,
  }))

  if (chartData.length < 2) {
    return (
      <div className="bg-bg-card border border-[#1A2332] rounded-lg px-4 py-8 text-center">
        <p className="text-txt-secondary text-sm font-mono">Inventory chart will appear after 2+ days of data</p>
      </div>
    )
  }

  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
      <ResponsiveContainer width="100%" height={180}>
        <AreaChart data={chartData} margin={{ top: 5, right: 5, left: 0, bottom: 0 }}>
          <XAxis dataKey="date" tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} tickFormatter={d => d.slice(5)} />
          <YAxis tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} tickFormatter={v => (v / 1000).toFixed(0) + 'k'} />
          <Tooltip contentStyle={TT} formatter={v => [fmt(v) + ' mt']} />
          <Area type="monotone" dataKey="shfe"  stackId="1" stroke="#8B5A2B" fill="#8B5A2B" fillOpacity={0.5} name="SHFE" />
          <Area type="monotone" dataKey="lme"   stackId="1" stroke="#E8A76C" fill="#E8A76C" fillOpacity={0.5} name="LME" />
          <Area type="monotone" dataKey="comex" stackId="1" stroke="#C87941" fill="#C87941" fillOpacity={0.5} name="COMEX" />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}

// ── Section 3A: Top Copper Corridors ──────────────────────────────────────
function TopCorridors({ corridors, navigate }) {
  if (!corridors.length) return null
  const maxVal = corridors[0]?.total || 1

  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
      <h3 className="text-txt-primary font-display font-semibold text-sm mb-3">Top Copper Corridors</h3>
      <div className="space-y-2">
        {corridors.map((c, i) => (
          <div key={i} className="group cursor-pointer" onClick={() => navigate('/trade-flows')}>
            <div className="flex items-center justify-between text-xs font-mono mb-0.5">
              <span className="text-txt-primary group-hover:text-copper transition-colors truncate max-w-[200px]">
                {c.flow}
              </span>
              <span className="text-copper-light ml-2 flex-shrink-0">{fmtB(c.total)}</span>
            </div>
            <div className="h-1 bg-[#1A2332] rounded-full overflow-hidden">
              <div
                className="h-full rounded-full bg-copper transition-all"
                style={{ width: `${(c.total / maxVal) * 100}%` }}
              />
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Section 3B: China Import Concentration ────────────────────────────────
function ChinaImports({ imports }) {
  if (!imports.length) return null

  const totalAll = imports.reduce((s, c) => s + c.total, 0)
  const top5 = imports.slice(0, 5)
  const otherTotal = imports.slice(5).reduce((s, c) => s + c.total, 0)
  const top3Pct = totalAll > 0
    ? ((imports.slice(0, 3).reduce((s, c) => s + c.total, 0) / totalAll) * 100).toFixed(0)
    : 0

  const pieData = [
    ...top5.map((c, i) => ({ name: c.name, value: c.total, color: DONUT_COLORS[i] })),
    ...(otherTotal > 0 ? [{ name: 'Other', value: otherTotal, color: DONUT_COLORS[5] }] : []),
  ]

  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
      <h3 className="text-txt-primary font-display font-semibold text-sm mb-3">China Import Sources</h3>
      <div className="flex items-center gap-4">
        <div className="relative" style={{ width: 140, height: 140 }}>
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={pieData}
                cx="50%" cy="50%"
                innerRadius={38} outerRadius={62}
                dataKey="value"
                stroke="#080D14"
                strokeWidth={1}
              >
                {pieData.map((d, i) => <Cell key={i} fill={d.color} />)}
              </Pie>
              <Tooltip contentStyle={TT} formatter={v => [fmtB(v)]} />
            </PieChart>
          </ResponsiveContainer>
          <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
            <span className="text-txt-primary text-sm font-mono font-bold">{top3Pct}%</span>
            <span className="text-txt-secondary text-[9px] font-mono">Top 3</span>
          </div>
        </div>
        <div className="flex-1 space-y-1.5">
          {pieData.map((d, i) => (
            <div key={i} className="flex items-center gap-2 text-xs font-mono">
              <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: d.color }} />
              <span className="text-txt-secondary truncate flex-1">{d.name}</span>
              <span className="text-txt-primary">{fmtB(d.value)}</span>
            </div>
          ))}
          {Number(top3Pct) > 70 && (
            <div className="mt-2">
              <SignalBadge signal="neutral" label="HIGH CONCENTRATION" size="sm" />
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Section 3C: YoY Deltas ────────────────────────────────────────────────
function YoYDeltas({ deltas }) {
  if (!deltas.length) return null

  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
      <h3 className="text-txt-primary font-display font-semibold text-sm mb-3">Trade Flow Shifts — YoY</h3>
      <div className="space-y-2.5">
        {deltas.map((d, i) => {
          const up = d.changePct > 0
          return (
            <div key={i} className="flex items-center justify-between">
              <span className="text-txt-secondary text-xs font-mono truncate max-w-[200px]">{d.flow}</span>
              <span className={`text-xs font-mono font-semibold flex-shrink-0 ml-2 ${up ? 'text-signal-bull' : 'text-signal-bear'}`}>
                {up ? '+' : ''}{d.changePct.toFixed(0)}% YoY
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ── Section 3D: Global Growth ─────────────────────────────────────────────
function GlobalGrowth({ growth, navigate }) {
  if (!growth) return null

  const sparkData = growth.years.slice(-10)

  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
      <h3 className="text-txt-primary font-display font-semibold text-sm mb-3">Copper Trade Growth</h3>
      <div className="grid grid-cols-2 gap-3 mb-3">
        <div>
          <p className="text-txt-secondary text-[10px] font-mono uppercase tracking-wider">
            {growth.latestYear} Total
          </p>
          <p className="text-txt-primary text-lg font-mono font-bold">{fmtB(growth.latestTotal)}</p>
        </div>
        <div>
          <p className="text-txt-secondary text-[10px] font-mono uppercase tracking-wider">
            {growth.baseYear}–{growth.latestYear} CAGR
          </p>
          <p className={`text-lg font-mono font-bold ${growth.cagr >= 0 ? 'text-signal-bull' : 'text-signal-bear'}`}>
            {growth.cagr >= 0 ? '+' : ''}{growth.cagr.toFixed(1)}%
          </p>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={60}>
        <LineChart data={sparkData} margin={{ top: 2, right: 2, left: 2, bottom: 2 }}>
          <Line type="monotone" dataKey="total" stroke="#C87941" strokeWidth={1.5} dot={false} />
          <Tooltip contentStyle={TT} formatter={v => [fmtB(v), 'Global Exports']} labelFormatter={l => `${l}`} />
        </LineChart>
      </ResponsiveContainer>
      <div className="mt-3 text-right">
        <Link to="/trade-flows" className="text-copper text-xs font-mono hover:text-copper-light transition-colors">
          View Trade Flows →
        </Link>
      </div>
    </div>
  )
}

// ── Main Dashboard ────────────────────────────────────────────────────────
export default function Dashboard() {
  const navigate = useNavigate()
  const {
    inventorySummary, latestInventory, signal,
    topCorridors, chinaImports, yoyDeltas, globalGrowth,
    loading,
  } = useDashboardData()

  const latestDate = latestInventory?.date || '—'

  if (loading) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-txt-primary font-display font-bold text-2xl">Dashboard</h1>
          <p className="text-txt-secondary text-sm font-body mt-1">Loading…</p>
        </div>
        <div className="flex items-center justify-center py-20">
          <span className="text-txt-secondary text-sm font-mono animate-pulse">Loading dashboard data…</span>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-txt-primary font-display font-bold text-2xl">Dashboard</h1>
        <p className="text-txt-secondary text-sm font-body mt-1">
          Copper market intelligence — as of {latestDate}
        </p>
      </div>

      {/* Section 1: Live Inventory KPI Cards */}
      <InventoryKPIs inv={latestInventory} signal={signal} navigate={navigate} />

      {/* Inventory signal banner */}
      {signal && signal.inventory_signal !== 'normal' && (
        <SignalBadge
          signal={signal.inventory_signal === 'very_tight' ? 'bear' : 'neutral'}
          label={signal.inventory_signal === 'very_tight' ? 'VERY TIGHT' : 'TIGHT'}
          description={`Combined 3-exchange stocks at ${fmt(Number(signal.combined_total))} mt. ${
            signal.inventory_signal === 'very_tight'
              ? 'Below 150,000 mt — historically very tight.'
              : 'Below 200,000 mt — historically tight.'
          }`}
        />
      )}

      {/* Section 2: Inventory Trend */}
      <div>
        <SectionHeader
          title="Combined Inventory Trend"
          subtitle="COMEX + LME + SHFE stacked · last 30 days"
          action={
            <Link to="/inventories" className="text-copper text-xs font-mono hover:text-copper-light transition-colors">
              View Inventories →
            </Link>
          }
        />
        <InventoryTrend summary={inventorySummary} />
      </div>

      {/* Section 3: Trade Flow Intelligence */}
      <div>
        <SectionHeader
          title="Trade Flow Intelligence"
          subtitle="Bilateral copper trade · UN Comtrade"
          action={
            <Link to="/trade-flows" className="text-copper text-xs font-mono hover:text-copper-light transition-colors">
              View Trade Flows →
            </Link>
          }
        />
        <div className="grid grid-cols-2 gap-4">
          <TopCorridors corridors={topCorridors} navigate={navigate} />
          <ChinaImports imports={chinaImports} />
          <YoYDeltas deltas={yoyDeltas} />
          <GlobalGrowth growth={globalGrowth} navigate={navigate} />
        </div>
      </div>
    </div>
  )
}
