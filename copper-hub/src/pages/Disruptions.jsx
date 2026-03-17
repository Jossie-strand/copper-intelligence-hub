import { useDisruptionsData } from '../data/hooks/useDisruptionsData'
import KPICard from '../components/ui/KPICard'
import SectionHeader from '../components/ui/SectionHeader'
import DataTable from '../components/ui/DataTable'

const SEVERITY_STYLES = {
  critical:    { bg: 'bg-signal-bear/20',    text: 'text-signal-bear',    label: 'Critical' },
  significant: { bg: 'bg-signal-neutral/15', text: 'text-signal-neutral', label: 'Significant' },
  moderate:    { bg: 'bg-signal-neutral/10', text: 'text-signal-neutral', label: 'Moderate' },
  minor:       { bg: 'bg-txt-secondary/10',  text: 'text-txt-secondary',  label: 'Minor' },
}

const TYPE_STYLES = {
  mine:      { bg: 'bg-signal-bear/15',    text: 'text-signal-bear',    label: 'Mine' },
  smelter:   { bg: 'bg-signal-neutral/15', text: 'text-signal-neutral', label: 'Smelter' },
  logistics: { bg: 'bg-blue-500/15',       text: 'text-blue-400',       label: 'Logistics' },
}

function Pill({ config, value }) {
  const s = config[value] || { bg: 'bg-txt-secondary/10', text: 'text-txt-secondary', label: value }
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-mono ${s.bg} ${s.text}`}>
      {s.label}
    </span>
  )
}

export default function Disruptions() {
  const { disruptions, summary, bullThresholdKt } = useDisruptionsData()

  // Sort by kt at risk descending
  const sorted = [...disruptions].sort((a, b) => b.ktAtRisk - a.ktAtRisk)

  const isBull = summary.totalKtAtRisk > bullThresholdKt

  const columns = [
    { key: 'name',               label: 'Event',                align: 'left',   render: (v, row) => <span className="font-semibold text-txt-primary">{v}</span> },
    { key: 'type',               label: 'Type',                 align: 'center', render: v => <Pill config={TYPE_STYLES} value={v} /> },
    { key: 'mine',               label: 'Mine / Exchange',       align: 'left',   render: v => <span className="text-txt-secondary text-xs">{v}</span> },
    { key: 'country',            label: 'Country',              align: 'left' },
    { key: 'ktAtRisk',           label: 'Kt at Risk (ann.)',     align: 'right',  render: v => v.toLocaleString() },
    { key: 'severity',           label: 'Severity',             align: 'center', render: v => <Pill config={SEVERITY_STYLES} value={v} /> },
    { key: 'expectedResolution', label: 'Expected Resolution',  align: 'left',   render: v => <span className="text-txt-secondary text-xs">{v}</span> },
    { key: 'ktLostToDate',       label: 'Kt Lost (cum.)',        align: 'right',  render: v => v.toLocaleString() },
    { key: 'source',             label: 'Source',               align: 'left',   render: v => <span className="text-txt-secondary text-xs">{v}</span> },
  ]

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-txt-primary font-display font-bold text-2xl">Disruptions</h1>
        <p className="text-txt-secondary text-sm font-body mt-1">Active supply disruption events — mines, smelters, logistics</p>
      </div>

      {/* Bull signal banner */}
      {isBull && (
        <div className="bg-signal-bull/10 border border-signal-bull/30 rounded-lg px-4 py-3">
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-signal-bull" />
            <span className="text-signal-bull text-sm font-mono font-bold">
              ▲ BULL SIGNAL — {summary.totalKtAtRisk.toLocaleString()} kt annualized at risk (threshold: &gt;{bullThresholdKt} kt)
            </span>
          </div>
          <p className="text-txt-secondary text-xs font-body mt-1">
            {summary.countBySeverity.critical} critical · {summary.countBySeverity.significant} significant · {summary.countBySeverity.moderate} moderate · {summary.countBySeverity.minor} minor
          </p>
        </div>
      )}

      {/* Summary KPIs */}
      <div className="grid grid-cols-5 gap-4">
        <KPICard label="Total Kt at Risk (ann.)" value={summary.totalKtAtRisk.toLocaleString()} unit="kt" signal="bull" />
        <KPICard label="Mine Disruptions" value={summary.countByType.mine} signal={summary.countByType.mine > 0 ? 'bear' : 'neutral'} />
        <KPICard label="Smelter Events" value={summary.countByType.smelter} signal={summary.countByType.smelter > 0 ? 'neutral' : 'neutral'} />
        <KPICard label="Logistics Events" value={summary.countByType.logistics} />
        <KPICard label="Critical Severity" value={summary.countBySeverity.critical} signal={summary.countBySeverity.critical > 0 ? 'bear' : 'neutral'} />
      </div>

      {/* Active events table */}
      <div>
        <SectionHeader title="Active Events" subtitle={`${sorted.length} events — sorted by kt at risk`} />
        <DataTable columns={columns} rows={sorted} />
      </div>

      {/* Notes panels for significant events */}
      <div>
        <SectionHeader title="Event Details" subtitle="Notes on significant disruptions" />
        <div className="space-y-3">
          {sorted.filter(d => d.severity === 'critical' || d.severity === 'significant').map((d, i) => (
            <div key={i} className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <Pill config={TYPE_STYLES} value={d.type} />
                <Pill config={SEVERITY_STYLES} value={d.severity} />
                <span className="text-txt-primary text-sm font-mono font-semibold">{d.name}</span>
                <span className="text-txt-secondary text-xs ml-auto">{d.country} — {d.operator}</span>
              </div>
              <p className="text-txt-secondary text-xs font-body leading-relaxed">{d.notes}</p>
              <div className="flex gap-6 mt-3">
                <span className="text-txt-secondary text-xs font-mono">Start: {d.startDate}</span>
                <span className="text-txt-secondary text-xs font-mono">Resolution: {d.expectedResolution}</span>
                <span className="text-txt-secondary text-xs font-mono">Last updated: {d.lastUpdated}</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
