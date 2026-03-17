import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
} from 'recharts'
import { useMinesData } from '../data/hooks/useMinesData'
import KPICard from '../components/ui/KPICard'
import SectionHeader from '../components/ui/SectionHeader'
import DataTable from '../components/ui/DataTable'

const TOOLTIP_STYLE = {
  backgroundColor: '#0C1220',
  border: '1px solid #1A2332',
  borderRadius: 6,
  color: '#E8E4DC',
  fontFamily: 'JetBrains Mono',
  fontSize: 11,
}

const STATUS_STYLES = {
  operating:   { bg: 'bg-signal-bull/15',    text: 'text-signal-bull',    label: 'Operating' },
  disrupted:   { bg: 'bg-signal-bear/15',    text: 'text-signal-bear',    label: 'Disrupted' },
  recovering:  { bg: 'bg-signal-neutral/15', text: 'text-signal-neutral', label: 'Recovering' },
  constrained: { bg: 'bg-signal-neutral/15', text: 'text-signal-neutral', label: 'Constrained' },
}

function StatusBadge({ status }) {
  const s = STATUS_STYLES[status] || STATUS_STYLES.operating
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-mono ${s.bg} ${s.text}`}>
      {s.label}
    </span>
  )
}

export default function GlobalMines() {
  const { mines, summary } = useMinesData()

  // Top 10 by H1 production for chart
  const top10 = [...mines]
    .filter(m => m.h1ProductionKt > 0)
    .sort((a, b) => b.h1ProductionKt - a.h1ProductionKt)
    .slice(0, 10)

  const mineColumns = [
    { key: 'name',               label: 'Mine',             align: 'left' },
    { key: 'country',            label: 'Country',          align: 'left' },
    { key: 'operator',           label: 'Operator',         align: 'left' },
    { key: 'annualCapacityKt',   label: 'Annual Cap (kt)',   align: 'right', render: v => v.toLocaleString() },
    { key: 'h1ProductionKt',     label: 'H1 Prod (kt)',      align: 'right', render: v => v > 0 ? v.toLocaleString() : '—' },
    { key: 'status',             label: 'Status',            align: 'center', render: v => <StatusBadge status={v} /> },
    { key: 'note',               label: 'Notes',             align: 'left',   render: v => <span className="text-txt-secondary text-xs">{v || '—'}</span> },
  ]

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-txt-primary font-display font-bold text-2xl">Global Mines</h1>
        <p className="text-txt-secondary text-sm font-body mt-1">20 major copper operations — static placeholder data</p>
      </div>

      {/* Summary KPIs */}
      <div className="grid grid-cols-4 gap-4">
        <KPICard label="Total Annual Capacity" value={summary.totalAnnualCapacityKt.toLocaleString()} unit="kt" />
        <KPICard label="Mines Disrupted" value={summary.minesDisrupted} signal={summary.minesDisrupted > 0 ? 'bear' : 'neutral'} />
        <KPICard label="Mines Constrained" value={summary.minesConstrained} signal={summary.minesConstrained > 0 ? 'neutral' : 'neutral'} />
        <KPICard label="Total Kt at Risk (ann.)" value={summary.ktAtRiskAnnualized.toLocaleString()} unit="kt" signal="bull" />
      </div>

      {/* Top 10 production chart */}
      <div>
        <SectionHeader title="Top 10 Mines by H1 2025 Production" subtitle="Thousand metric tons" />
        <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
          <ResponsiveContainer width="100%" height={260}>
            <BarChart
              data={top10}
              layout="vertical"
              margin={{ top: 5, right: 30, left: 140, bottom: 5 }}
            >
              <XAxis type="number" tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} tickFormatter={v => v + ' kt'} />
              <YAxis type="category" dataKey="name" tick={{ fill: '#E8E4DC', fontSize: 10, fontFamily: 'JetBrains Mono' }} width={130} />
              <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [`${v} kt`, 'H1 Production']} />
              <Bar dataKey="h1ProductionKt" radius={[0, 3, 3, 0]} name="H1 Production (kt)">
                {top10.map((m, i) => (
                  <Cell
                    key={i}
                    fill={m.status === 'disrupted' ? '#EF4444' : m.status === 'constrained' || m.status === 'recovering' ? '#F59E0B' : '#C87941'}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Full mine table */}
      <div>
        <SectionHeader title="All Mines" subtitle="20 operations" />
        <DataTable columns={mineColumns} rows={mines} maxHeight="480px" />
      </div>
    </div>
  )
}
