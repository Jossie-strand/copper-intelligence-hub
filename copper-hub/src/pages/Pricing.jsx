import {
  LineChart, Line, XAxis, YAxis, Tooltip, ReferenceLine,
  ResponsiveContainer, Legend,
} from 'recharts'
import { usePricingData } from '../data/hooks/usePricingData'
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

export default function Pricing() {
  const { forwardCurve, benchmarks, physicalPremiums, tcrcHistory, tcBearThreshold } = usePricingData()

  const latestTc = tcrcHistory[tcrcHistory.length - 1]

  const premiumColumns = [
    { key: 'region',   label: 'Region',        align: 'left' },
    { key: 'premium',  label: 'Premium ($/mt)', align: 'right', render: v => `+$${v}` },
    { key: 'weekAgo',  label: 'Week Ago',       align: 'right', render: v => `+$${v}` },
    {
      key: 'change',
      label: 'WoW',
      align: 'right',
      render: (v, row) => (
        <span className={v > 0 ? 'text-signal-bull' : v < 0 ? 'text-signal-bear' : 'text-txt-secondary'}>
          {v > 0 ? '▲' : v < 0 ? '▼' : '—'} {v > 0 ? '+' : ''}{v}
        </span>
      ),
    },
  ]

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-txt-primary font-display font-bold text-2xl">Pricing</h1>
        <p className="text-txt-secondary text-sm font-body mt-1">
          Static placeholder — Barchart API integration pending
        </p>
      </div>

      {/* Benchmark KPIs */}
      <div className="grid grid-cols-4 gap-4">
        <KPICard label="LME 3-Month" value={`$${benchmarks.lme3Month.toLocaleString()}`} unit="/mt" subtext="Placeholder" />
        <KPICard label="COMEX Front" value={`$${benchmarks.comexFront.toFixed(3)}`} unit="/lb" subtext="Placeholder" />
        <KPICard label="SHFE Front" value={`¥${benchmarks.shfeFront.toLocaleString()}`} unit="/mt" subtext="Placeholder" />
        <KPICard
          label="LME–COMEX Spread"
          value={`${benchmarks.lmeComexSpread > 0 ? '+' : ''}$${benchmarks.lmeComexSpread}`}
          unit="/mt"
          signal={benchmarks.lmeComexSpread > 0 ? 'bull' : 'bear'}
        />
      </div>

      {/* Forward curve */}
      <div>
        <SectionHeader title="LME Forward Curve" subtitle="Cash through M+15 — USD/mt" />
        <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={forwardCurve} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <XAxis dataKey="tenor" tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} />
              <YAxis
                tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }}
                domain={['auto', 'auto']}
                tickFormatter={v => '$' + v.toLocaleString()}
              />
              <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [`$${v.toLocaleString()}/mt`]} />
              <Line type="monotone" dataKey="price" stroke="#C87941" strokeWidth={2} dot={{ fill: '#C87941', r: 3 }} name="LME" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* TC/RC */}
      <div>
        <SectionHeader
          title="TC/RC History"
          subtitle={`18-month trend — $${tcBearThreshold}/t threshold (below = concentrate shortage = BULL)`}
          action={
            latestTc.tc < tcBearThreshold ? (
              <span className="text-xs font-mono text-signal-bull bg-signal-bull/15 px-2 py-1 rounded">
                ▲ BULL — TC at ${latestTc.tc.toFixed(1)}/t
              </span>
            ) : null
          }
        />
        <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={tcrcHistory} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <XAxis dataKey="month" tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} />
              <YAxis tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} tickFormatter={v => '$' + v} />
              <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [`$${v.toFixed(1)}/t`, 'TC']} />
              <ReferenceLine y={tcBearThreshold} stroke="#EF4444" strokeDasharray="4 2" label={{ value: `$${tcBearThreshold} threshold`, fill: '#EF4444', fontSize: 10, fontFamily: 'JetBrains Mono' }} />
              <Line type="monotone" dataKey="tc" stroke="#E8A76C" strokeWidth={2} dot={false} name="TC ($/t)" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Physical premiums */}
      <div>
        <SectionHeader title="Physical Premiums" subtitle="USD/mt over LME 3-Month" />
        <DataTable columns={premiumColumns} rows={physicalPremiums} />
      </div>
    </div>
  )
}
