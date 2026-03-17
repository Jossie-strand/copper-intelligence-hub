import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, Tooltip, Legend,
  ResponsiveContainer, ReferenceLine, PieChart, Pie, Cell,
} from 'recharts'
import { useSupplyDemandData } from '../data/hooks/useSupplyDemandData'
import KPICard from '../components/ui/KPICard'
import SectionHeader from '../components/ui/SectionHeader'

const TOOLTIP_STYLE = {
  backgroundColor: '#0C1220',
  border: '1px solid #1A2332',
  borderRadius: 6,
  color: '#E8E4DC',
  fontFamily: 'JetBrains Mono',
  fontSize: 11,
}

function PendingIndicator({ label }) {
  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4 flex items-center justify-between">
      <div>
        <p className="text-txt-secondary text-xs font-body uppercase tracking-wider mb-1">{label}</p>
        <p className="text-txt-secondary text-sm font-mono">Awaiting live data feed</p>
      </div>
      <span className="text-signal-neutral text-xs font-mono bg-signal-neutral/15 px-2 py-1 rounded">PENDING</span>
    </div>
  )
}

export default function SupplyDemand() {
  const { balance, countryProduction, demandSectors, chinaIndicators, narrative } = useSupplyDemandData()

  // Combine supply/demand/balance for bar+line combo
  const chartData = balance.map(r => ({ ...r, balanceAbs: Math.abs(r.balance) }))

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-txt-primary font-display font-bold text-2xl">Supply & Demand</h1>
        <p className="text-txt-secondary text-sm font-body mt-1">Global copper market balance — static placeholder data</p>
      </div>

      {/* Market narrative */}
      <div className="bg-signal-bull/10 border border-signal-bull/30 rounded-lg px-4 py-3">
        <div className="flex items-center gap-2 mb-1">
          <span className="w-2 h-2 rounded-full bg-signal-bull" />
          <span className="text-signal-bull text-xs font-mono font-bold tracking-widest">STRUCTURAL DEFICIT</span>
        </div>
        <p className="text-txt-secondary text-sm font-body">{narrative.summary}</p>
        <p className="text-txt-secondary text-xs font-mono mt-1">
          {narrative.year} deficit: ~{narrative.deficitKt.toLocaleString()} kt · {narrative.daysOfConsumption} days of consumption
        </p>
      </div>

      {/* S&D balance chart */}
      <div>
        <SectionHeader title="Supply vs Demand Balance" subtitle="Global refined copper — kt" />
        <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={balance} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
              <XAxis dataKey="year" tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} />
              <YAxis tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} tickFormatter={v => (v / 1000).toFixed(0) + 'k'} domain={[18000, 26000]} />
              <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [v.toLocaleString() + ' kt']} />
              <Legend wrapperStyle={{ fontFamily: 'JetBrains Mono', fontSize: 11, color: '#9CA3AF' }} />
              <Bar dataKey="supply" fill="#C87941" name="Supply (kt)" opacity={0.85} radius={[2, 2, 0, 0]} />
              <Bar dataKey="demand" fill="#8B5A2B" name="Demand (kt)" opacity={0.85} radius={[2, 2, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
          {/* Balance overlay */}
          <div className="mt-2 flex gap-4 flex-wrap">
            {balance.map(r => (
              <div key={r.year} className="text-center">
                <p className="text-txt-secondary text-xs font-mono">{r.year}</p>
                <p className={`text-xs font-mono font-semibold ${r.balance < 0 ? 'text-signal-bull' : 'text-signal-bear'}`}>
                  {r.balance > 0 ? '+' : ''}{r.balance.toLocaleString()} kt
                </p>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Two donuts side by side */}
      <div className="grid grid-cols-2 gap-6">
        {/* Country production */}
        <div>
          <SectionHeader title="Mine Production by Country" subtitle="% of global total" />
          <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
            <ResponsiveContainer width="100%" height={200}>
              <PieChart>
                <Pie
                  data={countryProduction}
                  dataKey="pct"
                  nameKey="country"
                  cx="40%"
                  cy="50%"
                  innerRadius={55}
                  outerRadius={85}
                  paddingAngle={2}
                >
                  {countryProduction.map((_, i) => (
                    <Cell
                      key={i}
                      fill={['#C87941','#E8A76C','#8B5A2B','#F59E0B','#22C55E','#EF4444','#9CA3AF','#60A5FA','#A78BFA','#34D399'][i % 10]}
                    />
                  ))}
                </Pie>
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v, n) => [`${v}%`, n]} />
                <Legend
                  layout="vertical"
                  align="right"
                  verticalAlign="middle"
                  wrapperStyle={{ fontFamily: 'JetBrains Mono', fontSize: 10, color: '#9CA3AF' }}
                  formatter={(v, entry) => `${v} ${entry.payload.pct}%`}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Demand sectors */}
        <div>
          <SectionHeader title="Demand by End-Use Sector" subtitle="% of total refined demand" />
          <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
            <ResponsiveContainer width="100%" height={200}>
              <PieChart>
                <Pie
                  data={demandSectors}
                  dataKey="pct"
                  nameKey="sector"
                  cx="40%"
                  cy="50%"
                  innerRadius={55}
                  outerRadius={85}
                  paddingAngle={2}
                >
                  {demandSectors.map((s, i) => (
                    <Cell key={i} fill={s.color} />
                  ))}
                </Pie>
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v, n) => [`${v}%`, n]} />
                <Legend
                  layout="vertical"
                  align="right"
                  verticalAlign="middle"
                  wrapperStyle={{ fontFamily: 'JetBrains Mono', fontSize: 10, color: '#9CA3AF' }}
                  formatter={(v, entry) => `${v} ${entry.payload.pct}%`}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* China indicators */}
      <div>
        <SectionHeader
          title="China Demand Proxy Indicators"
          subtitle="National Bureau of Statistics — live data feed pending"
        />
        <div className="grid grid-cols-2 gap-4">
          <PendingIndicator label="China Grid Investment YoY %" />
          <PendingIndicator label="China EV Sales MoM %" />
          <PendingIndicator label="Chinese Smelter Utilization %" />
          <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
            <p className="text-txt-secondary text-xs font-body uppercase tracking-wider mb-1">Market Balance (2026F)</p>
            <p className={`text-xl font-mono font-semibold ${narrative.balance < 0 ? 'text-signal-bull' : 'text-signal-bear'}`}>
              {narrative.deficitKt > 0 ? '-' : '+'}{narrative.deficitKt.toLocaleString()} kt
            </p>
            <p className="text-txt-secondary text-xs font-body mt-1">Structural deficit, trend: {narrative.trend}</p>
          </div>
        </div>
        {chinaIndicators.note && (
          <p className="text-txt-secondary text-xs font-body mt-3 border border-[#1A2332] rounded-lg px-4 py-3 bg-bg-card">
            {chinaIndicators.note}
          </p>
        )}
      </div>
    </div>
  )
}
