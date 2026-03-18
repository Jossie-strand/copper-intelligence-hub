import { useState } from 'react'
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, Tooltip, Legend,
  ResponsiveContainer, ReferenceLine, PieChart, Pie, Cell,
} from 'recharts'
import { useSupplyDemandData } from '../data/hooks/useSupplyDemandData'
import KPICard from '../components/ui/KPICard'
import SectionHeader from '../components/ui/SectionHeader'

const GROWTH_FORECASTS = [
  { sector: 'Electrical & Electronics', growth: '+6.2%', driver: 'grid investment + EV' },
  { sector: 'Construction',             growth: '+1.8%', driver: null },
  { sector: 'Consumer Products',        growth: '+2.4%', driver: null },
  { sector: 'Transport',               growth: '+8.5%', driver: 'EV acceleration' },
  { sector: 'Industrial Machinery',    growth: '+3.1%', driver: null },
]

const PRODUCTION_GROWTH = [
  { country: 'Chile',      growth: '+1.2%', driver: null },
  { country: 'Peru',       growth: '+3.5%', driver: 'Las Bambas recovery' },
  { country: 'DRC',        growth: '+12.8%', driver: 'Kamoa-Kakula ramp' },
  { country: 'China',      growth: '+2.1%', driver: null },
  { country: 'Indonesia',  growth: '-8.5%', driver: 'Grasberg disruption' },
  { country: 'USA',        growth: '+1.0%', driver: null },
  { country: 'Zambia',     growth: '+5.2%', driver: 'Sentinel expansion' },
  { country: 'Australia',  growth: '-2.3%', driver: 'Olympic Dam maintenance' },
  { country: 'Russia',     growth: '+0.8%', driver: null },
  { country: 'Kazakhstan', growth: '+1.5%', driver: null },
]

function growthColor(growth) {
  const val = parseFloat(growth)
  if (val > 5) return 'text-signal-bull'
  if (val >= 2) return 'text-signal-neutral'
  return 'text-[#C87941]'
}

function prodGrowthColor(growth) {
  const val = parseFloat(growth)
  return val >= 0 ? 'text-signal-bull' : 'text-signal-bear'
}

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

const COUNTRY_COLORS = ['#C87941','#E8A76C','#8B5A2B','#F59E0B','#22C55E','#EF4444','#9CA3AF','#60A5FA','#A78BFA','#34D399']

export default function SupplyDemand() {
  const { balance, countryProduction, demandSectors, chinaIndicators, narrative } = useSupplyDemandData()
  const [activeCountryIndex, setActiveCountryIndex] = useState(null)
  const [activeSectorIndex, setActiveSectorIndex] = useState(null)

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
            <div className="flex items-center gap-2">
              <div style={{ width: '52%', height: 200 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={countryProduction}
                      dataKey="pct"
                      nameKey="country"
                      cx="50%"
                      cy="50%"
                      innerRadius={55}
                      outerRadius={85}
                      paddingAngle={2}
                      onMouseEnter={(_, i) => setActiveCountryIndex(i)}
                      onMouseLeave={() => setActiveCountryIndex(null)}
                    >
                      {countryProduction.map((_, i) => (
                        <Cell key={i} fill={COUNTRY_COLORS[i % COUNTRY_COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip contentStyle={TOOLTIP_STYLE} itemStyle={{ color: '#E8E4DC' }} formatter={(v, n) => [`${v}%`, n]} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="flex flex-col gap-1" style={{ width: '48%' }}>
                {countryProduction.map((c, i) => (
                  <div key={i} className="flex items-center gap-1.5">
                    <span
                      className="rounded-full flex-shrink-0"
                      style={{
                        width: 8,
                        height: 8,
                        backgroundColor: activeCountryIndex === i ? '#E8A76C' : COUNTRY_COLORS[i % COUNTRY_COLORS.length],
                      }}
                    />
                    <span className={`font-mono text-[10px] truncate transition-colors ${activeCountryIndex === i ? 'text-txt-primary font-bold' : 'text-txt-secondary'}`}>
                      {c.country} {c.pct}%
                    </span>
                  </div>
                ))}
              </div>
            </div>
            <div className="border-t border-[#1A2332] mt-3 pt-3">
              <p className="text-txt-secondary text-[10px] font-mono uppercase tracking-wider mb-2">Production Growth 2026F</p>
              <div className="space-y-1">
                {PRODUCTION_GROWTH.map((row, i) => (
                  <div key={i} className="flex items-baseline justify-between gap-2">
                    <span className="text-txt-secondary font-mono text-[10px] truncate">{row.country}</span>
                    <div className="flex items-baseline gap-1.5 flex-shrink-0">
                      <span className={`font-mono text-[11px] font-semibold ${prodGrowthColor(row.growth)}`}>{row.growth}</span>
                      {row.driver && (
                        <span className="text-txt-secondary text-[9px] font-mono">({row.driver})</span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
              <p className="text-txt-secondary text-[10px] font-mono mt-2">2026F forecasts — ICSG/Cochilco estimates</p>
            </div>
          </div>
        </div>

        {/* Demand sectors */}
        <div>
          <SectionHeader title="Demand by End-Use Sector" subtitle="% of total refined demand" />
          <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
            <div className="flex items-center gap-2">
              <div style={{ width: '52%', height: 200 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={demandSectors}
                      dataKey="pct"
                      nameKey="sector"
                      cx="50%"
                      cy="50%"
                      innerRadius={55}
                      outerRadius={85}
                      paddingAngle={2}
                      onMouseEnter={(_, i) => setActiveSectorIndex(i)}
                      onMouseLeave={() => setActiveSectorIndex(null)}
                    >
                      {demandSectors.map((s, i) => (
                        <Cell key={i} fill={s.color} />
                      ))}
                    </Pie>
                    <Tooltip contentStyle={TOOLTIP_STYLE} itemStyle={{ color: '#E8E4DC' }} formatter={(v, n) => [`${v}%`, n]} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="flex flex-col gap-1" style={{ width: '48%' }}>
                {demandSectors.map((s, i) => (
                  <div key={i} className="flex items-center gap-1.5">
                    <span
                      className="rounded-full flex-shrink-0"
                      style={{
                        width: 8,
                        height: 8,
                        backgroundColor: activeSectorIndex === i ? '#E8A76C' : s.color,
                      }}
                    />
                    <span className={`font-mono text-[10px] truncate transition-colors ${activeSectorIndex === i ? 'text-txt-primary font-bold' : 'text-txt-secondary'}`}>
                      {s.sector} {s.pct}%
                    </span>
                  </div>
                ))}
              </div>
            </div>
            <div className="border-t border-[#1A2332] mt-3 pt-3">
              <p className="text-txt-secondary text-[10px] font-mono uppercase tracking-wider mb-2">Growth Forecast 2026F</p>
              <div className="space-y-1">
                {GROWTH_FORECASTS.map((row, i) => (
                  <div key={i} className="flex items-baseline justify-between gap-2">
                    <span className="text-txt-secondary font-mono text-[10px] truncate">{row.sector}</span>
                    <div className="flex items-baseline gap-1.5 flex-shrink-0">
                      <span className={`font-mono text-[11px] font-semibold ${growthColor(row.growth)}`}>{row.growth}</span>
                      {row.driver && (
                        <span className="text-txt-secondary text-[9px] font-mono">({row.driver})</span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
              <p className="text-txt-secondary text-[10px] font-mono mt-2">2026F forecasts — IEA/CRU estimates</p>
            </div>
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
