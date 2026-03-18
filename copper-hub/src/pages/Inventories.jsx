import { useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import {
  LineChart, Line, AreaChart, Area,
  XAxis, YAxis, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import { useInventoryData } from '../data/hooks/useInventoryData'
import ExchangeTab from '../components/ui/ExchangeTab'
import SectionHeader from '../components/ui/SectionHeader'
import KPICard from '../components/ui/KPICard'
import SignalBadge from '../components/ui/SignalBadge'

function fmt(n) { return n?.toLocaleString() ?? '—' }

const TOOLTIP_STYLE = {
  backgroundColor: '#0C1220',
  border: '1px solid #1A2332',
  borderRadius: 6,
  color: '#E8E4DC',
  fontFamily: 'JetBrains Mono',
  fontSize: 11,
}

const RANGES = ['30D', '3M', '6M', '1Y', '2Y']

// Returns cutoff date string based on range, relative to 2026-03-14
function rangeCutoff(range) {
  const cutoffs = {
    '30D': '2026-02-12',
    '3M':  '2025-12-14',
    '6M':  '2025-09-14',
    '1Y':  '2025-03-14',
    '2Y':  '2024-03-14',
  }
  return cutoffs[range] ?? '2026-02-12'
}

function filterByRange(history, range) {
  const cutoff = rangeCutoff(range)
  return history.filter(r => r.date >= cutoff)
}

function RangePicker({ value, onChange }) {
  return (
    <div className="flex gap-1">
      {RANGES.map(r => (
        <button
          key={r}
          onClick={() => onChange(r)}
          className={`px-3 py-1 text-xs font-mono rounded border transition-colors ${
            value === r
              ? 'bg-[#C87941] border-[#C87941] text-[#080D14] font-semibold'
              : 'bg-bg-card border-[#1A2332] text-txt-secondary hover:border-[#C87941]/50'
          }`}
        >
          {r}
        </button>
      ))}
    </div>
  )
}

function WarehouseAccordionTable({ warehouses }) {
  const [expanded, setExpanded] = useState(null)

  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg overflow-hidden">
      {/* Header */}
      <div className="grid grid-cols-[24px_1fr_120px_120px_120px_80px] gap-0 px-4 py-2 border-b border-[#1A2332] bg-[#080D14]">
        <div />
        <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider">Warehouse</div>
        <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider text-right">Registered</div>
        <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider text-right">Eligible</div>
        <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider text-right">Total</div>
        <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider text-right">Reg %</div>
      </div>

      {warehouses.map((wh) => {
        const isOpen = expanded === wh.name
        return (
          <div key={wh.name}>
            {/* Warehouse row */}
            <div
              className="grid grid-cols-[24px_1fr_120px_120px_120px_80px] gap-0 px-4 py-2.5 border-b border-[#1A2332] cursor-pointer hover:bg-[#0C1220] transition-colors"
              onClick={() => setExpanded(isOpen ? null : wh.name)}
            >
              <div className="text-txt-secondary text-xs font-mono flex items-center">
                {isOpen ? '▼' : '▶'}
              </div>
              <div className="text-txt-primary text-xs font-mono">{wh.name}</div>
              <div className="text-txt-primary text-xs font-mono text-right">{fmt(wh.registered)}</div>
              <div className="text-txt-primary text-xs font-mono text-right">{fmt(wh.eligible)}</div>
              <div className="text-txt-primary text-xs font-mono text-right font-semibold">{fmt(wh.total)}</div>
              <div className="text-txt-primary text-xs font-mono text-right">{wh.regPct.toFixed(1)}%</div>
            </div>

            {/* Expanded sub-table */}
            {isOpen && (
              <div className="bg-[#080D14] border-b border-[#1A2332]">
                <div className="grid grid-cols-[24px_1fr_120px_120px_120px] gap-0 px-4 py-1.5 border-b border-[#1A2332]/50">
                  <div />
                  <div className="text-[#C87941] text-xs font-mono uppercase tracking-wider">Week</div>
                  <div className="text-[#C87941] text-xs font-mono uppercase tracking-wider text-right">Registered</div>
                  <div className="text-[#C87941] text-xs font-mono uppercase tracking-wider text-right">Eligible</div>
                  <div className="text-[#C87941] text-xs font-mono uppercase tracking-wider text-right">Total</div>
                </div>
                {wh.history.map((row) => (
                  <div
                    key={row.week}
                    className="grid grid-cols-[24px_1fr_120px_120px_120px] gap-0 px-4 py-1.5 border-b border-[#1A2332]/30 last:border-b-0"
                  >
                    <div />
                    <div className="text-txt-secondary text-xs font-mono">{row.week}</div>
                    <div className="text-txt-secondary text-xs font-mono text-right">{fmt(row.registered)}</div>
                    <div className="text-txt-secondary text-xs font-mono text-right">{fmt(row.eligible)}</div>
                    <div className="text-txt-secondary text-xs font-mono text-right">{fmt(row.total)}</div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

function RegionAccordionTable({ regions, grandTotal }) {
  const [expanded, setExpanded] = useState(null)

  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg overflow-hidden">
      {/* Header */}
      <div className="grid grid-cols-[24px_1fr_120px_100px] gap-0 px-4 py-2 border-b border-[#1A2332] bg-[#080D14]">
        <div />
        <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider">Region</div>
        <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider text-right">Total (mt)</div>
        <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider text-right">Share %</div>
      </div>

      {regions.map((region) => {
        const isOpen = expanded === region.name
        return (
          <div key={region.name}>
            <div
              className="grid grid-cols-[24px_1fr_120px_100px] gap-0 px-4 py-2.5 border-b border-[#1A2332] cursor-pointer hover:bg-[#0C1220] transition-colors"
              onClick={() => setExpanded(isOpen ? null : region.name)}
            >
              <div className="text-txt-secondary text-xs font-mono flex items-center">
                {isOpen ? '▼' : '▶'}
              </div>
              <div className="text-txt-primary text-xs font-mono">{region.name}</div>
              <div className="text-txt-primary text-xs font-mono text-right font-semibold">{fmt(region.total)}</div>
              <div className="text-txt-primary text-xs font-mono text-right">
                {((region.total / grandTotal) * 100).toFixed(1)}%
              </div>
            </div>

            {isOpen && (
              <div className="bg-[#080D14] border-b border-[#1A2332]">
                <div className="grid grid-cols-[24px_1fr_120px] gap-0 px-4 py-1.5 border-b border-[#1A2332]/50">
                  <div />
                  <div className="text-[#C87941] text-xs font-mono uppercase tracking-wider">Week</div>
                  <div className="text-[#C87941] text-xs font-mono uppercase tracking-wider text-right">Total (mt)</div>
                </div>
                {region.history.map((row) => (
                  <div
                    key={row.week}
                    className="grid grid-cols-[24px_1fr_120px] gap-0 px-4 py-1.5 border-b border-[#1A2332]/30 last:border-b-0"
                  >
                    <div />
                    <div className="text-txt-secondary text-xs font-mono">{row.week}</div>
                    <div className="text-txt-secondary text-xs font-mono text-right">{fmt(row.total)}</div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )
      })}

      {/* Totals row */}
      <div className="grid grid-cols-[24px_1fr_120px_100px] gap-0 px-4 py-2.5 bg-[#080D14]">
        <div />
        <div className="text-txt-primary text-xs font-mono font-semibold">TOTAL</div>
        <div className="text-txt-primary text-xs font-mono text-right font-semibold">{fmt(grandTotal)}</div>
        <div className="text-txt-primary text-xs font-mono text-right">100.0%</div>
      </div>
    </div>
  )
}

export default function Inventories() {
  const [searchParams, setSearchParams] = useSearchParams()
  const activeTab = searchParams.get('tab') ?? 'comex'
  const setActiveTab = (tab) => setSearchParams({ tab })

  const [comexRange, setComexRange] = useState('30D')
  const [lmeRange, setLmeRange] = useState('30D')
  const [shfeRange, setShfeRange] = useState('30D')

  const { comex, comexHistory, lme, lmeHistory, shfe, shfeHistory } = useInventoryData()

  const filteredComex = filterByRange(comexHistory, comexRange)
  const filteredLme   = filterByRange(lmeHistory, lmeRange)
  const filteredShfe  = filterByRange(shfeHistory, shfeRange)

  const combinedTotal = comex.grandTotal + lme.total + shfe.grandTotal

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-txt-primary font-display font-bold text-2xl">Inventories</h1>
        <p className="text-txt-secondary text-sm font-body mt-1">Exchange warehouse stocks — as of 2026-03-14</p>
      </div>

      {/* Combined summary — always visible */}
      <div className="grid grid-cols-4 gap-4">
        <KPICard label="Combined Total" value={fmt(combinedTotal)} unit="mt" />
        <KPICard label="COMEX" value={fmt(comex.grandTotal)} unit="mt" subtext={`Reg: ${comex.regPct.toFixed(1)}%`} />
        <KPICard label="LME" value={fmt(lme.total)} unit="mt" subtext={`Cancelled: ${lme.cancelledPct.toFixed(1)}%`} signal={lme.cancelledPct > 30 ? 'bull' : 'neutral'} />
        <KPICard label="SHFE" value={fmt(shfe.grandTotal)} unit="mt" />
      </div>

      {/* Exchange tab switcher */}
      <div className="flex items-center gap-4">
        <ExchangeTab active={activeTab} onChange={setActiveTab} />
      </div>

      {/* ---- COMEX tab ---- */}
      {activeTab === 'comex' && (
        <div className="space-y-6">
          <SectionHeader
            title="COMEX Warehouse Breakdown"
            subtitle="7 licensed warehouses — click a row to expand weekly history"
          />
          <WarehouseAccordionTable warehouses={comex.warehouses} />

          {/* Totals row */}
          <div className="grid grid-cols-3 gap-4">
            <KPICard label="Total Registered" value={fmt(comex.totalRegistered)} unit="mt" />
            <KPICard label="Total Eligible" value={fmt(comex.totalEligible)} unit="mt" />
            <KPICard label="Grand Total" value={fmt(comex.grandTotal)} unit="mt" subtext={`Reg: ${comex.regPct.toFixed(1)}%`} />
          </div>

          <div className="flex items-center justify-between">
            <SectionHeader title="COMEX Inventory History" />
            <RangePicker value={comexRange} onChange={setComexRange} />
          </div>
          <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
            <ResponsiveContainer width="100%" height={220}>
              <AreaChart data={filteredComex} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                <XAxis dataKey="date" tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} tickFormatter={d => d.slice(5)} />
                <YAxis tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} tickFormatter={v => (v / 1000).toFixed(0) + 'k'} />
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [fmt(v) + ' mt']} />
                <Legend wrapperStyle={{ fontFamily: 'JetBrains Mono', fontSize: 11, color: '#9CA3AF' }} />
                <Area type="monotone" dataKey="registered" stackId="1" stroke="#C87941" fill="#C87941" fillOpacity={0.4} name="Registered" />
                <Area type="monotone" dataKey="eligible"   stackId="1" stroke="#8B5A2B" fill="#8B5A2B" fillOpacity={0.4} name="Eligible" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* ---- LME tab ---- */}
      {activeTab === 'lme' && (
        <div className="space-y-6">
          {lme.cancelledPct > 30 && (
            <div className="bg-signal-bull/10 border border-signal-bull/30 rounded-lg px-4 py-3">
              <p className="text-signal-bull text-sm font-mono font-semibold">
                ▲ BULL SIGNAL — LME cancelled warrants at {lme.cancelledPct.toFixed(1)}% (threshold: &gt;30%)
              </p>
              <p className="text-txt-secondary text-xs font-body mt-1">
                {fmt(lme.cancelledWarrants)} mt cancelled out of {fmt(lme.total)} mt total. Elevated physical demand signal.
              </p>
            </div>
          )}

          <div className="grid grid-cols-3 gap-4">
            <KPICard label="LME Total" value={fmt(lme.total)} unit="mt" />
            <KPICard label="Cancelled Warrants" value={fmt(lme.cancelledWarrants)} unit="mt" signal="bull" subtext="Manual entry — no feed yet" />
            <KPICard label="Cancelled %" value={`${lme.cancelledPct.toFixed(1)}%`} signal={lme.cancelledPct > 30 ? 'bull' : 'neutral'} subtext="Threshold: >30%" />
          </div>

          <div className="flex items-center justify-between">
            <SectionHeader title="LME Inventory History" subtitle="Cancelled warrants indicate imminent physical withdrawal" />
            <RangePicker value={lmeRange} onChange={setLmeRange} />
          </div>
          <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={filteredLme} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                <XAxis dataKey="date" tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} tickFormatter={d => d.slice(5)} />
                <YAxis tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} tickFormatter={v => (v / 1000).toFixed(0) + 'k'} />
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [fmt(v) + ' mt']} />
                <Legend wrapperStyle={{ fontFamily: 'JetBrains Mono', fontSize: 11, color: '#9CA3AF' }} />
                <Line type="monotone" dataKey="total" stroke="#E8A76C" strokeWidth={2} dot={false} name="Total" />
                <Line type="monotone" dataKey="cancelledWarrants" stroke="#EF4444" strokeWidth={1.5} dot={false} name="Cancelled" strokeDasharray="4 2" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* ---- SHFE tab ---- */}
      {activeTab === 'shfe' && (
        <div className="space-y-6">
          <SectionHeader title="SHFE Regional Breakdown" subtitle="Click a region to expand weekly history" />
          <RegionAccordionTable regions={shfe.regions} grandTotal={shfe.grandTotal} />

          <div className="flex items-center justify-between">
            <SectionHeader title="SHFE Inventory History" />
            <RangePicker value={shfeRange} onChange={setShfeRange} />
          </div>
          <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={filteredShfe} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                <XAxis dataKey="date" tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} tickFormatter={d => d.slice(5)} />
                <YAxis tick={{ fill: '#9CA3AF', fontSize: 10, fontFamily: 'JetBrains Mono' }} tickFormatter={v => (v / 1000).toFixed(0) + 'k'} />
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [fmt(v) + ' mt', 'SHFE Total']} />
                <Line type="monotone" dataKey="total" stroke="#8B5A2B" strokeWidth={2} dot={false} name="SHFE Total" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </div>
  )
}
