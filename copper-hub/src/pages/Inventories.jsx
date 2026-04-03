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

function fmt(n) { return n?.toLocaleString() ?? '—' }

const TOOLTIP_STYLE = {
  backgroundColor: '#0C1220',
  border: '1px solid #1A2332',
  borderRadius: 6,
  color: '#E8E4DC',
  fontFamily: 'JetBrains Mono',
  fontSize: 11,
}

const RANGES = ['30D', '3M', '6M', '1Y', 'ALL']

function rangeCutoff(range) {
  const now = new Date()
  const days = { '30D': 30, '3M': 90, '6M': 180, '1Y': 365, 'ALL': 9999 }
  const d = new Date(now.getTime() - (days[range] ?? 30) * 86400000)
  return d.toISOString().slice(0, 10)
}

function filterByRange(history, range) {
  if (range === 'ALL') return history
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

function AwaitingData({ label }) {
  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg px-4 py-8 text-center">
      <p className="text-txt-secondary text-sm font-mono">Awaiting {label} data</p>
      <p className="text-txt-secondary/60 text-xs font-body mt-1">Data will appear after the next successful feed run</p>
    </div>
  )
}

function WarehouseAccordionTable({ warehouses }) {
  const [expanded, setExpanded] = useState(null)

  if (!warehouses || warehouses.length === 0) return <AwaitingData label="COMEX warehouse" />

  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg overflow-hidden">
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
            <div
              className="grid grid-cols-[24px_1fr_120px_120px_120px_80px] gap-0 px-4 py-2.5 border-b border-[#1A2332] cursor-pointer hover:bg-[#0C1220] transition-colors"
              onClick={() => setExpanded(isOpen ? null : wh.name)}
            >
              <div className="text-txt-secondary text-xs font-mono flex items-center">
                {wh.history?.length > 0 ? (isOpen ? '▼' : '▶') : '·'}
              </div>
              <div className="text-txt-primary text-xs font-mono">{wh.name}</div>
              <div className="text-txt-primary text-xs font-mono text-right">{fmt(wh.registered)}</div>
              <div className="text-txt-primary text-xs font-mono text-right">{fmt(wh.eligible)}</div>
              <div className="text-txt-primary text-xs font-mono text-right font-semibold">{fmt(wh.total)}</div>
              <div className="text-txt-primary text-xs font-mono text-right">{(wh.regPct ?? 0).toFixed(1)}%</div>
            </div>

            {isOpen && wh.history?.length > 0 && (
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

  if (!regions || regions.length === 0) return <AwaitingData label="SHFE region" />

  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg overflow-hidden">
      <div className="grid grid-cols-[24px_1fr_120px_100px_100px] gap-0 px-4 py-2 border-b border-[#1A2332] bg-[#080D14]">
        <div />
        <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider">Region</div>
        <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider text-right">Total (mt)</div>
        <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider text-right">Change</div>
        <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider text-right">Share %</div>
      </div>

      {regions.map((region) => {
        const isOpen = expanded === region.name
        const share = grandTotal > 0 ? ((region.total / grandTotal) * 100).toFixed(1) : '0.0'
        return (
          <div key={region.name}>
            <div
              className="grid grid-cols-[24px_1fr_120px_100px_100px] gap-0 px-4 py-2.5 border-b border-[#1A2332] cursor-pointer hover:bg-[#0C1220] transition-colors"
              onClick={() => setExpanded(isOpen ? null : region.name)}
            >
              <div className="text-txt-secondary text-xs font-mono flex items-center">
                {region.history?.length > 0 ? (isOpen ? '▼' : '▶') : '·'}
              </div>
              <div className="text-txt-primary text-xs font-mono">{region.name}</div>
              <div className="text-txt-primary text-xs font-mono text-right font-semibold">{fmt(region.total)}</div>
              <div className={`text-xs font-mono text-right ${region.change < 0 ? 'text-signal-bear' : region.change > 0 ? 'text-signal-bull' : 'text-txt-secondary'}`}>
                {region.change > 0 ? '+' : ''}{fmt(region.change)}
              </div>
              <div className="text-txt-primary text-xs font-mono text-right">{share}%</div>
            </div>

            {isOpen && region.history?.length > 0 && (
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

      <div className="grid grid-cols-[24px_1fr_120px_100px_100px] gap-0 px-4 py-2.5 bg-[#080D14]">
        <div />
        <div className="text-txt-primary text-xs font-mono font-semibold">TOTAL</div>
        <div className="text-txt-primary text-xs font-mono text-right font-semibold">{fmt(grandTotal)}</div>
        <div />
        <div className="text-txt-primary text-xs font-mono text-right">100.0%</div>
      </div>
    </div>
  )
}

function inventorySignal(combinedTotal) {
  if (!combinedTotal || combinedTotal === 0) return null
  if (combinedTotal < 150000) return 'bear'
  if (combinedTotal < 200000) return 'neutral'
  return null
}

function inventoryLabel(combinedTotal) {
  if (!combinedTotal || combinedTotal === 0) return ''
  if (combinedTotal < 150000) return 'Very tight (<150kt)'
  if (combinedTotal < 200000) return 'Tight (<200kt)'
  return ''
}

export default function Inventories() {
  const [searchParams, setSearchParams] = useSearchParams()
  const activeTab = searchParams.get('tab') ?? 'comex'
  const setActiveTab = (tab) => setSearchParams({ tab })

  const [comexRange, setComexRange] = useState('ALL')
  const [lmeRange, setLmeRange]     = useState('ALL')
  const [shfeRange, setShfeRange]   = useState('ALL')

  const { comex, comexHistory, lme, lmeHistory, shfe, shfeHistory, loading, error } = useInventoryData()

  const filteredComex = filterByRange(comexHistory, comexRange)
  const filteredLme   = filterByRange(lmeHistory, lmeRange)
  const filteredShfe  = filterByRange(shfeHistory, shfeRange)

  const combinedTotal = (comex?.grandTotal || 0) + (lme?.total || 0) + (shfe?.grandTotal || 0)
  const latestDate = [comex?.date, lme?.date, shfe?.date].filter(Boolean).sort().pop() || '—'

  if (loading) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-txt-primary font-display font-bold text-2xl">Inventories</h1>
          <p className="text-txt-secondary text-sm font-body mt-1">Loading exchange data…</p>
        </div>
        <div className="flex items-center justify-center py-20">
          <span className="text-txt-secondary text-sm font-mono animate-pulse">Loading inventory data…</span>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-txt-primary font-display font-bold text-2xl">Inventories</h1>
        <p className="text-txt-secondary text-sm font-body mt-1">
          Exchange warehouse stocks — as of {latestDate}
          {error && <span className="text-signal-bear ml-2">· Error loading some data</span>}
        </p>
      </div>

      {/* Combined summary */}
      <div className="grid grid-cols-4 gap-4">
        <KPICard
          label="Combined Total"
          value={combinedTotal > 0 ? fmt(combinedTotal) : '—'}
          unit="mt"
          signal={inventorySignal(combinedTotal)}
          subtext={inventoryLabel(combinedTotal)}
        />
        <KPICard
          label="COMEX"
          value={comex?.grandTotal ? fmt(comex.grandTotal) : '—'}
          unit="mt"
          changeValue={comex?.change}
          changeUnit="mt"
          subtext={comex?.grandTotal ? `Reg: ${(comex.regPct ?? 0).toFixed(1)}%` : 'Awaiting data'}
        />
        <KPICard
          label="LME"
          value={lme?.total ? fmt(lme.total) : '—'}
          unit="mt"
          changeValue={lme?.change}
          changeUnit="mt"
          subtext={lme?.cancelledPct > 0 ? `Cancelled: ${lme.cancelledPct.toFixed(1)}%` : lme?.total ? '' : 'Awaiting data'}
          signal={lme?.cancelledPct > 30 ? 'bull' : null}
        />
        <KPICard
          label="SHFE"
          value={shfe?.grandTotal ? fmt(shfe.grandTotal) : '—'}
          unit="mt"
          changeValue={shfe?.change}
          changeUnit="mt"
          subtext={shfe?.date ? `${shfe.regions?.length || 0} regions` : 'Awaiting data'}
        />
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
            subtitle={comex?.date ? `Latest: ${comex.date} — 7 licensed warehouses` : 'Awaiting first feed run'}
          />
          <WarehouseAccordionTable warehouses={comex?.warehouses || []} />

          {comex?.grandTotal > 0 && (
            <div className="grid grid-cols-3 gap-4">
              <KPICard label="Total Registered" value={fmt(comex.totalRegistered)} unit="mt" />
              <KPICard label="Total Eligible" value={fmt(comex.totalEligible)} unit="mt" />
              <KPICard label="Grand Total" value={fmt(comex.grandTotal)} unit="mt" subtext={`Reg: ${(comex.regPct ?? 0).toFixed(1)}%`} />
            </div>
          )}

          {filteredComex.length > 1 && (
            <>
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
                    <Area type="monotone" dataKey="total" stroke="#C87941" fill="#C87941" fillOpacity={0.3} name="Total" />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </>
          )}

          {comex?.grandTotal === 0 && filteredComex.length === 0 && <AwaitingData label="COMEX inventory" />}
        </div>
      )}

      {/* ---- LME tab ---- */}
      {activeTab === 'lme' && (
        <div className="space-y-6">
          {lme?.cancelledPct > 30 && (
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
            <KPICard label="LME Total" value={lme?.total ? fmt(lme.total) : '—'} unit="mt" changeValue={lme?.change} changeUnit="mt" />
            <KPICard label="Cancelled Warrants" value={lme?.cancelledWarrants ? fmt(lme.cancelledWarrants) : '—'} unit="mt" subtext="Manual entry — no feed yet" />
            <KPICard label="Cancelled %" value={lme?.cancelledPct > 0 ? `${lme.cancelledPct.toFixed(1)}%` : '—'} signal={lme?.cancelledPct > 30 ? 'bull' : 'neutral'} subtext="Threshold: >30%" />
          </div>

          {filteredLme.length > 1 ? (
            <>
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
                    <Line type="monotone" dataKey="total" stroke="#E8A76C" strokeWidth={2} dot={false} name="Total" />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </>
          ) : (
            lme?.total > 0 && (
              <div className="bg-bg-card border border-[#1A2332] rounded-lg px-4 py-6 text-center">
                <p className="text-txt-secondary text-xs font-mono">Chart will appear after 2+ days of data</p>
              </div>
            )
          )}

          {!lme?.total && <AwaitingData label="LME inventory" />}
        </div>
      )}

      {/* ---- SHFE tab ---- */}
      {activeTab === 'shfe' && (
        <div className="space-y-6">
          <SectionHeader
            title="SHFE Regional Breakdown"
            subtitle={shfe?.date ? `Latest: ${shfe.date}` : 'Awaiting first feed run'}
          />
          <RegionAccordionTable regions={shfe?.regions || []} grandTotal={shfe?.grandTotal || 0} />

          {filteredShfe.length > 1 ? (
            <>
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
            </>
          ) : (
            shfe?.grandTotal > 0 && (
              <div className="bg-bg-card border border-[#1A2332] rounded-lg px-4 py-6 text-center">
                <p className="text-txt-secondary text-xs font-mono">Chart will appear after 2+ days of data</p>
              </div>
            )
          )}

          {!shfe?.grandTotal && <AwaitingData label="SHFE inventory" />}
        </div>
      )}
    </div>
  )
}
