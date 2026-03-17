import { useState } from 'react'
import {
  LineChart, Line, AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, ReferenceLine,
} from 'recharts'
import { useInventoryData } from '../data/hooks/useInventoryData'
import ExchangeTab from '../components/ui/ExchangeTab'
import SectionHeader from '../components/ui/SectionHeader'
import DataTable from '../components/ui/DataTable'
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

export default function Inventories() {
  const [activeTab, setActiveTab] = useState('comex')
  const { comex, comexHistory, lme, lmeHistory, shfe, shfeHistory } = useInventoryData()

  // ---- COMEX warehouse table columns ----
  const warehouseColumns = [
    { key: 'name',       label: 'Warehouse',      align: 'left' },
    { key: 'registered', label: 'Registered (mt)', align: 'right', render: v => fmt(v) },
    { key: 'eligible',   label: 'Eligible (mt)',   align: 'right', render: v => fmt(v) },
    { key: 'total',      label: 'Total (mt)',       align: 'right', render: v => fmt(v) },
    { key: 'regPct',     label: 'Reg %',            align: 'right', render: v => `${v.toFixed(1)}%` },
  ]

  // ---- Combined top bar ----
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
            subtitle="7 licensed warehouses — registered vs eligible"
          />
          <DataTable columns={warehouseColumns} rows={comex.warehouses} />

          {/* Totals row */}
          <div className="grid grid-cols-3 gap-4">
            <KPICard label="Total Registered" value={fmt(comex.totalRegistered)} unit="mt" />
            <KPICard label="Total Eligible" value={fmt(comex.totalEligible)} unit="mt" />
            <KPICard label="Grand Total" value={fmt(comex.grandTotal)} unit="mt" subtext={`Reg: ${comex.regPct.toFixed(1)}%`} />
          </div>

          <SectionHeader title="COMEX 30-Day History" />
          <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
            <ResponsiveContainer width="100%" height={220}>
              <AreaChart data={comexHistory} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
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

          <SectionHeader title="LME 30-Day History" subtitle="Cancelled warrants indicate imminent physical withdrawal" />
          <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={lmeHistory} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
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
          <SectionHeader title="SHFE Regional Breakdown" subtitle="Shanghai, Guangdong, Jiangsu, Zhejiang" />
          <DataTable
            columns={[
              { key: 'name',  label: 'Region',     align: 'left' },
              { key: 'total', label: 'Total (mt)',  align: 'right', render: v => fmt(v) },
              { key: 'total', label: 'Share %',     align: 'right', render: (v) => `${((v / shfe.grandTotal) * 100).toFixed(1)}%` },
            ]}
            rows={[
              ...shfe.regions,
              { name: 'TOTAL', total: shfe.grandTotal },
            ]}
          />

          <SectionHeader title="SHFE 30-Day History" />
          <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4">
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={shfeHistory} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
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
