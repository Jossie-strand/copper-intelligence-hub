import { useState, useEffect, useCallback } from 'react'
import { useTradeFlowsData } from '../data/hooks/useTradeFlowsData'
import { ISO3_TO_WITS } from '../data/static/witsCountryCodes'
import TradeMap        from './TradeFlows/TradeMap'
import CountrySidebar  from './TradeFlows/CountrySidebar'
import TimeSeriesChart from './TradeFlows/TimeSeriesChart'
import RankingsTable   from './TradeFlows/RankingsTable'

const PRODUCTS = [
  { id: '260300', label: 'Ores & Concentrates', sub: 'HS 260300' },
  { id: '740311', label: 'Cathodes',            sub: 'HS 740311' },
  { id: 'both',   label: 'Both Combined',        sub: 'HS 260300 + 740311' },
]

export default function TradeFlows() {
  const [product, setProduct]               = useState('both')
  const [selectedCountry, setSelectedCountry] = useState(null)
  const [timeSeries, setTimeSeries]           = useState([])
  const [partnerData, setPartnerData]         = useState(null)

  const { netPositions, rankings, loading, fetchTimeSeries, fetchPartners } = useTradeFlowsData(product)

  // When a country is selected or product changes, fetch time series + partner data
  useEffect(() => {
    if (!selectedCountry?.iso3) { setTimeSeries([]); setPartnerData(null); return }
    const witscode = selectedCountry.witscode ?? ISO3_TO_WITS[selectedCountry.iso3]
    if (!witscode) return
    let cancelled = false

    fetchTimeSeries(witscode).then(data => {
      if (!cancelled) setTimeSeries(data)
    }).catch(console.error)

    fetchPartners(witscode).then(data => {
      if (!cancelled) setPartnerData(data)
    }).catch(console.error)

    return () => { cancelled = true }
  }, [selectedCountry?.iso3, fetchTimeSeries, fetchPartners])

  const handleCountryClick = useCallback((country) => {
    setSelectedCountry(prev => prev?.iso3 === country.iso3 ? null : country)
  }, [])

  return (
    <div className="space-y-4">
      {/* Page header */}
      <div className="flex items-end justify-between">
        <div>
          <h1 className="text-txt-primary font-display font-bold text-2xl">Trade Flows</h1>
          <p className="text-txt-secondary text-sm font-body mt-1">
            Global copper trade · 1988–2024 · UN Comtrade / WITS
          </p>
        </div>
        {loading && (
          <span className="text-txt-secondary text-xs font-mono animate-pulse">Loading trade data…</span>
        )}
      </div>

      {/* Product toggle */}
      <div className="flex gap-1 bg-bg-card border border-[#1A2332] rounded-lg p-1 w-fit">
        {PRODUCTS.map(p => (
          <button
            key={p.id}
            onClick={() => setProduct(p.id)}
            className={`px-4 py-2 rounded text-sm transition-colors ${
              product === p.id
                ? 'bg-copper text-bg-primary font-medium font-mono'
                : 'text-txt-secondary hover:text-txt-primary font-mono'
            }`}
          >
            {p.label}
            <span className={`ml-1.5 text-xs ${product === p.id ? 'text-bg-primary/70' : 'text-txt-secondary/60'}`}>
              {p.sub}
            </span>
          </button>
        ))}
      </div>

      {/* Map + Sidebar row */}
      <div className="flex gap-4 items-start">
        <div className="flex-1 min-w-0">
          <TradeMap
            netPositions={netPositions}
            selectedIso3={selectedCountry?.iso3 || null}
            onCountryClick={handleCountryClick}
            fetchPartners={fetchPartners}
          />
        </div>
        {selectedCountry && (
          <CountrySidebar
            country={selectedCountry}
            partnerData={partnerData}
            timeSeries={timeSeries}
            onClose={() => setSelectedCountry(null)}
          />
        )}
      </div>

      {/* Bottom row: time series + rankings */}
      <div className="grid grid-cols-2 gap-4">
        <TimeSeriesChart data={timeSeries} country={selectedCountry} />
        <RankingsTable
          rankings={rankings}
          selectedIso3={selectedCountry?.iso3 || null}
          onCountryClick={handleCountryClick}
        />
      </div>
    </div>
  )
}
