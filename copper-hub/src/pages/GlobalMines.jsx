import 'leaflet/dist/leaflet.css'
import { useState, useMemo, useRef, useEffect } from 'react'
import { MapContainer, TileLayer, CircleMarker, Tooltip, useMap } from 'react-leaflet'
import { useMinesData } from '../data/hooks/useMinesData'
import KPICard from '../components/ui/KPICard'
import SectionHeader from '../components/ui/SectionHeader'

const STATUS_STYLES = {
  operating:   { bg: 'bg-signal-bull/15',    text: 'text-signal-bull',    label: 'Operating' },
  disrupted:   { bg: 'bg-signal-bear/15',    text: 'text-signal-bear',    label: 'Disrupted' },
  recovering:  { bg: 'bg-signal-neutral/15', text: 'text-signal-neutral', label: 'Recovering' },
  constrained: { bg: 'bg-signal-neutral/15', text: 'text-signal-neutral', label: 'Constrained' },
}

function statusColor(status) {
  switch (status) {
    case 'operating':   return '#22C55E'
    case 'disrupted':   return '#EF4444'
    case 'recovering':
    case 'constrained': return '#F59E0B'
    default:            return '#9CA3AF'
  }
}

function StatusBadge({ status }) {
  const s = STATUS_STYLES[status] || STATUS_STYLES.operating
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-mono ${s.bg} ${s.text}`}>
      {s.label}
    </span>
  )
}

const MAX_CAPACITY = 1180 // Escondida

function dotRadius(annualCapacityKt) {
  return Math.max(6, Math.sqrt(annualCapacityKt / MAX_CAPACITY) * 14)
}

function MapController({ selectedCountry, mines }) {
  const map = useMap()
  useEffect(() => {
    if (!selectedCountry) {
      map.flyTo([20, 0], 2, { duration: 1 })
      return
    }
    const countryMines = mines.filter(m => m.country === selectedCountry)
    if (countryMines.length === 0) return
    const bounds = countryMines.map(m => [m.lat, m.lng])
    map.flyToBounds(bounds, { padding: [80, 80], maxZoom: 7, duration: 1 })
  }, [selectedCountry, map, mines])
  return null
}

const SORT_COLS = [
  { key: 'rank',             label: 'Rank' },
  { key: 'name',             label: 'Mine Name' },
  { key: 'country',          label: 'Country' },
  { key: 'operator',         label: 'Operator' },
  { key: 'annualCapacityKt', label: 'Annual Cap (kt)' },
  { key: 'status',           label: 'Status' },
]

export default function GlobalMines() {
  const { mines, summary } = useMinesData()

  const [selectedCountry, setSelectedCountry] = useState(null)
  const [selectedOperator, setSelectedOperator] = useState(null)
  const [sortKey, setSortKey] = useState('annualCapacityKt')
  const [sortDir, setSortDir] = useState('desc')
  const [activeMineName, setActiveMineName] = useState(null)

  const rowRefs = useRef({})

  const countries = useMemo(() =>
    [...new Set(mines.map(m => m.country))].sort(), [mines])

  const operators = useMemo(() =>
    [...new Set(mines.map(m => m.operator))].sort(), [mines])

  const filteredMines = useMemo(() => {
    let result = mines
    if (selectedCountry) result = result.filter(m => m.country === selectedCountry)
    if (selectedOperator) result = result.filter(m => m.operator === selectedOperator)

    return [...result].sort((a, b) => {
      let aVal = a[sortKey]
      let bVal = b[sortKey]
      if (sortKey === 'rank') {
        aVal = a.annualCapacityKt
        bVal = b.annualCapacityKt
        return bVal - aVal // rank always largest first
      }
      if (typeof aVal === 'string') {
        return sortDir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
      }
      return sortDir === 'asc' ? aVal - bVal : bVal - aVal
    })
  }, [mines, selectedCountry, selectedOperator, sortKey, sortDir])

  const maxCapacity = useMemo(() =>
    Math.max(...mines.map(m => m.annualCapacityKt)), [mines])

  function handleMineClick(mineName) {
    setActiveMineName(mineName)
    const el = rowRefs.current[mineName]
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' })
  }

  function handleSort(key) {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir(key === 'annualCapacityKt' || key === 'rank' ? 'desc' : 'asc')
    }
  }

  function clearFilters() {
    setSelectedCountry(null)
    setSelectedOperator(null)
  }

  const hasFilter = selectedCountry || selectedOperator

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-txt-primary font-display font-bold text-2xl">Global Mines</h1>
        <p className="text-txt-secondary text-sm font-body mt-1">20 major copper operations — static placeholder data</p>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-4">
        <KPICard label="Total Annual Capacity" value={summary.totalAnnualCapacityKt.toLocaleString()} unit="kt" />
        <KPICard label="Mines Disrupted" value={summary.minesDisrupted} signal={summary.minesDisrupted > 0 ? 'bear' : 'neutral'} />
        <KPICard label="Mines Constrained" value={summary.minesConstrained} signal={summary.minesConstrained > 0 ? 'neutral' : 'neutral'} />
        <KPICard label="Total Kt at Risk (ann.)" value={summary.ktAtRiskAnnualized.toLocaleString()} unit="kt" signal="bull" />
      </div>

      {/* Interactive World Map */}
      <div>
        <SectionHeader title="Global Mine Production Map" subtitle="Click a dot to jump to table row · dot size ∝ annual capacity" />
        <div className="bg-bg-card border border-[#1A2332] rounded-lg overflow-hidden" style={{ height: 480 }}>
          <MapContainer
            center={[20, 0]}
            zoom={2}
            minZoom={1}
            maxZoom={10}
            style={{ height: '100%', width: '100%', background: '#080D14' }}
            scrollWheelZoom={true}
          >
            <TileLayer
              url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
              attribution='&copy; <a href="https://carto.com/">CARTO</a>'
            />
            <MapController selectedCountry={selectedCountry} mines={mines} />
            {mines.map(mine => (
              <CircleMarker
                key={mine.name}
                center={[mine.lat, mine.lng]}
                radius={dotRadius(mine.annualCapacityKt)}
                pathOptions={{
                  fillColor: statusColor(mine.status),
                  fillOpacity: selectedOperator && mine.operator !== selectedOperator ? 0.2 : 0.85,
                  color: activeMineName === mine.name ? '#E8E4DC' : statusColor(mine.status),
                  weight: activeMineName === mine.name ? 2 : 1,
                  opacity: selectedOperator && mine.operator !== selectedOperator ? 0.25 : 1,
                }}
                eventHandlers={{ click: () => handleMineClick(mine.name) }}
              >
                <Tooltip direction="top" offset={[0, -8]}>
                  <div style={{ fontFamily: 'JetBrains Mono', fontSize: 11, lineHeight: 1.6, color: '#E8E4DC', background: '#0C1220', padding: '6px 10px', border: '1px solid #1A2332', borderRadius: 4, minWidth: 180 }}>
                    <div style={{ fontWeight: 700, color: '#C87941', marginBottom: 2 }}>{mine.name}</div>
                    <div>{mine.operator}</div>
                    <div style={{ color: '#9CA3AF' }}>{mine.country}</div>
                    <div style={{ marginTop: 4 }}>
                      <span style={{ color: '#9CA3AF' }}>Cap: </span>{mine.annualCapacityKt.toLocaleString()} kt/yr
                    </div>
                    <div>
                      <span style={{ color: statusColor(mine.status) }}>
                        {STATUS_STYLES[mine.status]?.label ?? mine.status}
                      </span>
                    </div>
                    {mine.note && <div style={{ color: '#9CA3AF', marginTop: 2, maxWidth: 200 }}>{mine.note}</div>}
                  </div>
                </Tooltip>
              </CircleMarker>
            ))}
          </MapContainer>
        </div>
      </div>

      {/* Filter Controls */}
      <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4 space-y-4">
        {/* Country filter */}
        <div>
          <p className="text-txt-secondary text-xs font-mono mb-2 uppercase tracking-wider">Filter by Country</p>
          <div className="flex flex-wrap gap-2">
            <button
              onClick={() => setSelectedCountry(null)}
              className={`px-3 py-1 rounded text-xs font-mono border transition-colors ${
                !selectedCountry
                  ? 'bg-copper border-copper text-bg-primary'
                  : 'bg-transparent border-[#1A2332] text-txt-secondary hover:border-copper hover:text-txt-primary'
              }`}
            >
              All
            </button>
            {countries.map(c => (
              <button
                key={c}
                onClick={() => setSelectedCountry(c === selectedCountry ? null : c)}
                className={`px-3 py-1 rounded text-xs font-mono border transition-colors ${
                  selectedCountry === c
                    ? 'bg-copper border-copper text-bg-primary'
                    : 'bg-transparent border-[#1A2332] text-txt-secondary hover:border-copper hover:text-txt-primary'
                }`}
              >
                {c}
              </button>
            ))}
          </div>
        </div>

        {/* Operator filter */}
        <div className="flex items-center gap-4">
          <div>
            <p className="text-txt-secondary text-xs font-mono mb-2 uppercase tracking-wider">Filter by Operator</p>
            <select
              value={selectedOperator ?? ''}
              onChange={e => setSelectedOperator(e.target.value || null)}
              className="bg-bg-primary border border-[#1A2332] text-txt-primary text-xs font-mono rounded px-3 py-1.5 focus:outline-none focus:border-copper"
            >
              <option value="">All operators</option>
              {operators.map(op => (
                <option key={op} value={op}>{op}</option>
              ))}
            </select>
          </div>

          {hasFilter && (
            <div className="flex items-end pb-0.5 gap-3">
              <span className="text-xs font-mono text-txt-secondary">
                {[selectedCountry, selectedOperator].filter(Boolean).join(' · ')}
              </span>
              <button
                onClick={clearFilters}
                className="text-xs font-mono text-copper hover:text-copper-light underline underline-offset-2"
              >
                Clear all
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Mines Table */}
      <div>
        <SectionHeader title="Mine Production Details" subtitle={`${filteredMines.length} mine${filteredMines.length !== 1 ? 's' : ''}`} />
        <div className="bg-bg-card border border-[#1A2332] rounded-lg overflow-hidden">
          <div className="overflow-y-auto" style={{ maxHeight: 520 }}>
            <table className="w-full text-xs font-mono border-collapse">
              <thead className="sticky top-0 bg-bg-primary z-10">
                <tr className="border-b border-[#1A2332]">
                  {SORT_COLS.map(col => (
                    <th
                      key={col.key}
                      onClick={() => handleSort(col.key)}
                      className="px-4 py-2.5 text-left text-txt-secondary uppercase tracking-wider cursor-pointer select-none hover:text-txt-primary transition-colors whitespace-nowrap"
                    >
                      {col.label}
                      {sortKey === col.key && (
                        <span className="ml-1 text-copper">{sortDir === 'asc' ? '↑' : '↓'}</span>
                      )}
                    </th>
                  ))}
                  <th className="px-4 py-2.5 text-left text-txt-secondary uppercase tracking-wider whitespace-nowrap">Capacity</th>
                  <th className="px-4 py-2.5 text-left text-txt-secondary uppercase tracking-wider">Notes</th>
                </tr>
              </thead>
              <tbody>
                {filteredMines.map((mine, i) => {
                  const isActive = activeMineName === mine.name
                  const barWidth = (mine.annualCapacityKt / maxCapacity) * 100
                  // compute rank by position in full sorted-by-capacity list
                  const globalRank = [...mines]
                    .sort((a, b) => b.annualCapacityKt - a.annualCapacityKt)
                    .findIndex(m => m.name === mine.name) + 1

                  return (
                    <tr
                      key={mine.name}
                      ref={el => { rowRefs.current[mine.name] = el }}
                      className={`border-b border-[#1A2332] transition-colors ${
                        isActive
                          ? 'bg-copper/10'
                          : i % 2 === 0 ? 'bg-transparent' : 'bg-bg-primary/30'
                      } hover:bg-copper/5`}
                    >
                      <td className="px-4 py-2.5 text-txt-secondary">{globalRank}</td>
                      <td className="px-4 py-2.5 text-txt-primary font-semibold whitespace-nowrap">{mine.name}</td>
                      <td className="px-4 py-2.5 text-txt-secondary whitespace-nowrap">{mine.country}</td>
                      <td className="px-4 py-2.5 text-txt-secondary whitespace-nowrap">{mine.operator}</td>
                      <td className="px-4 py-2.5 text-right text-txt-primary tabular-nums">{mine.annualCapacityKt.toLocaleString()}</td>
                      <td className="px-4 py-2.5">
                        <StatusBadge status={mine.status} />
                      </td>
                      {/* Capacity bar */}
                      <td className="px-4 py-2.5" style={{ minWidth: 120 }}>
                        <div className="flex items-center gap-2">
                          <div className="flex-1 bg-[#1A2332] rounded-full overflow-hidden" style={{ height: 6 }}>
                            <div
                              className="h-full rounded-full"
                              style={{ width: `${barWidth}%`, backgroundColor: '#C87941' }}
                            />
                          </div>
                          <span className="text-txt-secondary tabular-nums" style={{ minWidth: 40, textAlign: 'right' }}>
                            {mine.annualCapacityKt} kt
                          </span>
                        </div>
                      </td>
                      <td className="px-4 py-2.5 text-txt-secondary" style={{ maxWidth: 220 }}>
                        {mine.note || '—'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  )
}
