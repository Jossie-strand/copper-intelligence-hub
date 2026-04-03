import { useState } from 'react'

function fmtUSD(val) {
  const abs = Math.abs(val || 0)
  if (abs >= 1e9) return `$${(abs / 1e9).toFixed(1)}B`
  if (abs >= 1e6) return `$${(abs / 1e6).toFixed(0)}M`
  return '—'
}

function fmtNet(net) {
  const sign = net >= 0 ? '+' : '–'
  return sign + fmtUSD(Math.abs(net))
}

const COLS = [
  { key: 'rank',    label: '#',         align: 'left'  },
  { key: 'name',    label: 'Country',   align: 'left'  },
  { key: 'exports', label: 'Exports',   align: 'right' },
  { key: 'imports', label: 'Imports',   align: 'right' },
  { key: 'net',     label: 'Net',       align: 'right' },
]

export default function RankingsTable({ rankings, onCountryClick, selectedIso3 }) {
  const [tab, setTab]   = useState('exporters') // 'exporters' | 'importers'
  const [sortKey, setSortKey] = useState('exports')
  const [sortDir, setSortDir] = useState('desc')

  const rows = rankings?.[tab] || []

  const sorted = [...rows].sort((a, b) => {
    let av = a[sortKey], bv = b[sortKey]
    if (sortKey === 'rank') { av = a.exports; bv = b.exports } // rank = exports desc
    const mult = sortDir === 'asc' ? 1 : -1
    if (typeof av === 'string') return mult * av.localeCompare(bv)
    return mult * ((av || 0) - (bv || 0))
  })

  function handleSort(key) {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir('desc') }
  }

  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg overflow-hidden flex flex-col" style={{ height: 280 }}>
      {/* Tab bar */}
      <div className="flex border-b border-[#1A2332]">
        {[
          { id: 'exporters', label: 'Top Exporters' },
          { id: 'importers', label: 'Top Importers' },
        ].map(({ id, label }) => (
          <button
            key={id}
            onClick={() => setTab(id)}
            className={`flex-1 py-2.5 text-xs font-mono transition-colors ${
              tab === id
                ? 'text-copper border-b-2 border-copper bg-copper/5'
                : 'text-txt-secondary hover:text-txt-primary'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Table */}
      <div className="flex-1 overflow-y-auto">
        <table className="w-full text-xs font-mono border-collapse">
          <thead className="sticky top-0 bg-bg-primary">
            <tr className="border-b border-[#1A2332]">
              {COLS.map(col => (
                <th
                  key={col.key}
                  onClick={() => handleSort(col.key)}
                  className={`px-3 py-2 text-txt-secondary uppercase tracking-wider cursor-pointer select-none hover:text-txt-primary transition-colors whitespace-nowrap ${
                    col.align === 'right' ? 'text-right' : 'text-left'
                  }`}
                >
                  {col.label}
                  {sortKey === col.key && (
                    <span className="ml-1 text-copper">{sortDir === 'asc' ? '↑' : '↓'}</span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((row, i) => {
              const isSelected = row.iso3 === selectedIso3
              return (
                <tr
                  key={row.iso3}
                  onClick={() => onCountryClick?.({ iso3: row.iso3, name: row.name, witscode: row.witscode, data: row })}
                  className={`border-b border-[#1A2332] cursor-pointer transition-colors ${
                    isSelected
                      ? 'bg-copper/10'
                      : i % 2 === 0 ? 'hover:bg-white/5' : 'bg-bg-primary/30 hover:bg-white/5'
                  }`}
                >
                  <td className="px-3 py-2 text-txt-secondary">{i + 1}</td>
                  <td className="px-3 py-2 text-txt-primary font-medium whitespace-nowrap max-w-[120px] truncate">
                    {isSelected && <span className="mr-1 text-copper">›</span>}
                    {row.name}
                  </td>
                  <td className="px-3 py-2 text-right text-signal-bull tabular-nums">{fmtUSD(row.exports)}</td>
                  <td className="px-3 py-2 text-right text-signal-bear tabular-nums">{fmtUSD(row.imports)}</td>
                  <td className={`px-3 py-2 text-right tabular-nums ${row.net >= 0 ? 'text-signal-bull' : 'text-signal-bear'}`}>
                    {fmtNet(row.net)}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
