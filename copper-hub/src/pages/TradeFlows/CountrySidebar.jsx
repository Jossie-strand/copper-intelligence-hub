function fmtUSD(val) {
  const abs = Math.abs(val || 0)
  if (abs >= 1e9) return `$${(abs / 1e9).toFixed(2)}B`
  if (abs >= 1e6) return `$${(abs / 1e6).toFixed(0)}M`
  return `$${Math.round(abs).toLocaleString()}`
}

function fmtPct(val) {
  if (val == null || !isFinite(val)) return '—'
  return `${val >= 0 ? '+' : ''}${(val * 100).toFixed(1)}%`
}

function PartnerBar({ name, total, maxTotal, color }) {
  const pct = maxTotal > 0 ? (total / maxTotal) * 100 : 0
  return (
    <div>
      <div className="flex justify-between text-xs font-mono mb-0.5">
        <span className="text-txt-secondary truncate max-w-[140px]">{name}</span>
        <span style={{ color }}>{fmtUSD(total)}</span>
      </div>
      <div className="h-1 bg-[#1A2332] rounded-full overflow-hidden">
        <div className="h-full rounded-full transition-all" style={{ width: `${pct}%`, backgroundColor: color }} />
      </div>
    </div>
  )
}

export default function CountrySidebar({ country, partnerData, timeSeries, onClose }) {
  if (!country) return null

  const { name, data } = country
  const exports_ = data?.exports ?? 0
  const imports_ = data?.imports ?? 0
  const net      = data?.net ?? 0

  // YoY: compare last 2 years in timeSeries
  let yoyExports = null, yoyImports = null
  if (timeSeries && timeSeries.length >= 2) {
    const last    = timeSeries[timeSeries.length - 1]
    const prev    = timeSeries[timeSeries.length - 2]
    yoyExports = prev.exports > 0 ? (last.exports - prev.exports) / prev.exports : null
    yoyImports = prev.imports > 0 ? (last.imports - prev.imports) / prev.imports : null
  }

  const topExports = partnerData?.topExports || []
  const topImports = partnerData?.topImports || []
  const maxExport  = Math.max(...topExports.map(p => p.total), 1)
  const maxImport  = Math.max(...topImports.map(p => p.total), 1)

  return (
    <div className="w-72 flex-shrink-0 bg-bg-card border border-[#1A2332] rounded-lg flex flex-col overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-[#1A2332]">
        <div>
          <div className="text-txt-primary font-display font-bold text-base leading-tight">{name}</div>
          <div className="text-txt-secondary text-xs font-mono mt-0.5">{country.iso3} · 2024</div>
        </div>
        <button
          onClick={onClose}
          className="text-txt-secondary hover:text-txt-primary text-lg leading-none px-1"
        >
          ×
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-5">
        {/* KPI row */}
        <div className="grid grid-cols-3 gap-2">
          {[
            { label: 'Exports', value: fmtUSD(exports_), color: '#22C55E', yoy: yoyExports },
            { label: 'Imports', value: fmtUSD(imports_), color: '#EF4444', yoy: yoyImports },
            { label: 'Net',     value: (net >= 0 ? '+' : '–') + fmtUSD(Math.abs(net)),
              color: net >= 0 ? '#22C55E' : '#EF4444', yoy: null },
          ].map(({ label, value, color, yoy }) => (
            <div key={label} className="bg-bg-primary rounded p-2 text-center">
              <div className="text-txt-secondary text-xs font-mono mb-1">{label}</div>
              <div className="font-mono font-bold text-xs" style={{ color }}>{value}</div>
              {yoy != null && (
                <div className={`text-xs font-mono mt-0.5 ${yoy >= 0 ? 'text-signal-bull' : 'text-signal-bear'}`}>
                  {fmtPct(yoy)} YoY
                </div>
              )}
            </div>
          ))}
        </div>

        {/* Top export destinations */}
        {topExports.length > 0 && (
          <div>
            <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider mb-2">
              Top Export Destinations
            </div>
            <div className="space-y-2">
              {topExports.map(p => (
                <PartnerBar key={p.code} name={p.name} total={p.total} maxTotal={maxExport} color="#C87941" />
              ))}
            </div>
          </div>
        )}

        {/* Top import sources */}
        {topImports.length > 0 && (
          <div>
            <div className="text-txt-secondary text-xs font-mono uppercase tracking-wider mb-2">
              Top Import Sources
            </div>
            <div className="space-y-2">
              {topImports.map(p => (
                <PartnerBar key={p.code} name={p.name} total={p.total} maxTotal={maxImport} color="#38BDF8" />
              ))}
            </div>
          </div>
        )}

        {!data && (
          <div className="text-txt-secondary text-xs font-mono text-center py-4">
            No trade data available for this country.
          </div>
        )}
      </div>
    </div>
  )
}
