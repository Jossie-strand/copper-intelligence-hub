import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts'

function fmtBn(val) {
  if (val == null) return ''
  return `$${(val / 1e9).toFixed(1)}B`
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div
      className="bg-bg-card border border-[#1A2332] rounded px-3 py-2 shadow-lg"
      style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 11 }}
    >
      <div className="text-txt-secondary mb-1">{label}</div>
      {payload.map(p => (
        <div key={p.dataKey} style={{ color: p.color }}>
          {p.name}: {fmtBn(p.value)}
        </div>
      ))}
    </div>
  )
}

export default function TimeSeriesChart({ data, country }) {
  if (!country) {
    return (
      <div className="bg-bg-card border border-[#1A2332] rounded-lg flex items-center justify-center" style={{ height: 280 }}>
        <span className="text-txt-secondary text-sm font-mono">← Select a country on the map</span>
      </div>
    )
  }

  if (!data || data.length === 0) {
    return (
      <div className="bg-bg-card border border-[#1A2332] rounded-lg flex items-center justify-center" style={{ height: 280 }}>
        <span className="text-txt-secondary text-sm font-mono">Loading time series…</span>
      </div>
    )
  }

  return (
    <div className="bg-bg-card border border-[#1A2332] rounded-lg p-4" style={{ height: 280 }}>
      <div className="mb-3">
        <div className="text-txt-primary text-sm font-display font-semibold">{country.name} — Trade History</div>
        <div className="text-txt-secondary text-xs font-mono">1988 – 2024 · USD</div>
      </div>
      <ResponsiveContainer width="100%" height={200}>
        <AreaChart data={data} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="gradExports" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%"  stopColor="#C87941" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#C87941" stopOpacity={0.02} />
            </linearGradient>
            <linearGradient id="gradImports" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%"  stopColor="#38BDF8" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#38BDF8" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#1A2332" vertical={false} />
          <XAxis
            dataKey="year"
            tick={{ fill: '#9CA3AF', fontFamily: 'JetBrains Mono', fontSize: 10 }}
            tickLine={false}
            axisLine={{ stroke: '#1A2332' }}
            interval={5}
          />
          <YAxis
            tickFormatter={fmtBn}
            tick={{ fill: '#9CA3AF', fontFamily: 'JetBrains Mono', fontSize: 10 }}
            tickLine={false}
            axisLine={false}
            width={52}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend
            wrapperStyle={{ fontFamily: 'JetBrains Mono', fontSize: 11, paddingTop: 4 }}
            formatter={(val) => <span style={{ color: '#9CA3AF' }}>{val}</span>}
          />
          <Area
            type="monotone"
            dataKey="exports"
            name="Exports"
            stroke="#C87941"
            strokeWidth={1.5}
            fill="url(#gradExports)"
            dot={false}
            activeDot={{ r: 3, fill: '#C87941' }}
          />
          <Area
            type="monotone"
            dataKey="imports"
            name="Imports"
            stroke="#38BDF8"
            strokeWidth={1.5}
            fill="url(#gradImports)"
            dot={false}
            activeDot={{ r: 3, fill: '#38BDF8' }}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}
