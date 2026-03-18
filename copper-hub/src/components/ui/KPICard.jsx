// KPI card: label + large number + change badge
// changeValue: positive = green, negative = red, null = hide badge
// signal: 'bull' | 'bear' | 'neutral' | null — colors the left border

const SIGNAL_BORDER = {
  bull:    'border-l-signal-bull',
  bear:    'border-l-signal-bear',
  neutral: 'border-l-signal-neutral',
}

export default function KPICard({ label, value, unit, changeValue, changeUnit, signal, subtext, onClick }) {
  const isPositiveChange = changeValue > 0
  const changeColor = isPositiveChange ? 'text-signal-bull' : changeValue < 0 ? 'text-signal-bear' : 'text-txt-secondary'
  const borderClass = signal ? `border-l-2 ${SIGNAL_BORDER[signal]}` : ''
  const clickClass = onClick ? 'cursor-pointer hover:border-[#C87941]/50 transition-colors' : ''

  return (
    <div className={`bg-bg-card rounded-lg p-4 ${borderClass} border border-[#1A2332] ${clickClass}`} onClick={onClick}>
      <p className="text-txt-secondary text-xs font-body uppercase tracking-wider mb-2">{label}</p>
      <div className="flex items-end gap-2">
        <span className="text-txt-primary text-2xl font-mono font-semibold leading-none">
          {value}
        </span>
        {unit && (
          <span className="text-txt-secondary text-sm font-mono mb-0.5">{unit}</span>
        )}
      </div>
      {changeValue !== undefined && changeValue !== null && (
        <div className={`flex items-center gap-1 mt-2 text-xs font-mono ${changeColor}`}>
          <span>{changeValue > 0 ? '▲' : changeValue < 0 ? '▼' : '—'}</span>
          <span>
            {changeValue > 0 ? '+' : ''}
            {changeUnit === 'pct' ? `${changeValue}%` : `${changeValue?.toLocaleString()} ${changeUnit || ''}`}
          </span>
        </div>
      )}
      {subtext && (
        <p className="text-txt-secondary text-xs font-body mt-1">{subtext}</p>
      )}
    </div>
  )
}
