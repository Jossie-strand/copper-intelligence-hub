// Signal badge — BULL / BEAR / NEUTRAL pill

const CONFIG = {
  bull:    { label: 'BULL',    bg: 'bg-signal-bull/15',    text: 'text-signal-bull',    dot: 'bg-signal-bull' },
  bear:    { label: 'BEAR',    bg: 'bg-signal-bear/15',    text: 'text-signal-bear',    dot: 'bg-signal-bear' },
  neutral: { label: 'NEUTRAL', bg: 'bg-signal-neutral/15', text: 'text-signal-neutral', dot: 'bg-signal-neutral' },
}

export default function SignalBadge({ signal, label, description, size = 'md' }) {
  const cfg = CONFIG[signal] || CONFIG.neutral
  const displayLabel = label || cfg.label

  if (size === 'sm') {
    return (
      <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-mono font-semibold ${cfg.bg} ${cfg.text}`}>
        <span className={`w-1 h-1 rounded-full ${cfg.dot}`} />
        {displayLabel}
      </span>
    )
  }

  return (
    <div className={`flex items-start gap-3 rounded-lg p-3 border ${cfg.bg} border-current/10`}>
      <div className={`flex items-center gap-2 flex-shrink-0 ${cfg.text}`}>
        <span className={`w-2 h-2 rounded-full ${cfg.dot}`} />
        <span className="text-xs font-mono font-bold tracking-widest">{displayLabel}</span>
      </div>
      {description && (
        <p className="text-txt-secondary text-xs font-body leading-snug">{description}</p>
      )}
    </div>
  )
}
