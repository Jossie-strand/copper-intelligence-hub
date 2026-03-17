// Tab switcher for COMEX / LME / SHFE

const TABS = [
  { id: 'comex', label: 'COMEX' },
  { id: 'lme',   label: 'LME' },
  { id: 'shfe',  label: 'SHFE' },
]

export default function ExchangeTab({ active, onChange }) {
  return (
    <div className="flex gap-1 bg-bg-primary rounded-lg p-1 w-fit">
      {TABS.map(tab => (
        <button
          key={tab.id}
          onClick={() => onChange(tab.id)}
          className={`px-4 py-1.5 rounded text-xs font-mono font-semibold transition-colors ${
            active === tab.id
              ? 'bg-copper text-bg-primary'
              : 'text-txt-secondary hover:text-txt-primary'
          }`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  )
}
