export default function SectionHeader({ title, subtitle, action }) {
  return (
    <div className="flex items-start justify-between mb-4">
      <div>
        <h2 className="text-txt-primary font-display font-semibold text-base">{title}</h2>
        {subtitle && (
          <p className="text-txt-secondary text-xs font-body mt-0.5">{subtitle}</p>
        )}
      </div>
      {action && <div>{action}</div>}
    </div>
  )
}
