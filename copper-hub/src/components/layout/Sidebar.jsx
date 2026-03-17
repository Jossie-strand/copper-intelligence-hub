import { NavLink } from 'react-router-dom'

const navItems = [
  { to: '/dashboard',     label: 'Dashboard' },
  { to: '/inventories',   label: 'Inventories' },
  { to: '/pricing',       label: 'Pricing' },
  { to: '/mines',         label: 'Global Mines' },
  { to: '/supply-demand', label: 'Supply & Demand' },
  { to: '/disruptions',   label: 'Disruptions' },
]

export default function Sidebar() {
  return (
    <aside className="fixed left-0 top-0 h-screen w-60 bg-bg-card border-r border-[#1A2332] flex flex-col z-10">
      {/* Logo */}
      <div className="px-6 py-5 border-b border-[#1A2332]">
        <div className="flex items-center gap-2">
          <span className="text-copper font-display font-bold text-lg leading-none">Cu</span>
          <span className="text-txt-primary font-display font-bold text-lg leading-none tracking-wide">
            COPPER HUB
          </span>
        </div>
        <p className="text-txt-secondary text-xs font-body mt-1">Market Intelligence</p>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-3 py-4 space-y-0.5">
        {navItems.map(({ to, label }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2.5 rounded text-sm font-body transition-colors ${
                isActive
                  ? 'text-copper bg-copper/10 border-l-2 border-copper font-medium'
                  : 'text-txt-secondary hover:text-txt-primary hover:bg-white/5'
              }`
            }
          >
            {({ isActive }) => (
              <>
                <span
                  className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                    isActive ? 'bg-copper' : 'bg-[#1A2332]'
                  }`}
                />
                {label}
              </>
            )}
          </NavLink>
        ))}
      </nav>

      {/* Footer */}
      <div className="px-5 py-4 border-t border-[#1A2332]">
        <p className="text-txt-secondary text-xs font-mono mb-1">Last updated</p>
        <p className="text-txt-primary text-xs font-mono">2026-03-14 22:31 ET</p>
        <div className="flex items-center gap-1.5 mt-2">
          <span className="w-1.5 h-1.5 rounded-full bg-signal-bull" />
          <span className="text-signal-bull text-xs font-mono">3/3 feeds OK</span>
        </div>
      </div>
    </aside>
  )
}
