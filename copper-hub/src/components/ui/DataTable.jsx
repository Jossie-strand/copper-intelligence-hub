// Reusable table — all data cells use JetBrains Mono
// columns: [{ key, label, align?: 'right'|'left'|'center', render?: (val, row) => node }]

export default function DataTable({ columns, rows, onRowClick, maxHeight }) {
  return (
    <div
      className="overflow-auto border border-[#1A2332] rounded-lg"
      style={maxHeight ? { maxHeight } : undefined}
    >
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="border-b border-[#1A2332] sticky top-0 bg-bg-card">
            {columns.map(col => (
              <th
                key={col.key}
                className={`px-4 py-3 text-txt-secondary text-xs font-body uppercase tracking-wider whitespace-nowrap ${
                  col.align === 'right' ? 'text-right' : col.align === 'center' ? 'text-center' : 'text-left'
                }`}
              >
                {col.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr
              key={row.id ?? row.key ?? i}
              onClick={onRowClick ? () => onRowClick(row) : undefined}
              className={`border-b border-[#1A2332]/50 transition-colors ${
                onRowClick ? 'cursor-pointer hover:bg-white/5' : 'hover:bg-white/3'
              } ${i % 2 === 0 ? '' : 'bg-white/[0.02]'}`}
            >
              {columns.map(col => (
                <td
                  key={col.key}
                  className={`px-4 py-3 font-mono text-txt-primary whitespace-nowrap ${
                    col.align === 'right' ? 'text-right' : col.align === 'center' ? 'text-center' : 'text-left'
                  }`}
                >
                  {col.render ? col.render(row[col.key], row) : row[col.key]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
