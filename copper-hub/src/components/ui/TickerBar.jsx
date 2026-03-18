const TICKERS = [
  { symbol: 'COPX',  exchange: 'NYSE Arca', price: '$43.12', change: '+1.8%', positive: true,  url: 'https://finance.yahoo.com/quote/COPX' },
  { symbol: 'COPR',  exchange: 'NYSE Arca', price: '$21.84', change: '+2.1%', positive: true,  url: 'https://finance.yahoo.com/quote/COPR' },
  { symbol: 'FCX',   exchange: 'NYSE',      price: '$38.67', change: '+0.9%', positive: true,  url: 'https://finance.yahoo.com/quote/FCX' },
  { symbol: 'SCCO',  exchange: 'NYSE',      price: '$91.20', change: '-0.4%', positive: false, url: 'https://finance.yahoo.com/quote/SCCO' },
  { symbol: 'TECK',  exchange: 'TSX',       price: 'C$52.80', change: '+1.2%', positive: true,  url: 'https://finance.yahoo.com/quote/TECK' },
  { symbol: 'BHP',   exchange: 'NYSE',      price: '$54.33', change: '-0.7%', positive: false, url: 'https://finance.yahoo.com/quote/BHP' },
  { symbol: 'RIO',   exchange: 'NYSE',      price: '$62.18', change: '+0.3%', positive: true,  url: 'https://finance.yahoo.com/quote/RIO' },
  { symbol: 'GLEN',  exchange: 'LSE',       price: '£4.82',  change: '+0.6%', positive: true,  url: 'https://finance.yahoo.com/quote/GLEN.L' },
  { symbol: 'IVNH',  exchange: 'TSX',       price: 'C$18.45', change: '+3.2%', positive: true,  url: 'https://finance.yahoo.com/quote/IVPAF' },
  { symbol: 'FM',    exchange: 'TSX',       price: 'C$15.90', change: '-1.1%', positive: false, url: 'https://finance.yahoo.com/quote/FQVLF' },
]

function TickerItem({ symbol, exchange, price, change, positive, url }) {
  return (
    <a
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      className="inline-flex items-center gap-2 px-4 whitespace-nowrap no-underline hover:opacity-80"
    >
      <span className="text-[#C87941] font-mono text-xs font-semibold">{symbol}</span>
      <span className="text-[#9CA3AF] font-mono text-xs">{exchange}</span>
      <span className="text-[#E8E4DC] font-mono text-xs">{price}</span>
      <span className={`font-mono text-xs font-semibold ${positive ? 'text-[#22C55E]' : 'text-[#EF4444]'}`}>{change}</span>
      <span className="text-[#1A2332] text-xs">|</span>
    </a>
  )
}

export default function TickerBar() {
  return (
    <>
      <style>{`
        @keyframes marquee {
          from { transform: translateX(0); }
          to   { transform: translateX(-50%); }
        }
        .ticker-track {
          animation: marquee 40s linear infinite;
        }
        .ticker-track:hover {
          animation-play-state: paused;
        }
      `}</style>
      <div
        style={{ position: 'fixed', bottom: 0, left: 0, right: 0, zIndex: 50 }}
        className="bg-[#080D14] border-t border-[#1A2332] overflow-hidden h-9 flex items-center"
      >
        <div className="ticker-track flex items-center">
          {TICKERS.map((t) => <TickerItem key={t.symbol + '1'} {...t} />)}
          {TICKERS.map((t) => <TickerItem key={t.symbol + '2'} {...t} />)}
        </div>
      </div>
    </>
  )
}
