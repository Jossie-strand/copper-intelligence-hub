import { useState, useEffect, useCallback } from 'react'
import { supabase } from '../../lib/supabase'
import { WITS_TO_ISO3 } from '../static/witsCountryCodes'

const LATEST_YEAR = 2024

export function getProductCodes(product) {
  if (product === 'both') return ['260300', '740311']
  return [product]
}

function buildNetPositions(rows) {
  // Returns Map<iso3, {witscode, name, exports, imports, net}>
  const map = new Map()
  for (const row of rows) {
    const iso3 = WITS_TO_ISO3[parseInt(row.reporter_code)]
    if (!iso3) continue
    if (!map.has(iso3)) {
      map.set(iso3, { witscode: row.reporter_code, name: row.reporter_name, exports: 0, imports: 0 })
    }
    const entry = map.get(iso3)
    const val = Number(row.value_usd) || 0
    if (row.indicator === 'XPRT-TRD-VL') entry.exports += val
    else if (row.indicator === 'MPRT-TRD-VL') entry.imports += val
  }
  for (const entry of map.values()) {
    entry.net = entry.exports - entry.imports
  }
  return map
}

export function useTradeFlowsData(product) {
  const [netPositions, setNetPositions] = useState(new Map())
  const [rankings, setRankings] = useState({ exporters: [], importers: [] })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)

    supabase
      .from('wits_trade_annual')
      .select('reporter_code, reporter_name, indicator, value_usd')
      .eq('year', LATEST_YEAR)
      .in('product_code', getProductCodes(product))
      .eq('partner_code', '0')
      .limit(2000)
      .then(({ data, error: err }) => {
        if (cancelled) return
        if (err) { setError(err); setLoading(false); return }

        const positions = buildNetPositions(data || [])
        setNetPositions(positions)

        const entries = Array.from(positions.entries())
          .map(([iso3, v]) => ({ iso3, ...v }))
          .filter(e => e.exports > 0 || e.imports > 0)

        setRankings({
          exporters: [...entries].sort((a, b) => b.exports - a.exports).slice(0, 25),
          importers: [...entries].sort((a, b) => b.imports - a.imports).slice(0, 25),
        })
        setLoading(false)
      })

    return () => { cancelled = true }
  }, [product])

  const fetchTimeSeries = useCallback(async (witscode) => {
    const { data, error: err } = await supabase
      .from('wits_trade_annual')
      .select('year, indicator, value_usd')
      .eq('reporter_code', witscode)
      .in('product_code', getProductCodes(product))
      .eq('partner_code', '0')
      .order('year', { ascending: true })
      .limit(500)

    if (err) throw err

    const byYear = new Map()
    for (const row of data || []) {
      if (!byYear.has(row.year)) byYear.set(row.year, { year: row.year, exports: 0, imports: 0 })
      const entry = byYear.get(row.year)
      const val = Number(row.value_usd) || 0
      if (row.indicator === 'XPRT-TRD-VL') entry.exports += val
      else if (row.indicator === 'MPRT-TRD-VL') entry.imports += val
    }
    return Array.from(byYear.values()).sort((a, b) => a.year - b.year)
  }, [product])

  const fetchPartners = useCallback(async (witscode) => {
    const { data, error: err } = await supabase
      .from('wits_trade_annual')
      .select('partner_code, partner_name, indicator, value_usd')
      .eq('reporter_code', witscode)
      .eq('year', LATEST_YEAR)
      .in('product_code', getProductCodes(product))
      .neq('partner_code', '0')
      .limit(1000)

    if (err) throw err

    const expMap = new Map()
    const impMap = new Map()

    for (const row of data || []) {
      const map = row.indicator === 'XPRT-TRD-VL' ? expMap : impMap
      const code = row.partner_code
      if (!map.has(code)) map.set(code, { code, name: row.partner_name, total: 0 })
      map.get(code).total += Number(row.value_usd) || 0
    }

    return {
      topExports: [...expMap.values()].sort((a, b) => b.total - a.total).slice(0, 5),
      topImports: [...impMap.values()].sort((a, b) => b.total - a.total).slice(0, 5),
    }
  }, [product])

  return { netPositions, rankings, loading, error, fetchTimeSeries, fetchPartners }
}
