import { useState, useEffect } from 'react'
import { supabase } from '../../lib/supabase'

function num(v) { return v != null ? Number(v) : null }

export function useDashboardData() {
  const [state, setState] = useState({
    inventorySummary: [],
    signal: null,
    topCorridors: [],
    chinaImports: [],
    yoyDeltas: [],
    globalGrowth: null,
    loading: true,
    error: null,
  })

  useEffect(() => {
    let cancelled = false

    async function fetchAll() {
      try {
        // Get latest year in trade data
        const yearRes = await supabase
          .from('wits_trade_annual').select('year')
          .order('year', { ascending: false }).limit(1)
        const latestYear = yearRes.data?.[0]?.year ?? 2024
        const prevYear = latestYear - 1

        const [summaryRes, signalRes, corridorRes, chinaRes, yoyRes, globalRes] = await Promise.all([
          supabase.from('v_dashboard_summary').select('*')
            .order('data_date', { ascending: false }).limit(60),
          supabase.from('v_signal_thresholds').select('*').limit(1),
          // Top bilateral flows (export side)
          supabase.from('wits_trade_annual')
            .select('reporter_name, partner_name, value_usd')
            .eq('indicator', 'XPRT-TRD-VL').neq('partner_code', '0')
            .eq('year', latestYear).limit(500),
          // China imports by source
          supabase.from('wits_trade_annual')
            .select('partner_name, value_usd')
            .eq('reporter_code', '156').eq('indicator', 'MPRT-TRD-VL')
            .neq('partner_code', '0').eq('year', latestYear).limit(200),
          // YoY comparison (export side, 2 years)
          supabase.from('wits_trade_annual')
            .select('reporter_code, reporter_name, partner_code, partner_name, year, value_usd')
            .eq('indicator', 'XPRT-TRD-VL').neq('partner_code', '0')
            .in('year', [latestYear, prevYear]).limit(5000),
          // Global trade by year
          supabase.from('wits_trade_annual')
            .select('year, value_usd')
            .eq('partner_code', '0').eq('indicator', 'XPRT-TRD-VL')
            .order('year', { ascending: true }).limit(5000),
        ])

        if (cancelled) return

        // ── Inventory ────────────────────────────────────────────
        const summary = (summaryRes.data || []).reverse()

        // ── Top corridors ────────────────────────────────────────
        const corridorMap = new Map()
        for (const r of corridorRes.data || []) {
          const key = `${r.reporter_name} → ${r.partner_name}`
          corridorMap.set(key, (corridorMap.get(key) || 0) + Number(r.value_usd))
        }
        const topCorridors = [...corridorMap.entries()]
          .map(([flow, total]) => ({ flow, total }))
          .sort((a, b) => b.total - a.total)
          .slice(0, 8)

        // ── China imports ────────────────────────────────────────
        const chinaMap = new Map()
        for (const r of chinaRes.data || []) {
          chinaMap.set(r.partner_name, (chinaMap.get(r.partner_name) || 0) + Number(r.value_usd))
        }
        const chinaImports = [...chinaMap.entries()]
          .map(([name, total]) => ({ name, total }))
          .sort((a, b) => b.total - a.total)

        // ── YoY deltas ───────────────────────────────────────────
        const yoyMap = new Map()
        for (const r of yoyRes.data || []) {
          const key = `${r.reporter_code}-${r.partner_code}`
          if (!yoyMap.has(key)) yoyMap.set(key, {
            flow: `${r.reporter_name} → ${r.partner_name}`,
            latest: 0, prev: 0,
          })
          const e = yoyMap.get(key)
          if (r.year === latestYear) e.latest += Number(r.value_usd)
          else e.prev += Number(r.value_usd)
        }
        const yoyDeltas = [...yoyMap.values()]
          .filter(e => e.prev > 100_000_000 && e.latest > 100_000_000)
          .map(e => ({
            flow: e.flow,
            latest: e.latest,
            prev: e.prev,
            changePct: ((e.latest - e.prev) / e.prev) * 100,
          }))
          .sort((a, b) => Math.abs(b.changePct) - Math.abs(a.changePct))
          .slice(0, 5)

        // ── Global growth ────────────────────────────────────────
        const yearMap = new Map()
        for (const r of globalRes.data || []) {
          yearMap.set(r.year, (yearMap.get(r.year) || 0) + Number(r.value_usd))
        }
        const years = [...yearMap.entries()]
          .map(([year, total]) => ({ year, total }))
          .sort((a, b) => a.year - b.year)

        let globalGrowth = null
        if (years.length >= 2) {
          const lt = years[years.length - 1]
          const base = years.find(y => y.year === latestYear - 5) || years[0]
          const n = lt.year - base.year
          globalGrowth = {
            years,
            latestTotal: lt.total,
            latestYear: lt.year,
            absoluteChange: lt.total - base.total,
            cagr: n > 0 ? (Math.pow(lt.total / base.total, 1 / n) - 1) * 100 : 0,
            baseYear: base.year,
          }
        }

        setState({
          inventorySummary: summary,
          signal: signalRes.data?.[0] || null,
          topCorridors,
          chinaImports,
          yoyDeltas,
          globalGrowth,
          loading: false,
          error: null,
        })
      } catch (err) {
        if (!cancelled) setState(s => ({ ...s, loading: false, error: err }))
      }
    }

    fetchAll()
    return () => { cancelled = true }
  }, [])

  const { inventorySummary } = state
  const latest = inventorySummary.length > 0
    ? inventorySummary[inventorySummary.length - 1] : null

  return {
    ...state,
    latestInventory: latest ? {
      date: latest.data_date,
      comexTotal: num(latest.comex_total),
      comexChange: num(latest.comex_change),
      lmeTotal: num(latest.lme_total),
      lmeChange: num(latest.lme_change),
      shfeTotal: num(latest.shfe_total),
      shfeChange: num(latest.shfe_change),
      combinedTotal: num(latest.combined_total),
      lmeCancelledPct: num(latest.lme_cancelled_pct),
      lmeCancelled: num(latest.lme_cancelled),
    } : null,
  }
}
