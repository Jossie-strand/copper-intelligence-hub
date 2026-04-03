import { useState, useEffect } from 'react'
import { supabase } from '../../lib/supabase'

export function useInventoryData() {
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)
  const [data, setData]       = useState(null)

  useEffect(() => {
    let cancelled = false

    async function fetchAll() {
      setLoading(true)
      try {
        const [invRes, comexRes, shfeRes, lmeDetailRes] = await Promise.all([
          supabase
            .from('exchange_inventory_daily')
            .select('data_date, exchange, total_mt, change_mt')
            .order('data_date', { ascending: true })
            .limit(5000),
          supabase
            .from('comex_warehouse_daily')
            .select('*')
            .order('data_date', { ascending: false })
            .limit(100),
          supabase
            .from('shfe_region_daily')
            .select('*')
            .order('data_date', { ascending: false })
            .limit(200),
          supabase
            .from('lme_detail_daily')
            .select('*')
            .order('data_date', { ascending: false })
            .limit(1),
        ])

        if (invRes.error) throw invRes.error

        if (!cancelled) setData({
          inventory: invRes.data || [],
          comexWarehouses: comexRes.data || [],
          shfeRegions: shfeRes.data || [],
          lmeDetail: lmeDetailRes.data?.[0] || null,
        })
      } catch (err) {
        if (!cancelled) setError(err)
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    fetchAll()
    return () => { cancelled = true }
  }, [])

  if (!data) return { comex: null, comexHistory: [], lme: null, lmeHistory: [], shfe: null, shfeHistory: [], loading, error }

  const { inventory, comexWarehouses, shfeRegions, lmeDetail } = data

  // ── Group inventory rows by exchange ────────────────────────────────────
  const byExchange = {}
  for (const row of inventory) {
    if (!byExchange[row.exchange]) byExchange[row.exchange] = []
    byExchange[row.exchange].push(row)
  }

  function latestOf(exchange) {
    const rows = byExchange[exchange]
    if (!rows || rows.length === 0) return null
    return rows[rows.length - 1] // already sorted ascending
  }

  function historyOf(exchange) {
    return (byExchange[exchange] || []).map(r => ({
      date: r.data_date,
      total: Number(r.total_mt) || 0,
      change: Number(r.change_mt) || 0,
    }))
  }

  // ── COMEX ───────────────────────────────────────────────────────────────
  const comexLatest = latestOf('COMEX')
  const comexLatestDate = comexWarehouses.length > 0
    ? comexWarehouses.reduce((max, r) => r.data_date > max ? r.data_date : max, comexWarehouses[0].data_date)
    : null
  const latestWarehouses = comexWarehouses.filter(r => r.data_date === comexLatestDate)

  const totalRegistered = latestWarehouses.reduce((s, w) => s + (Number(w.registered_mt) || 0), 0)
  const totalEligible   = latestWarehouses.reduce((s, w) => s + (Number(w.eligible_mt) || 0), 0)
  const comexGrandTotal = Number(comexLatest?.total_mt) || (totalRegistered + totalEligible) || 0

  const comex = {
    grandTotal: comexGrandTotal,
    totalRegistered,
    totalEligible,
    regPct: comexGrandTotal > 0 ? (totalRegistered / comexGrandTotal) * 100 : 0,
    change: Number(comexLatest?.change_mt) ?? null,
    date: comexLatest?.data_date || comexLatestDate,
    warehouses: latestWarehouses.map(w => ({
      name: w.warehouse,
      registered: Number(w.registered_mt) || 0,
      eligible:   Number(w.eligible_mt)   || 0,
      total:      Number(w.total_mt)      || 0,
      regPct: Number(w.total_mt) > 0
        ? (Number(w.registered_mt) / Number(w.total_mt)) * 100
        : 0,
      history: [], // populated once we have multi-day data
    })),
  }

  const comexHistory = historyOf('COMEX').map(r => ({
    ...r,
    registered: 0, // will come from warehouse history later
    eligible: 0,
  }))

  // ── LME ─────────────────────────────────────────────────────────────────
  const lmeLatest = latestOf('LME')
  const lme = {
    total: Number(lmeLatest?.total_mt) || 0,
    change: Number(lmeLatest?.change_mt) ?? null,
    date: lmeLatest?.data_date || null,
    cancelledWarrants: Number(lmeDetail?.cancelled_warrants_mt) || 0,
    cancelledPct: Number(lmeDetail?.cancelled_pct) || 0,
  }

  const lmeHistory = historyOf('LME').map(r => ({
    ...r,
    cancelledWarrants: 0,
  }))

  // ── SHFE ────────────────────────────────────────────────────────────────
  const shfeLatest = latestOf('SHFE')
  const shfeLatestDate = shfeRegions.length > 0
    ? shfeRegions.reduce((max, r) => r.data_date > max ? r.data_date : max, shfeRegions[0].data_date)
    : null
  const latestRegions = shfeRegions.filter(r => r.data_date === shfeLatestDate)

  const shfe = {
    grandTotal: Number(shfeLatest?.total_mt) || 0,
    change: Number(shfeLatest?.change_mt) ?? null,
    date: shfeLatest?.data_date || shfeLatestDate,
    regions: latestRegions.map(r => ({
      name: r.region,
      total: Number(r.total_mt) || 0,
      change: Number(r.change_mt) || 0,
      history: [],
    })),
  }

  const shfeHistory = historyOf('SHFE')

  return { comex, comexHistory, lme, lmeHistory, shfe, shfeHistory, loading, error }
}
