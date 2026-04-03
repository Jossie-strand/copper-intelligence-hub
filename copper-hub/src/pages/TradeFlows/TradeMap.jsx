import 'leaflet/dist/leaflet.css'
import { useRef, useEffect, useState, useMemo } from 'react'
import { MapContainer, CircleMarker, Tooltip, Polyline, useMap } from 'react-leaflet'
import L from 'leaflet'
import { WITS_TO_ISO3, ISO3_TO_WITS } from '../../data/static/witsCountryCodes'

const GEOJSON_URL =
  'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson'

// ── Color helpers ─────────────────────────────────────────────────────────
const NEUTRAL = [20, 30, 46]
const COPPER  = [200, 121, 65]
const BLUE    = [14, 165, 233]

function lerp(a, b, t) { return Math.round(a + (b - a) * t) }

function getNetColor(net) {
  const abs = Math.abs(net || 0)
  if (abs < 5_000_000) return '#141E2E'
  const t = Math.min(1,
    (Math.log10(abs) - Math.log10(5e6)) / (Math.log10(1e11) - Math.log10(5e6)))
  const [c1, c2] = net > 0 ? [NEUTRAL, COPPER] : [NEUTRAL, BLUE]
  return `rgb(${lerp(c1[0],c2[0],t)},${lerp(c1[1],c2[1],t)},${lerp(c1[2],c2[2],t)})`
}

function buildStyle(data, isSelected) {
  return {
    fillColor:   data ? getNetColor(data.net) : '#0F1824',
    fillOpacity: 0.85,
    color:       isSelected ? '#C87941' : '#1A2332',
    weight:      isSelected ? 2 : 0.5,
  }
}

// ── Arc helpers ───────────────────────────────────────────────────────────
function arcPoints(from, to, steps = 40) {
  if (!from || !to) return []
  const [la1,lo1]=from, [la2,lo2]=to
  const mLa=(la1+la2)/2, mLo=(lo1+lo2)/2
  const dLa=la2-la1, dLo=lo2-lo1
  const dist=Math.sqrt(dLa*dLa+dLo*dLo)
  if (dist<0.5) return [[la1,lo1],[la2,lo2]]
  const h=Math.min(dist*0.35,30)
  const cpLa=mLa+(-dLo/dist)*h, cpLo=mLo+(dLa/dist)*h
  const pts=[]
  for (let i=0;i<=steps;i++) {
    const t=i/steps, u=1-t
    pts.push([u*u*la1+2*u*t*cpLa+t*t*la2, u*u*lo1+2*u*t*cpLo+t*t*lo2])
  }
  return pts
}

function arcWeight(value, maxValue) {
  if (!maxValue) return 1.5
  return Math.max(1.5, Math.min(5, (Math.log10(Math.max(value,1))/Math.log10(maxValue))*5))
}

// For MultiPolygon countries (France with overseas territories, USA with Alaska/Hawaii, etc.)
// use the largest polygon's bounding-box centroid rather than the whole feature's bbox,
// so the circle lands on the main landmass not in the ocean.
function polyBboxArea(rings) {
  let minLa=90, maxLa=-90, minLo=180, maxLo=-180
  rings[0].forEach(([lo,la]) => {
    if(la<minLa)minLa=la; if(la>maxLa)maxLa=la
    if(lo<minLo)minLo=lo; if(lo>maxLo)maxLo=lo
  })
  return (maxLo-minLo)*(maxLa-minLa)
}

function computeCentroid(geometry) {
  if (geometry.type === 'Polygon') {
    let minLa=90, maxLa=-90, minLo=180, maxLo=-180
    geometry.coordinates[0].forEach(([lo,la]) => {
      if(la<minLa)minLa=la; if(la>maxLa)maxLa=la
      if(lo<minLo)minLo=lo; if(lo>maxLo)maxLo=lo
    })
    return [(minLa+maxLa)/2, (minLo+maxLo)/2]
  }
  if (geometry.type === 'MultiPolygon') {
    // Use centroid of the largest polygon (by bbox area) to avoid overseas territory skew
    let best = geometry.coordinates[0]
    let bestArea = polyBboxArea(best)
    for (const poly of geometry.coordinates) {
      const a = polyBboxArea(poly)
      if (a > bestArea) { bestArea = a; best = poly }
    }
    let minLa=90, maxLa=-90, minLo=180, maxLo=-180
    best[0].forEach(([lo,la]) => {
      if(la<minLa)minLa=la; if(la>maxLa)maxLa=la
      if(lo<minLo)minLo=lo; if(lo>maxLo)maxLo=lo
    })
    return [(minLa+maxLa)/2, (minLo+maxLo)/2]
  }
  return [0, 0]
}

function fmtUSD(val) {
  const abs=Math.abs(val||0)
  if(abs>=1e9) return `$${(abs/1e9).toFixed(1)}B`
  if(abs>=1e6) return `$${(abs/1e6).toFixed(0)}M`
  return `$${Math.round(abs).toLocaleString()}`
}

// ── Visual-only choropleth layer (GeoJSON polygons, no pointer events) ────
// Exposes highlight/unhighlight imperatively via highlightRef to avoid
// re-rendering 195 CircleMarkers on every hover.
function GeoJsonLayer({ geojson, netPositions, selectedIso3, highlightRef }) {
  const map        = useMap()
  const layerRef   = useRef(null)
  const fLayersRef = useRef({})
  const live       = useRef()
  live.current = { netPositions, selectedIso3 }

  // Create layer once when GeoJSON is available
  useEffect(() => {
    if (!geojson || !map) return
    const fl = {}
    const layer = L.geoJSON(geojson, {
      style(feature) {
        const iso3 = feature.properties["ISO3166-1-Alpha-3"]
        return buildStyle(live.current.netPositions.get(iso3), iso3 === live.current.selectedIso3)
      },
      onEachFeature(feature, l) {
        const iso3 = feature.properties["ISO3166-1-Alpha-3"]
        if (iso3 && iso3 !== '-99') fl[iso3] = l
      },
    })
    layer.addTo(map)
    layerRef.current = layer
    fLayersRef.current = fl
    return () => {
      layer.remove()
      layerRef.current = null
      fLayersRef.current = {}
    }
  }, [geojson, map]) // eslint-disable-line react-hooks/exhaustive-deps

  // Refresh choropleth when trade data or selection changes
  useEffect(() => {
    if (!layerRef.current) return
    layerRef.current.setStyle(feature => {
      const iso3 = feature.properties["ISO3166-1-Alpha-3"]
      return buildStyle(netPositions.get(iso3), iso3 === selectedIso3)
    })
  }, [netPositions, selectedIso3])

  // Expose imperative highlight/unhighlight — called by CircleMarker handlers
  // Runs every render so live.current is always fresh in the closures
  useEffect(() => {
    highlightRef.current = {
      highlight(iso3) {
        const { netPositions: np, selectedIso3: sel } = live.current
        layerRef.current?.setStyle(f => {
          const fi = f.properties["ISO3166-1-Alpha-3"]
          return buildStyle(np.get(fi), fi === sel)
        })
        const fl = fLayersRef.current[iso3]
        if (fl) {
          fl.setStyle({ weight: 2, color: '#E8A76C', fillOpacity: 1 })
          fl.bringToFront?.()
        }
      },
      unhighlight() {
        const { netPositions: np, selectedIso3: sel } = live.current
        layerRef.current?.setStyle(f => {
          const fi = f.properties["ISO3166-1-Alpha-3"]
          return buildStyle(np.get(fi), fi === sel)
        })
      },
    }
  })

  return null
}

// ── Main exported component ───────────────────────────────────────────────
export default function TradeMap({ netPositions, selectedIso3, onCountryClick, fetchPartners }) {
  const [geojson, setGeojson]     = useState(null)
  const [centroids, setCentroids] = useState(new Map())
  const [arcs, setArcs]           = useState([])
  const highlightRef = useRef({ highlight: () => {}, unhighlight: () => {} })

  useEffect(() => {
    fetch(GEOJSON_URL)
      .then(r => { if (!r.ok) throw new Error(r.status); return r.json() })
      .then(data => {
        const ctrs = new Map()
        data.features.forEach(f => {
          const iso3 = f.properties?.["ISO3166-1-Alpha-3"]
          if (!iso3 || iso3 === '-99' || !f.geometry) return
          ctrs.set(iso3, computeCentroid(f.geometry))
        })
        setCentroids(ctrs)
        setGeojson(data)
      })
      .catch(err => console.error('GeoJSON load error:', err))
  }, [])

  const centroidEntries = useMemo(() =>
    Array.from(centroids.entries()).map(([iso3, [lat, lng]]) => ({ iso3, lat, lng })),
    [centroids])

  return (
    <div className="relative rounded-lg overflow-hidden border border-[#1A2332]" style={{ height: '60vh' }}>
      {!geojson && (
        <div className="absolute inset-0 bg-bg-primary flex items-center justify-center z-10">
          <span className="text-txt-secondary text-sm font-mono">Loading world map…</span>
        </div>
      )}

      <MapContainer
        center={[20, 0]}
        zoom={2}
        minZoom={1}
        maxZoom={8}
        style={{ height: '100%', width: '100%', background: '#080D14' }}
        scrollWheelZoom
        worldCopyJump
        zoomControl={false}
      >
        {geojson && (
          <GeoJsonLayer
            geojson={geojson}
            netPositions={netPositions}
            selectedIso3={selectedIso3}
            highlightRef={highlightRef}
          />
        )}

        {/* Invisible CircleMarkers at country centroids — proven-reliable interaction layer.
            GeoJSON polygon events are broken in this Leaflet/React setup; CircleMarkers work. */}
        {centroidEntries.map(({ iso3, lat, lng }) => {
          const data     = netPositions.get(iso3)
          const name     = data?.name ?? iso3
          const witscode = data?.witscode ?? ISO3_TO_WITS[iso3]
          return (
            <CircleMarker
              key={iso3}
              center={[lat, lng]}
              radius={22}
              pathOptions={{ fillOpacity: 0, color: 'transparent', weight: 0 }}
              eventHandlers={{
                mouseover() {
                  highlightRef.current.highlight(iso3)
                  if (!witscode) return
                  fetchPartners(witscode)
                    .then(({ topExports, topImports }) => {
                      const fromC  = centroids.get(iso3)
                      const maxVal = Math.max(...topExports.map(p=>p.total), ...topImports.map(p=>p.total), 1)
                      function mkArcs(list, color, inbound) {
                        return list.flatMap(p => {
                          const pIso = WITS_TO_ISO3[parseInt(p.code)]
                          const toC  = pIso ? centroids.get(pIso) : null
                          const pts  = inbound ? arcPoints(toC, fromC) : arcPoints(fromC, toC)
                          return pts.length ? [{ pts, color, weight: arcWeight(p.total, maxVal) }] : []
                        })
                      }
                      setArcs([
                        ...mkArcs(topExports, '#C87941', false),
                        ...mkArcs(topImports, '#38BDF8', true),
                      ])
                    })
                    .catch(console.error)
                },
                mouseout() {
                  highlightRef.current.unhighlight()
                  setArcs([])
                },
                click() {
                  onCountryClick?.({ iso3, name, witscode, data })
                },
              }}
            >
              <Tooltip direction="top" offset={[0, -4]} opacity={1}>
                <div style={{
                  fontFamily: 'JetBrains Mono, monospace', fontSize: 11, lineHeight: 1.8,
                  color: '#E8E4DC', background: '#0C1220',
                  padding: '6px 10px', border: '1px solid #1A2332',
                  borderRadius: 4, minWidth: 160,
                }}>
                  <div style={{ fontWeight: 700, color: '#C87941', marginBottom: 4 }}>{name}</div>
                  {data ? (
                    <>
                      <div>
                        <span style={{ color: '#9CA3AF' }}>Exports  </span>
                        <span style={{ color: '#22C55E' }}>{fmtUSD(data.exports)}</span>
                      </div>
                      <div>
                        <span style={{ color: '#9CA3AF' }}>Imports  </span>
                        <span style={{ color: '#EF4444' }}>{fmtUSD(data.imports)}</span>
                      </div>
                      <div style={{ borderTop: '1px solid #1A2332', marginTop: 4, paddingTop: 4 }}>
                        <span style={{ color: '#9CA3AF' }}>Net      </span>
                        <span style={{ color: data.net >= 0 ? '#22C55E' : '#EF4444' }}>
                          {data.net >= 0 ? '+' : '–'}{fmtUSD(Math.abs(data.net))}
                        </span>
                      </div>
                    </>
                  ) : (
                    <div style={{ color: '#9CA3AF' }}>No trade data</div>
                  )}
                  <div style={{ color: '#9CA3AF', marginTop: 4, fontSize: 10 }}>Click to explore →</div>
                </div>
              </Tooltip>
            </CircleMarker>
          )
        })}

        {arcs.map((arc, i) => (
          <Polyline
            key={i}
            positions={arc.pts}
            pathOptions={{ color: arc.color, weight: arc.weight, opacity: 0.75, smoothFactor: 1 }}
          />
        ))}
      </MapContainer>

      {/* Legend */}
      <div className="absolute bottom-3 left-3 z-[1000] bg-bg-card/90 border border-[#1A2332] rounded px-3 py-2 text-xs font-mono text-txt-secondary">
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-1.5">
            <div className="w-10 h-2 rounded" style={{ background: 'linear-gradient(to right, #141E2E, #C87941)' }} />
            <span>Net exporter</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-10 h-2 rounded" style={{ background: 'linear-gradient(to right, #141E2E, #38BDF8)' }} />
            <span>Net importer</span>
          </div>
        </div>
        <div className="flex items-center gap-3 mt-1.5">
          <div className="flex items-center gap-1.5">
            <div className="w-5 border-t-2" style={{ borderColor: '#C87941' }} />
            <span>Export flows</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-5 border-t-2" style={{ borderColor: '#38BDF8' }} />
            <span>Import flows</span>
          </div>
        </div>
      </div>
    </div>
  )
}
