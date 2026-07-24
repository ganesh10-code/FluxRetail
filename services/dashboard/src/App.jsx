import { useState, useCallback, useRef, useEffect } from 'react'
import { KPICards } from './components/KPICards'
import { EventLog } from './components/EventLog'
import { ZoneChart } from './components/ZoneChart'
import { LiveFeed } from './components/LiveFeed'
import { Sidebar } from './components/Sidebar'
import { SystemStatus } from './components/SystemStatus'
import { useWebSocket } from './hooks/useWebSocket'
import { RefreshCw, Wifi, WifiOff } from 'lucide-react'

// Flicker-free preloading image component for live snapshots
function SmoothImage({ src, alt, className, onError, onLoadSuccess, ...props }) {
  const [displaySrc, setDisplaySrc] = useState(src)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    setLoading(true)
    const img = new Image()
    img.src = src
    img.onload = () => {
      setDisplaySrc(src)
      setLoading(false)
      if (onLoadSuccess) onLoadSuccess()
    }
    img.onerror = () => {
      setLoading(false)
      if (onError) onError()
    }
  }, [src])

  return (
    <div className="w-full h-full bg-slate-950 relative overflow-hidden">
      <img
        src={displaySrc}
        alt={alt}
        className={`${className} transition-opacity duration-300 ${loading ? 'opacity-80' : 'opacity-100'}`}
        {...props}
      />
    </div>
  )
}

const MAX_EVENTS = 200  // keep last 200 events in memory
const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8000'
const API_URL = WS_URL.replace('ws://', 'http://').replace('wss://', 'https://')

export default function App() {
  const [activeTab, setActiveTab] = useState('dashboard')
  const [events, setEvents] = useState([])
  // kpiData is the live WebSocket KPI broadcast — authoritative source for active_visitors
  const [kpiData, setKpiData] = useState(null)
  const totalEventsRef = useRef(0)
  const [totalEvents, setTotalEvents] = useState(0)

  // Configuration-driven Store and Camera State
  const [selectedStore, setSelectedStore] = useState('store_1')
  const [storeConfig, setStoreConfig] = useState(null)
  const [selectedCameraId, setSelectedCameraId] = useState('')
  const [snapshotUrl, setSnapshotUrl] = useState('')
  const [viewMode, setViewMode] = useState('snapshot') // snapshot | layout

  // Dynamic API metrics states
  const [metrics, setMetrics] = useState(null)
  const [funnel, setFunnel] = useState(null)
  const [anomalies, setAnomalies] = useState([])
  const [health, setHealth] = useState(null)
  const [snapshotOk, setSnapshotOk] = useState(true)

  // Fetch store configuration when selectedStore changes
  useEffect(() => {
    fetch(`${API_URL}/api/v1/stores/${selectedStore}/config`)
      .then(res => res.json())
      .then(data => {
        setStoreConfig(data)
        if (data.cameras) {
          const firstCamId = Object.values(data.cameras)[0]?.camera_id
          setSelectedCameraId(firstCamId || '')
        }
      })
      .catch(err => console.error("Error fetching store config:", err))
  }, [selectedStore])

  // Periodic frame snapshot refresh — append timestamp cachebuster
  useEffect(() => {
    if (!selectedCameraId) return
    const updateSnapshot = () => {
      setSnapshotUrl(`${API_URL}/data/frames/${selectedStore}/${selectedCameraId}/latest.jpg?t=${Date.now()}`)
    }
    updateSnapshot()
    const interval = setInterval(updateSnapshot, 1500)
    return () => clearInterval(interval)
  }, [selectedStore, selectedCameraId])

  // Periodic API metrics polling (every 5 s — WS KPI handles real-time)
  const fetchStoreData = useCallback(() => {
    fetch(`${API_URL}/api/v1/stores/${selectedStore}/metrics`)
      .then(res => res.json())
      .then(setMetrics)
      .catch(err => console.error("Error fetching metrics:", err))

    fetch(`${API_URL}/api/v1/stores/${selectedStore}/funnel`)
      .then(res => res.json())
      .then(setFunnel)
      .catch(err => console.error("Error fetching funnel:", err))

    fetch(`${API_URL}/api/v1/stores/${selectedStore}/anomalies`)
      .then(res => res.json())
      .then(data => setAnomalies(data.anomalies || []))
      .catch(err => console.error("Error fetching anomalies:", err))
  }, [selectedStore])

  useEffect(() => {
    fetchStoreData()
    const interval = setInterval(fetchStoreData, 5000)
    return () => clearInterval(interval)
  }, [fetchStoreData])

  // Periodic global health polling (every 3 s)
  useEffect(() => {
    const fetchHealth = () => {
      fetch(`${API_URL}/health`)
        .then(res => res.json())
        .then(setHealth)
        .catch(err => console.error("Error fetching health:", err))
    }
    fetchHealth()
    const interval = setInterval(fetchHealth, 3000)
    return () => clearInterval(interval)
  }, [])

  // Called immediately on each raw event from WebSocket
  const handleEvent = useCallback((event) => {
    setEvents((prev) => {
      const next = [...prev, event]
      return next.length > MAX_EVENTS ? next.slice(-MAX_EVENTS) : next
    })
    totalEventsRef.current += 1
    setTotalEvents(totalEventsRef.current)
  }, [])

  // Called every 2 seconds with aggregated KPI data from the server-side accumulator
  const handleKpi = useCallback((kpi) => {
    setKpiData(kpi)
  }, [])

  const connectionState = useWebSocket(handleEvent, handleKpi)

  // Filter events for the selected store only
  const filteredEvents = events.filter(e => e.store_id === selectedStore)

  // ── KPI data assembly ────────────────────────────────────────────────────
  const activeVisitors =
    kpiData?.active_visitors ??
    metrics?.active_visitors ??
    0

  const totalVisitorsToday = metrics?.total_visitors ?? kpiData?.total_visitors_today ?? 0
  const conversionRate = metrics?.conversion_rate ?? kpiData?.conversion_rate ?? 0.0
  const conversions = Math.round(totalVisitorsToday * conversionRate / 100)
  const peakZone = metrics?.peak_zone ?? kpiData?.peak_zone ?? 'None'
  const zoneCounts = metrics?.zone_breakdown ?? kpiData?.zone_counts ?? {}

  const eventTypeCounts = filteredEvents.reduce((acc, e) => {
    acc[e.event_type] = (acc[e.event_type] || 0) + 1
    return acc
  }, {})

  const displayKpiData = {
    total_visitors_today: totalVisitorsToday,
    active_visitors: activeVisitors,
    conversion_rate: conversionRate,
    conversions,
    peak_zone: peakZone,
    zone_counts: zoneCounts,
    event_type_counts: eventTypeCounts,
  }

  return (
    <div className="flex h-screen overflow-hidden" style={{ background: '#0A0F1E', fontFamily: 'Inter, sans-serif' }}>
      <Sidebar
        activeTab={activeTab}
        onTabChange={setActiveTab}
        connectionState={connectionState}
      />

      {/* Main content */}
      <main className="flex-1 overflow-y-auto p-6">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-xl font-bold text-white">
              {activeTab === 'dashboard' && 'Live Intelligence Dashboard'}
              {activeTab === 'metrics' && 'Store Metrics'}
              {activeTab === 'funnel' && 'Visitor Funnel'}
              {activeTab === 'anomalies' && 'Anomaly Detection'}
            </h2>
            <p className="text-sm mt-0.5" style={{ color: '#4B5563' }}>
              {storeConfig?.store_name || selectedStore} · {selectedCameraId || 'No Camera'} · Real-time
            </p>
          </div>
          <div className="flex items-center gap-3">
            {connectionState === 'open' ? (
              <div className="flex items-center gap-2 px-3 py-1.5 rounded-full" style={{ background: 'rgba(0,245,212,0.1)', border: '1px solid rgba(0,245,212,0.25)' }}>
                <Wifi size={12} style={{ color: '#00F5D4' }} />
                <span className="text-xs font-medium" style={{ color: '#00F5D4' }}>Live</span>
              </div>
            ) : (
              <div className="flex items-center gap-2 px-3 py-1.5 rounded-full" style={{ background: 'rgba(244,63,94,0.1)', border: '1px solid rgba(244,63,94,0.25)' }}>
                <WifiOff size={12} style={{ color: '#F43F5E' }} />
                <span className="text-xs font-medium" style={{ color: '#F43F5E' }}>Offline</span>
              </div>
            )}
          </div>
        </div>

        {/* Dashboard Tab */}
        {activeTab === 'dashboard' && (
          <div className="space-y-6 animate-fade-in">
            {/* KPI Cards */}
            <KPICards kpiData={displayKpiData} />

            {/* System Status Panel */}
            <SystemStatus health={health} selectedStore={selectedStore} snapshotOk={snapshotOk} />

            {/* Main grid */}
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
              {/* Event Log — 2/3 width */}
              <div className="xl:col-span-2 glass-card p-5">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-sm font-semibold text-white">Event Stream</h3>
                  <span className="text-xs font-mono" style={{ color: '#4B5563' }}>
                    last {Math.min(filteredEvents.length, 200)} events
                  </span>
                </div>
                <EventLog events={filteredEvents} />
              </div>

              {/* Live feed + Zone chart — 1/3 width */}
              <div className="space-y-4">
                {/* Live Monitor */}
                <div className="glass-card p-5 space-y-4">
                  <div className="flex items-center justify-between">
                    <h3 className="text-sm font-semibold text-white">Live Monitor</h3>
                    <div className="flex items-center gap-1.5">
                      <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
                      <span className="text-[10px] uppercase tracking-wider text-emerald-400 font-semibold">Live</span>
                    </div>
                  </div>

                  {/* Selectors */}
                  <div className="grid grid-cols-2 gap-2">
                    <div>
                      <label className="block text-[10px] text-gray-500 uppercase font-medium mb-1">Store</label>
                      <select
                        value={selectedStore}
                        onChange={(e) => setSelectedStore(e.target.value)}
                        className="w-full bg-slate-900 border border-slate-800 rounded-lg text-xs text-white p-2 outline-none focus:border-indigo-500"
                      >
                        <option value="store_1">Store 1 (Primary)</option>
                        <option value="store_2">Store 2</option>
                      </select>
                    </div>
                    <div>
                      <label className="block text-[10px] text-gray-500 uppercase font-medium mb-1">Camera</label>
                      <select
                        value={selectedCameraId}
                        onChange={(e) => setSelectedCameraId(e.target.value)}
                        className="w-full bg-slate-900 border border-slate-800 rounded-lg text-xs text-white p-2 outline-none focus:border-indigo-500"
                      >
                        {storeConfig && Object.values(storeConfig.cameras).map(cam => (
                          <option key={cam.camera_id} value={cam.camera_id}>{cam.name}</option>
                        ))}
                      </select>
                    </div>
                  </div>

                  {/* Preview Frame */}
                  <div className="relative aspect-video rounded-xl overflow-hidden bg-slate-950 border border-slate-900 group">
                    {viewMode === 'snapshot' ? (
                      <SmoothImage
                        src={snapshotUrl || `${API_URL}/data/frames/${selectedStore}/${selectedCameraId}/latest.jpg`}
                        alt="Live Camera Snapshot"
                        onError={() => setSnapshotOk(false)}
                        onLoadSuccess={() => setSnapshotOk(true)}
                        className="w-full h-full object-cover transition-transform duration-500 group-hover:scale-105"
                      />
                    ) : (
                      <img
                        src={storeConfig ? `${API_URL}/${storeConfig.layout_image}` : ''}
                        alt="Store Layout"
                        className="w-full h-full object-contain p-2"
                        onError={(e) => {
                          e.onerror = null;
                          e.target.src = "https://images.unsplash.com/photo-1557683316-973673baf926?w=640";
                        }}
                      />
                    )}
                    
                    <div className="absolute bottom-3 right-3 flex gap-1.5 font-sans">
                      <button
                        onClick={() => setViewMode('snapshot')}
                        className={`px-2 py-1 rounded text-[10px] font-semibold transition-all ${
                          viewMode === 'snapshot' ? 'bg-indigo-600 text-white shadow-lg' : 'bg-slate-900/80 text-gray-400 hover:text-white'
                        }`}
                      >
                        Snapshot
                      </button>
                      <button
                        onClick={() => setViewMode('layout')}
                        className={`px-2 py-1 rounded text-[10px] font-semibold transition-all ${
                          viewMode === 'layout' ? 'bg-indigo-600 text-white shadow-lg' : 'bg-slate-900/80 text-gray-400 hover:text-white'
                        }`}
                      >
                        Layout Map
                      </button>
                    </div>
                  </div>
                </div>

                <div className="glass-card p-5">
                  <LiveFeed recentEvents={filteredEvents} totalEvents={filteredEvents.length} />
                </div>

                <div className="glass-card p-5">
                  <h3 className="text-sm font-semibold text-white mb-4">Zone Activity</h3>
                  <ZoneChart zoneCounts={displayKpiData.zone_counts || {}} />
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Metrics Tab */}
        {activeTab === 'metrics' && (
          <div className="animate-fade-in space-y-6">
            <KPICards kpiData={displayKpiData} />
            <SystemStatus health={health} selectedStore={selectedStore} snapshotOk={snapshotOk} />
            <div className="glass-card p-6">
              <h3 className="text-sm font-semibold text-white mb-4">Zone Visitor Distribution</h3>
              <ZoneChart zoneCounts={displayKpiData.zone_counts || {}} />
            </div>
            {displayKpiData && (
              <div className="glass-card p-6">
                <h3 className="text-sm font-semibold text-white mb-4">Event Breakdown</h3>
                <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                  {Object.entries(displayKpiData.event_type_counts || {}).map(([type, count]) => (
                    <div key={type} className="p-3 rounded-xl" style={{ background: 'rgba(17,24,39,0.6)' }}>
                      <p className="text-xs" style={{ color: '#6B7280' }}>{type}</p>
                      <p className="text-xl font-bold text-white mt-1">{count}</p>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* Funnel Tab */}
        {activeTab === 'funnel' && (
          <div className="animate-fade-in">
            <div className="glass-card p-6">
              <h3 className="text-sm font-semibold text-white mb-6">Visitor Zone Funnel</h3>
              <div className="space-y-3">
                {funnel?.stages?.map((stage, idx, arr) => {
                  const count = stage.visitor_count
                  const maxCount = Math.max(...arr.map(s => s.visitor_count), 1)
                  const pct = Math.round((count / maxCount) * 100)
                  return (
                    <div key={stage.zone_id}>
                      <div className="flex justify-between text-xs mb-1.5">
                        <span style={{ color: '#9CA3AF' }}>{stage.zone_id.replace('_', ' ')}</span>
                        <span style={{ color: '#6B7280' }}>{count} visitors</span>
                      </div>
                      <div className="h-7 rounded-lg overflow-hidden" style={{ background: 'rgba(17,24,39,0.6)' }}>
                        <div
                          className="h-full rounded-lg transition-all duration-700 flex items-center px-3"
                          style={{
                            width: `${pct}%`,
                            background: 'linear-gradient(90deg, #6C63FF, #00F5D4)',
                            minWidth: count > 0 ? '40px' : '0',
                          }}
                        >
                          <span className="text-xs font-medium text-white">{pct}%</span>
                        </div>
                      </div>
                    </div>
                  )
                })}
                {(!funnel?.stages || funnel.stages.length === 0) && (
                  <p className="text-sm text-center py-8" style={{ color: '#374151' }}>Waiting for funnel data…</p>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Anomalies Tab */}
        {activeTab === 'anomalies' && (
          <div className="animate-fade-in">
            <div className="glass-card p-6">
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-sm font-semibold text-white">Operational Anomaly Detection</h3>
                {anomalies.length > 0 && (
                  <span
                    className="text-xs font-bold px-2 py-0.5 rounded-full"
                    style={{ background: 'rgba(244,63,94,0.12)', color: '#F43F5E', border: '1px solid rgba(244,63,94,0.25)' }}
                  >
                    {anomalies.length} active
                  </span>
                )}
              </div>
              <p className="text-xs mb-6" style={{ color: '#4B5563' }}>
                Anomalies are detected dynamically from recent event patterns in the database.
              </p>
              <div className="space-y-3">
                {anomalies.map((anomaly, idx) => {
                  // Map backend severity values (high/medium/low) to display colours
                  const severityMap = {
                    high:   { bg: 'rgba(244,63,94,0.06)',   border: 'rgba(244,63,94,0.2)',   text: '#F43F5E',  badge: 'rgba(244,63,94,0.12)'  },
                    medium: { bg: 'rgba(245,158,11,0.06)',  border: 'rgba(245,158,11,0.2)',  text: '#F59E0B',  badge: 'rgba(245,158,11,0.12)' },
                    low:    { bg: 'rgba(59,130,246,0.06)',   border: 'rgba(59,130,246,0.2)',   text: '#60A5FA',  badge: 'rgba(59,130,246,0.12)'  },
                  }
                  const sev = severityMap[anomaly.severity] || severityMap.medium
                  const title = anomaly.type.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, c => c.toUpperCase())
                  const timestamp = new Date(anomaly.detected_at).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })

                  return (
                    <div
                      key={idx}
                      className="p-4 rounded-xl animate-fade-in flex flex-col md:flex-row md:items-center justify-between gap-4"
                      style={{ background: sev.bg, border: `1px solid ${sev.border}` }}
                    >
                      <div className="flex-1 space-y-1">
                        <div className="flex items-center gap-2.5">
                          <h4 className="text-sm font-bold text-white tracking-tight">{title}</h4>
                          <span
                            className="text-[9px] font-bold px-2 py-0.5 rounded uppercase tracking-wider font-mono"
                            style={{ background: sev.badge, color: sev.text, border: `1px solid ${sev.border}` }}
                          >
                            {anomaly.severity}
                          </span>
                          {anomaly.affected_zone && (
                            <span className="text-[11px] font-medium text-gray-400">
                              · {anomaly.affected_zone}
                            </span>
                          )}
                        </div>
                        <p className="text-xs text-gray-300 leading-relaxed">{anomaly.detail}</p>
                      </div>
                      <div className="text-right md:flex-shrink-0">
                        <span className="text-[10px] text-gray-500 font-mono">
                          Detected: {timestamp}
                        </span>
                      </div>
                    </div>
                  )
                })}
                {anomalies.length === 0 && (
                  <div className="flex flex-col items-center justify-center py-16 gap-3">
                    <div className="w-12 h-12 rounded-full flex items-center justify-center" style={{ background: 'rgba(59,130,246,0.1)' }}>
                      <span className="text-xl text-blue-400">✓</span>
                    </div>
                    <p className="text-sm text-blue-400 font-semibold tracking-wide">No active anomalies detected</p>
                    <p className="text-xs" style={{ color: '#4B5563' }}>Store operations are running within normal parameters.</p>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  )
}
