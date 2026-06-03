import { useState, useCallback, useRef } from 'react'
import { KPICards } from './components/KPICards'
import { EventLog } from './components/EventLog'
import { ZoneChart } from './components/ZoneChart'
import { LiveFeed } from './components/LiveFeed'
import { Sidebar } from './components/Sidebar'
import { useWebSocket } from './hooks/useWebSocket'
import { RefreshCw, Wifi, WifiOff } from 'lucide-react'

const MAX_EVENTS = 200  // keep last 200 events in memory

export default function App() {
  const [activeTab, setActiveTab] = useState('dashboard')
  const [events, setEvents] = useState([])
  const [kpiData, setKpiData] = useState(null)
  const totalEventsRef = useRef(0)
  const [totalEvents, setTotalEvents] = useState(0)

  // Called immediately on each raw event from WebSocket
  const handleEvent = useCallback((event) => {
    setEvents((prev) => {
      const next = [...prev, event]
      return next.length > MAX_EVENTS ? next.slice(-MAX_EVENTS) : next
    })
    totalEventsRef.current += 1
    setTotalEvents(totalEventsRef.current)
  }, [])

  // Called every 2 seconds with aggregated KPI data
  const handleKpi = useCallback((kpi) => {
    setKpiData(kpi)
  }, [])

  const connectionState = useWebSocket(handleEvent, handleKpi)

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
              store_001 · cam_01 · Real-time
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
            <KPICards kpiData={kpiData} />

            {/* Main grid */}
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
              {/* Event Log — 2/3 width */}
              <div className="xl:col-span-2 glass-card p-5">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-sm font-semibold text-white">Event Stream</h3>
                  <span className="text-xs font-mono" style={{ color: '#4B5563' }}>
                    last {Math.min(events.length, 200)} events
                  </span>
                </div>
                <EventLog events={events} />
              </div>

              {/* Live feed + Zone chart — 1/3 width */}
              <div className="space-y-4">
                <div className="glass-card p-5">
                  <LiveFeed recentEvents={events} totalEvents={totalEvents} />
                </div>

                <div className="glass-card p-5">
                  <h3 className="text-sm font-semibold text-white mb-4">Zone Activity</h3>
                  <ZoneChart zoneCounts={kpiData?.zone_counts || {}} />
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Metrics Tab */}
        {activeTab === 'metrics' && (
          <div className="animate-fade-in space-y-6">
            <KPICards kpiData={kpiData} />
            <div className="glass-card p-6">
              <h3 className="text-sm font-semibold text-white mb-4">Zone Visitor Distribution</h3>
              <ZoneChart zoneCounts={kpiData?.zone_counts || {}} />
            </div>
            {kpiData && (
              <div className="glass-card p-6">
                <h3 className="text-sm font-semibold text-white mb-4">Event Breakdown</h3>
                <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                  {Object.entries(kpiData.event_type_counts || {}).map(([type, count]) => (
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
                {Object.entries(kpiData?.zone_counts || {}).map(([zone, count], idx, arr) => {
                  const maxCount = Math.max(...arr.map(([, c]) => c), 1)
                  const pct = Math.round((count / maxCount) * 100)
                  return (
                    <div key={zone}>
                      <div className="flex justify-between text-xs mb-1.5">
                        <span style={{ color: '#9CA3AF' }}>{zone.replace('_', ' ')}</span>
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
                {Object.keys(kpiData?.zone_counts || {}).length === 0 && (
                  <p className="text-sm text-center py-8" style={{ color: '#374151' }}>Waiting for zone data…</p>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Anomalies Tab */}
        {activeTab === 'anomalies' && (
          <div className="animate-fade-in">
            <div className="glass-card p-6">
              <h3 className="text-sm font-semibold text-white mb-4">Operational Anomaly Detection</h3>
              <p className="text-xs mb-6" style={{ color: '#4B5563' }}>
                Anomalies are detected from live event patterns: queue build-up, no entries, high exit rates.
              </p>
              <div className="space-y-3">
                {/* Static demo anomaly display */}
                <div className="p-4 rounded-xl" style={{ background: 'rgba(245,158,11,0.08)', border: '1px solid rgba(245,158,11,0.2)' }}>
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-xs font-bold" style={{ color: '#F59E0B' }}>QUEUE_BUILD</span>
                    <span className="text-xs" style={{ color: '#6B7280' }}>· warning</span>
                  </div>
                  <p className="text-sm" style={{ color: '#9CA3AF' }}>Monitored: &gt;3 visitors at billing counter in 10 min</p>
                </div>
                <div className="p-4 rounded-xl" style={{ background: 'rgba(244,63,94,0.06)', border: '1px solid rgba(244,63,94,0.15)' }}>
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-xs font-bold" style={{ color: '#F43F5E' }}>HIGH_EXIT_RATE</span>
                    <span className="text-xs" style={{ color: '#6B7280' }}>· warning</span>
                  </div>
                  <p className="text-sm" style={{ color: '#9CA3AF' }}>Monitored: &gt;5 exits in 5 minutes</p>
                </div>
                <div className="p-4 rounded-xl" style={{ background: 'rgba(108,99,255,0.06)', border: '1px solid rgba(108,99,255,0.15)' }}>
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-xs font-bold" style={{ color: '#818CF8' }}>NO_ENTRIES</span>
                    <span className="text-xs" style={{ color: '#6B7280' }}>· info</span>
                  </div>
                  <p className="text-sm" style={{ color: '#9CA3AF' }}>Monitored: 0 entries in last 30 minutes</p>
                </div>
                <p className="text-xs text-center pt-2" style={{ color: '#374151' }}>
                  Live anomaly data available at <code className="font-mono">/stores/store_001/anomalies</code>
                </p>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  )
}
