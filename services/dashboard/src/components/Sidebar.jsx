import { Activity, BarChart2, GitBranch, AlertTriangle, Heart } from 'lucide-react'

const NAV_ITEMS = [
  { id: 'dashboard', label: 'Live Dashboard', icon: Activity },
  { id: 'metrics', label: 'Store Metrics', icon: BarChart2 },
  { id: 'funnel', label: 'Visitor Funnel', icon: GitBranch },
  { id: 'anomalies', label: 'Anomalies', icon: AlertTriangle },
]

export function Sidebar({ activeTab, onTabChange, connectionState }) {
  return (
    <aside className="w-64 flex-shrink-0 flex flex-col h-full" style={{ background: '#0D1426', borderRight: '1px solid rgba(108,99,255,0.12)' }}>
      {/* Logo */}
      <div className="px-6 py-6">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'linear-gradient(135deg, #6C63FF, #00F5D4)' }}>
            <Activity size={16} className="text-white" />
          </div>
          <div>
            <h1 className="text-white font-bold text-base tracking-tight">FluxRetail</h1>
            <p className="text-xs" style={{ color: '#4B5563' }}>Intelligence Platform</p>
          </div>
        </div>
      </div>

      {/* Connection status */}
      <div className="px-6 pb-4">
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg" style={{ background: 'rgba(17,24,39,0.6)' }}>
          <span
            className="status-dot animate-pulse-dot"
            style={{
              backgroundColor:
                connectionState === 'open' ? '#00F5D4' :
                connectionState === 'connecting' ? '#F59E0B' : '#F43F5E',
            }}
          />
          <span className="text-xs font-medium" style={{ color: '#9CA3AF' }}>
            {connectionState === 'open' ? 'Live Stream' :
             connectionState === 'connecting' ? 'Connecting…' : 'Disconnected'}
          </span>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-3">
        {NAV_ITEMS.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            id={`nav-${id}`}
            onClick={() => onTabChange(id)}
            className="w-full flex items-center gap-3 px-3 py-2.5 mb-1 rounded-xl text-sm font-medium transition-all duration-200"
            style={{
              background: activeTab === id ? 'rgba(108,99,255,0.15)' : 'transparent',
              color: activeTab === id ? '#6C63FF' : '#6B7280',
              borderLeft: activeTab === id ? '2px solid #6C63FF' : '2px solid transparent',
            }}
          >
            <Icon size={16} />
            {label}
          </button>
        ))}
      </nav>

      {/* Footer */}
      <div className="px-6 py-4">
        <div className="flex items-center gap-2 text-xs" style={{ color: '#374151' }}>
          <Heart size={12} style={{ color: '#6C63FF' }} />
          <span>FluxRetail v1.0</span>
        </div>
      </div>
    </aside>
  )
}
