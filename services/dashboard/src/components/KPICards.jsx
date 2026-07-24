import { Users, UserCheck, TrendingUp, ShoppingCart, Zap } from 'lucide-react'

const KPI_CONFIG = [
  {
    key: 'total_visitors_today',
    label: 'Visitors Today',
    icon: Users,
    color: '#6C63FF',
    format: (v) => v?.toLocaleString() ?? '—',
  },
  {
    key: 'active_visitors',
    label: 'Active Now',
    icon: Zap,
    color: '#00F5D4',
    format: (v) => v ?? '—',
  },
  {
    key: 'conversion_rate',
    label: 'Conversion Rate',
    icon: TrendingUp,
    color: '#F59E0B',
    format: (v) => v != null ? `${v}%` : '—',
  },
  {
    key: 'conversions',
    label: 'Converted',
    icon: ShoppingCart,
    color: '#818CF8',
    format: (v) => v ?? '—',
  },
  {
    key: 'peak_zone',
    label: 'Peak Zone',
    icon: UserCheck,
    color: '#F43F5E',
    format: (v) => v ? v.replace(/_ZONE/g, '').replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, c => c.toUpperCase()) : '—',
  },
]

function KPICard({ config, value, isNew }) {
  const Icon = config.icon
  return (
    <div
      className={`glass-card p-6 flex flex-col justify-between transition-all duration-300 hover:scale-[1.02] hover:shadow-lg ${isNew ? 'animate-slide-up' : ''}`}
      style={{ borderLeft: `4px solid ${config.color}`, borderColor: `${config.color}22` }}
    >
      <div className="flex items-center justify-between mb-3">
        <span className="text-xs font-semibold tracking-wider uppercase" style={{ color: '#9CA3AF' }}>{config.label}</span>
        <div
          className="w-8 h-8 rounded-lg flex items-center justify-center"
          style={{ background: `${config.color}15` }}
        >
          <Icon size={16} style={{ color: config.color }} />
        </div>
      </div>
      <div
        className="text-3xl font-bold tracking-tight text-white font-sans"
      >
        {config.format(value)}
      </div>
    </div>
  )
}

export function KPICards({ kpiData }) {
  return (
    <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-5 gap-6">
      {KPI_CONFIG.map((cfg) => (
        <KPICard
          key={cfg.key}
          config={cfg}
          value={kpiData?.[cfg.key]}
          isNew={false}
        />
      ))}
    </div>
  )
}
