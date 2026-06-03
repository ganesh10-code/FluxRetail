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
    format: (v) => v ? v.replace('_', ' ') : '—',
  },
]

function KPICard({ config, value, isNew }) {
  const Icon = config.icon
  return (
    <div
      className={`glass-card p-5 flex flex-col gap-3 transition-all duration-300 ${isNew ? 'animate-slide-up' : ''}`}
      style={{ borderColor: `${config.color}22` }}
    >
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium" style={{ color: '#6B7280' }}>{config.label}</span>
        <div
          className="w-8 h-8 rounded-lg flex items-center justify-center"
          style={{ background: `${config.color}18` }}
        >
          <Icon size={16} style={{ color: config.color }} />
        </div>
      </div>
      <div
        className="text-3xl font-bold tracking-tight"
        style={{ color: config.color }}
      >
        {config.format(value)}
      </div>
    </div>
  )
}

export function KPICards({ kpiData }) {
  return (
    <div className="grid grid-cols-2 xl:grid-cols-5 gap-4">
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
