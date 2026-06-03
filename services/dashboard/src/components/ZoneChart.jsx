import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell
} from 'recharts'

const ZONE_COLORS = {
  ENTRY_ZONE: '#00F5D4',
  BILLING_ZONE: '#6C63FF',
  MAKEUP_ZONE: '#F43F5E',
  SKINCARE_ZONE: '#F59E0B',
  CENTER_ZONE: '#818CF8',
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null
  return (
    <div className="glass-card px-4 py-3" style={{ border: '1px solid rgba(108,99,255,0.3)' }}>
      <p className="text-sm font-semibold text-white">{label?.replace('_', ' ')}</p>
      <p className="text-sm" style={{ color: '#00F5D4' }}>{payload[0].value} visitors</p>
    </div>
  )
}

export function ZoneChart({ zoneCounts }) {
  const data = Object.entries(zoneCounts || {}).map(([zone, count]) => ({
    zone: zone.replace('_ZONE', ''),
    full: zone,
    count,
  }))

  if (data.length === 0) {
    return (
      <div className="flex items-center justify-center h-48" style={{ color: '#374151' }}>
        <p className="text-sm">Waiting for zone data…</p>
      </div>
    )
  }

  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart data={data} margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" />
        <XAxis
          dataKey="zone"
          tick={{ fill: '#6B7280', fontSize: 11 }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          tick={{ fill: '#6B7280', fontSize: 11 }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(108,99,255,0.06)' }} />
        <Bar dataKey="count" radius={[6, 6, 0, 0]}>
          {data.map((entry) => (
            <Cell key={entry.full} fill={ZONE_COLORS[entry.full] || '#6C63FF'} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}
