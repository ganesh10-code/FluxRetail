import { Activity } from 'lucide-react'

const EVENT_STYLES = {
  ENTRY: { color: '#00F5D4', label: 'ENTRY' },
  EXIT: { color: '#F43F5E', label: 'EXIT' },
  ZONE_ENTER: { color: '#6C63FF', label: 'ZONE IN' },
  ZONE_EXIT: { color: '#6B7280', label: 'ZONE OUT' },
  ZONE_DWELL: { color: '#F59E0B', label: 'DWELL' },
}

export function LiveFeed({ recentEvents, totalEvents }) {
  const recent = recentEvents.slice(-6).reverse()
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Activity size={14} style={{ color: '#00F5D4' }} />
          <span className="text-xs font-medium" style={{ color: '#6B7280' }}>Live Events</span>
        </div>
        <span className="text-xs font-mono" style={{ color: '#4B5563' }}>
          {totalEvents.toLocaleString()} total
        </span>
      </div>
      {recent.map((event, idx) => {
        const style = EVENT_STYLES[event.event_type] || EVENT_STYLES.ZONE_EXIT
        return (
          <div
            key={`${event.event_id}-${idx}`}
            className="flex items-center gap-2 animate-fade-in"
          >
            <div
              className="w-1.5 h-1.5 rounded-full flex-shrink-0"
              style={{ backgroundColor: style.color }}
            />
            <span className="text-xs font-mono font-medium" style={{ color: style.color }}>
              {style.label}
            </span>
            {event.zone_id && (
              <span className="text-xs" style={{ color: '#6B7280' }}>
                → {event.zone_id.replace('_ZONE', '')}
              </span>
            )}
            <span className="text-xs truncate" style={{ color: '#4B5563' }}>
              {event.visitor_id?.slice(0, 8)}
            </span>
          </div>
        )
      })}
      {recent.length === 0 && (
        <p className="text-xs" style={{ color: '#374151' }}>No events yet…</p>
      )}
    </div>
  )
}
