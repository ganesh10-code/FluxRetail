import { Activity } from 'lucide-react'

const EVENT_STYLES = {
  ENTRY:     { color: '#00F5D4', label: 'ENTRY' },
  EXIT:      { color: '#F43F5E', label: 'EXIT' },
  ZONE_ENTER:{ color: '#6C63FF', label: 'ZONE IN' },
  ZONE_EXIT: { color: '#6B7280', label: 'ZONE OUT' },
  ZONE_DWELL:{ color: '#F59E0B', label: 'DWELL' },
}

const VISITOR_PREFIXES = ['Visitor', 'Shopper', 'Returning', 'Guest', 'Customer']

/**
 * Convert a UUID visitor_id to a deterministic, business-readable label.
 * Consistent with the logic in EventLog.jsx — same UUID → same label.
 */
function toVisitorLabel(visitorId) {
  if (!visitorId) return '—'
  const seed = parseInt(visitorId.replace(/-/g, '').slice(-6), 16) || 0
  const prefix = VISITOR_PREFIXES[seed % VISITOR_PREFIXES.length]
  const num = String(seed % 1000).padStart(3, '0')
  return `${prefix}-${num}`
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
        const visitorLabel = toVisitorLabel(event.visitor_id)
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
                → {event.zone_id.replace(/_ZONE/g, '').replace(/_/g, ' ')}
              </span>
            )}
            <span className="text-xs truncate font-medium" style={{ color: '#4B5563' }}>
              {visitorLabel}
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
