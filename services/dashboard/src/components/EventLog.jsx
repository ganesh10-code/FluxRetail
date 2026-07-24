import React from 'react'

const EVENT_STYLES = {
  ENTRY:     { bg: 'rgba(0,245,212,0.08)',   border: 'rgba(0,245,212,0.25)',   dot: '#00F5D4', label: '#00F5D4' },
  EXIT:      { bg: 'rgba(244,63,94,0.08)',   border: 'rgba(244,63,94,0.25)',   dot: '#F43F5E', label: '#F43F5E' },
  ZONE_ENTER:{ bg: 'rgba(108,99,255,0.08)', border: 'rgba(108,99,255,0.25)', dot: '#6C63FF', label: '#818CF8' },
  ZONE_EXIT: { bg: 'rgba(75,85,99,0.12)',   border: 'rgba(75,85,99,0.25)',   dot: '#6B7280', label: '#9CA3AF' },
  ZONE_DWELL:{ bg: 'rgba(245,158,11,0.08)', border: 'rgba(245,158,11,0.25)', dot: '#F59E0B', label: '#F59E0B' },
}

const VISITOR_PREFIXES = ['Visitor', 'Shopper', 'Returning', 'Guest', 'Customer']

/**
 * Convert a UUID visitor_id to a deterministic, business-readable label.
 * E.g. "3f7a2c..." → "Shopper-204"
 * Stable for the lifetime of the session — same UUID always maps to the same label.
 */
function toVisitorLabel(visitorId) {
  if (!visitorId) return '—'
  // Use last 6 hex chars of UUID as a numeric seed
  const seed = parseInt(visitorId.replace(/-/g, '').slice(-6), 16) || 0
  const prefix = VISITOR_PREFIXES[seed % VISITOR_PREFIXES.length]
  const num = String(seed % 1000).padStart(3, '0')
  return `${prefix}-${num}`
}

function formatTime(iso) {
  try {
    return new Date(iso).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  } catch {
    return '—'
  }
}

const EventRow = React.memo(function EventRow({ event }) {
  const style = EVENT_STYLES[event.event_type] || EVENT_STYLES.ZONE_EXIT
  const visitorLabel = toVisitorLabel(event.visitor_id)
  return (
    <div
      className="flex items-center gap-3 px-4 py-2.5 rounded-xl mb-1.5 animate-fade-in transition-all duration-200 hover:bg-slate-900/40"
      style={{ background: style.bg, border: `1px solid ${style.border}` }}
    >
      <span className="status-dot flex-shrink-0" style={{ backgroundColor: style.dot }} />
      <span className="event-badge" style={{ background: `${style.dot}18`, color: style.label }}>
        {event.event_type}
      </span>
      {event.zone_id && (
        <span className="text-xs font-mono" style={{ color: '#9CA3AF' }}>
          {event.zone_id.replace(/_/g, ' ')}
        </span>
      )}
      <span className="text-xs flex-1 truncate font-medium text-gray-300">
        {visitorLabel}
      </span>
      <span className="text-xs font-mono flex-shrink-0 text-gray-400">
        {formatTime(event.timestamp)}
      </span>
    </div>
  )
})

export function EventLog({ events }) {
  return (
    <div
      className="overflow-y-auto pr-1"
      style={{
        maxHeight: '360px',
        minHeight: '360px',
        display: 'flex',
        flexDirection: 'column-reverse',
        overflowAnchor: 'none',
      }}
    >
      {events.length === 0 ? (
        <div className="flex items-center justify-center py-12" style={{ color: '#374151' }}>
          <p className="text-sm">Waiting for events from pipeline…</p>
        </div>
      ) : (
        <div>
          {[...events].reverse().map((event) => (
            <EventRow key={event.event_id} event={event} />
          ))}
        </div>
      )}
    </div>
  )
}
