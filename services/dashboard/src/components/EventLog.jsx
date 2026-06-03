import { useEffect, useRef } from 'react'

const EVENT_STYLES = {
  ENTRY: { bg: 'rgba(0,245,212,0.08)', border: 'rgba(0,245,212,0.25)', dot: '#00F5D4', label: '#00F5D4' },
  EXIT: { bg: 'rgba(244,63,94,0.08)', border: 'rgba(244,63,94,0.25)', dot: '#F43F5E', label: '#F43F5E' },
  ZONE_ENTER: { bg: 'rgba(108,99,255,0.08)', border: 'rgba(108,99,255,0.25)', dot: '#6C63FF', label: '#818CF8' },
  ZONE_EXIT: { bg: 'rgba(75,85,99,0.12)', border: 'rgba(75,85,99,0.25)', dot: '#6B7280', label: '#9CA3AF' },
  ZONE_DWELL: { bg: 'rgba(245,158,11,0.08)', border: 'rgba(245,158,11,0.25)', dot: '#F59E0B', label: '#F59E0B' },
}

function formatTime(iso) {
  try {
    return new Date(iso).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  } catch {
    return '—'
  }
}

function EventRow({ event }) {
  const style = EVENT_STYLES[event.event_type] || EVENT_STYLES.ZONE_EXIT
  return (
    <div
      className="flex items-center gap-3 px-4 py-2.5 rounded-xl mb-1 animate-fade-in transition-all duration-200"
      style={{ background: style.bg, border: `1px solid ${style.border}` }}
    >
      <span className="status-dot flex-shrink-0" style={{ backgroundColor: style.dot }} />
      <span className="event-badge" style={{ background: `${style.dot}18`, color: style.label }}>
        {event.event_type}
      </span>
      {event.zone_id && (
        <span className="text-xs font-mono" style={{ color: '#6B7280' }}>
          {event.zone_id.replace('_', ' ')}
        </span>
      )}
      <span className="text-xs flex-1 truncate" style={{ color: '#9CA3AF' }}>
        {event.visitor_id?.slice(0, 8)}…
      </span>
      <span className="text-xs font-mono flex-shrink-0" style={{ color: '#4B5563' }}>
        {formatTime(event.timestamp)}
      </span>
    </div>
  )
}

export function EventLog({ events }) {
  const bottomRef = useRef(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [events.length])

  return (
    <div className="overflow-y-auto" style={{ maxHeight: '360px' }}>
      {events.length === 0 ? (
        <div className="flex items-center justify-center py-12" style={{ color: '#374151' }}>
          <p className="text-sm">Waiting for events from pipeline…</p>
        </div>
      ) : (
        events.map((event, idx) => <EventRow key={`${event.event_id}-${idx}`} event={event} />)
      )}
      <div ref={bottomRef} />
    </div>
  )
}
