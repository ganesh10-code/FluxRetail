import { useEffect, useRef, useState, useCallback } from 'react'

const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8000'
const RECONNECT_DELAY_MS = 3000
const MAX_RECONNECT_ATTEMPTS = 20

/**
 * Reconnecting WebSocket hook.
 *
 * Handles:
 * - Auto-reconnect with exponential-ish backoff
 * - Connection state tracking
 * - Separate callbacks for 'event' and 'kpi' message types
 *
 * @param {function} onEvent  - Called with raw retail event payload
 * @param {function} onKpi    - Called with compact KPI payload
 */
export function useWebSocket(onEvent, onKpi) {
  const wsRef = useRef(null)
  const attemptsRef = useRef(0)
  const timerRef = useRef(null)
  const [connectionState, setConnectionState] = useState('connecting') // connecting | open | closed

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return

    const url = `${WS_URL}/ws/live`
    const ws = new WebSocket(url)
    wsRef.current = ws

    ws.onopen = () => {
      attemptsRef.current = 0
      setConnectionState('open')
      console.log('[FluxRetail WS] Connected')
    }

    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data)
        if (msg.message_type === 'event') {
          onEvent?.(msg.payload)
        } else if (msg.message_type === 'kpi') {
          onKpi?.(msg.payload)
        }
      } catch {
        // Ignore malformed messages
      }
    }

    ws.onerror = () => {
      setConnectionState('closed')
    }

    ws.onclose = () => {
      setConnectionState('closed')
      const attempt = attemptsRef.current
      if (attempt < MAX_RECONNECT_ATTEMPTS) {
        const delay = Math.min(RECONNECT_DELAY_MS * Math.pow(1.3, attempt), 15000)
        console.log(`[FluxRetail WS] Reconnecting in ${Math.round(delay / 1000)}s (attempt ${attempt + 1})`)
        attemptsRef.current += 1
        timerRef.current = setTimeout(connect, delay)
      }
    }
  }, [onEvent, onKpi])

  useEffect(() => {
    connect()
    return () => {
      clearTimeout(timerRef.current)
      wsRef.current?.close()
    }
  }, [connect])

  return connectionState
}
