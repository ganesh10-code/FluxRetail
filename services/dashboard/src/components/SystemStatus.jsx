import { Database, Server, Cpu, RefreshCw, Camera } from 'lucide-react'

const getStatusStyles = (status) => {
  if (status === 'ok') return { dot: '#00F5D4', color: '#00F5D4', bg: 'rgba(0, 245, 212, 0.08)', label: 'Online' }
  if (status === 'warning') return { dot: '#F59E0B', color: '#F59E0B', bg: 'rgba(245, 158, 11, 0.08)', label: 'Warning' }
  return { dot: '#F43F5E', color: '#F43F5E', bg: 'rgba(244, 63, 94, 0.08)', label: 'Offline' }
}

/**
 * StatusCard — stacked layout so name never overlaps the status badge.
 * Row 1: icon (left) + status badge (right)
 * Row 2: service name (full width, no truncation)
 * Row 3: detail / latency
 */
function StatusCard({ name, icon: Icon, status, detail, latency }) {
  const styles = getStatusStyles(status)
  return (
    <div
      className="flex flex-col px-3 py-3 rounded-xl bg-slate-950/60 border border-slate-900 transition-all hover:border-slate-800"
      style={{ minWidth: 0 }}
    >
      {/* Row 1: icon + status badge */}
      <div className="flex items-center justify-between mb-2">
        <div
          className="w-7 h-7 rounded-lg flex items-center justify-center flex-shrink-0"
          style={{ background: styles.bg }}
        >
          <Icon size={13} style={{ color: styles.color }} />
        </div>
        <span className="flex items-center gap-1 flex-shrink-0">
          <span
            className="w-1.5 h-1.5 rounded-full flex-shrink-0"
            style={{ backgroundColor: styles.dot }}
          />
          <span
            className="text-[9px] font-bold uppercase tracking-widest"
            style={{ color: styles.color }}
          >
            {styles.label}
          </span>
        </span>
      </div>

      {/* Row 2: service name — full width, never overlaps badge */}
      <span className="text-xs font-semibold text-gray-200 leading-tight">
        {name}
      </span>

      {/* Row 3: latency or detail */}
      <p className="text-[10px] text-gray-500 font-mono mt-1 truncate">
        {latency != null ? `${latency}ms` : detail || '—'}
      </p>
    </div>
  )
}

export function SystemStatus({ health, selectedStore, snapshotOk }) {
  const comps = health?.components || {}

  // 1. Postgres
  const pgStatus = comps.postgres?.status || 'error'
  const pgLatency = comps.postgres?.latency_ms

  // 2. Redis
  const redisStatus = comps.redis?.status || 'error'
  const redisLatency = comps.redis?.latency_ms

  // 3. Kafka
  const kafkaStatus = comps.kafka?.status || 'error'
  const kafkaDetail = comps.kafka?.detail || 'Unreachable'

  // 4. Pipeline
  const pipelineStatus = comps.pipeline_heartbeat?.status || 'warning'
  const pipelineDetail = comps.pipeline_heartbeat?.detail || 'Not Started'

  // 5. Active cameras running YOLO detection
  const isStore1 = selectedStore === 'store_1'
  const activeCamCount = isStore1 ? 2 : 0
  const activeCamStatus = activeCamCount > 0 ? 'ok' : 'warning'
  const activeCamDetail = isStore1 ? 'cam_entry_01, cam_zone_01' : '0 — Inactive'

  // 6. Snapshot Refresh
  const snapshotStatus = snapshotOk ? 'ok' : 'warning'
  const snapshotDetail = snapshotOk ? 'Every 1.5s' : 'Stale feed'

  return (
    <div className="glass-card p-4 space-y-3">
      <div className="flex items-center justify-between border-b border-slate-900 pb-2">
        <h4 className="text-xs font-bold uppercase tracking-wider text-gray-400">
          Enterprise Operations Console
        </h4>
        <span className="text-[10px] font-mono text-gray-500">
          Last Check: {health?.timestamp ? new Date(health.timestamp).toLocaleTimeString() : 'Never'}
        </span>
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-6 gap-2">
        <StatusCard name="PostgreSQL"    icon={Database}  status={pgStatus}       latency={pgLatency} />
        <StatusCard name="Redis Cache"   icon={Server}    status={redisStatus}    latency={redisLatency} />
        <StatusCard name="Kafka Bus"     icon={Cpu}       status={kafkaStatus}    detail={kafkaDetail} />
        <StatusCard name="CV Pipeline"   icon={Cpu}       status={pipelineStatus} detail={pipelineDetail} />
        <StatusCard name="Active Cams"   icon={Camera}    status={activeCamStatus} detail={activeCamDetail} />
        <StatusCard name="Snapshots"     icon={RefreshCw} status={snapshotStatus} detail={snapshotDetail} />
      </div>
    </div>
  )
}
