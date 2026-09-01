import { useState, useCallback } from 'react';
import { api } from '../api/client';
import type { Instance } from '../api/client';
import { InstanceGrid } from '../components/Instances/InstanceGrid';
import { InstanceLog } from '../components/Instances/InstanceLog';
import { useVisibilityAwareInterval } from '../hooks/useVisibilityAwareInterval';
import { useWebSocket } from '../hooks/useWebSocket';

export function Dashboard() {
  const [instances, setInstances] = useState<Instance[]>([]);
  const [stats, setStats] = useState<{ tasks: Record<string, number>; running_instances: number } | null>(null);
  const [logInstanceId, setLogInstanceId] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [inst, st] = await Promise.all([api.listInstances(), api.stats()]);
      setInstances(inst);
      setStats(st);
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  const { isConnected } = useWebSocket(['tasks', 'system'], refresh);
  useVisibilityAwareInterval(refresh, isConnected ? 30000 : 5000);

  return (
    <div className="space-y-6">
      {error && (
        <div className="bg-red-500/20 text-red-400 px-4 py-2 rounded text-sm">
          Error: {error}
        </div>
      )}
      {/* Stats bar */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          {Object.entries(stats.tasks).map(([status, count]) => (
            <div key={status} className="bg-gray-800 border border-gray-700/60 rounded-xl p-4 text-center shadow-sm">
              <p className="text-2xl font-semibold tracking-tight text-foreground tabular-nums">{count}</p>
              <p className="text-xs text-gray-500 capitalize mt-0.5">{status.replace('_', ' ')}</p>
            </div>
          ))}
          <div className="bg-gray-800 border border-gray-700/60 rounded-xl p-4 text-center shadow-sm">
            <p className="text-2xl font-semibold tracking-tight text-green-400 tabular-nums">{stats.running_instances}</p>
            <p className="text-xs text-gray-500 mt-0.5">Running</p>
          </div>
        </div>
      )}

      {/* Instances */}
      <div>
        <h2 className="text-foreground font-semibold mb-3">Instances</h2>
        <InstanceGrid
          instances={instances}
          onRefresh={refresh}
          onViewLogs={(id) => setLogInstanceId(id)}
        />
      </div>

      {/* Log modal */}
      {logInstanceId !== null && (
        <InstanceLog key={logInstanceId} instanceId={logInstanceId} onClose={() => setLogInstanceId(null)} />
      )}

    </div>
  );
}
