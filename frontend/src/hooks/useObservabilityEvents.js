import { useState, useEffect, useRef, useCallback } from 'react';

/**
 * Custom React hook for SupplySense AI Server-Sent Events (SSE) live state synchronization.
 *
 * Implements Phase D Real-Time Reliability:
 *  - 4 formal lifecycle states: 'CONNECTING' | 'CONNECTED' | 'RECONNECTING' | 'OFFLINE'.
 *  - Exponential backoff retry loop with jitter (1s, 2s, 4s, 8s, max 15s).
 *  - 30-second heartbeat watchdog timer detecting stale network connections.
 *  - Cross-event deduplication via in-memory Set (seenEventIdsRef).
 *  - Automatic fallback to REST snapshot fetch on 'REST_SNAPSHOT_REQUIRED'.
 *  - Active alert tracking for payment gateway spikes and critical conditions.
 *  - Clean teardown on unmount with protection against React 18 Strict Mode double-connections.
 */

// Exponential backoff tiers in milliseconds
const BACKOFF_STEPS_MS = [1000, 2000, 4000, 8000, 15000];
const HEARTBEAT_TIMEOUT_MS = 30000; // 30s watchdog (backend emits every 15s)

export function useObservabilityEvents({ onEvent, onSnapshotRequired } = {}) {
  const [connectionStatus, setConnectionStatus] = useState('CONNECTING');
  const [latestEvent, setLatestEvent] = useState(null);
  const [events, setEvents] = useState([]);
  const [activeAlerts, setActiveAlerts] = useState([]);

  const seenEventIdsRef = useRef(new Set());
  const eventSourceRef = useRef(null);
  const onEventRef = useRef(onEvent);
  const onSnapshotRequiredRef = useRef(onSnapshotRequired);
  
  // Reconnect and watchdog refs
  const retryCountRef = useRef(0);
  const reconnectTimerRef = useRef(null);
  const watchdogTimerRef = useRef(null);
  const isUnmountedRef = useRef(false);

  // Keep callback refs fresh without re-triggering effects
  useEffect(() => {
    onEventRef.current = onEvent;
    onSnapshotRequiredRef.current = onSnapshotRequired;
  }, [onEvent, onSnapshotRequired]);

  // Clean teardown helper
  const teardown = useCallback(() => {
    if (watchdogTimerRef.current) {
      clearTimeout(watchdogTimerRef.current);
      watchdogTimerRef.current = null;
    }
    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
  }, []);

  // Heartbeat watchdog: resets 30s timer on any received event or heartbeat
  const resetWatchdog = useCallback((reconnectFn) => {
    if (watchdogTimerRef.current) {
      clearTimeout(watchdogTimerRef.current);
    }
    watchdogTimerRef.current = setTimeout(() => {
      if (isUnmountedRef.current) return;
      console.warn('[SSE Watchdog] No heartbeat or event received in 30s. Reconnecting...');
      setConnectionStatus('RECONNECTING');
      teardown();
      reconnectFn();
    }, HEARTBEAT_TIMEOUT_MS);
  }, [teardown]);

  const connect = useCallback(() => {
    if (isUnmountedRef.current) return;

    teardown();

    // If initial attempt, status is CONNECTING; otherwise RECONNECTING
    setConnectionStatus((prev) => (prev === 'CONNECTING' ? 'CONNECTING' : 'RECONNECTING'));

    const sseUrl = '/api/observability/events/stream';
    let es;
    try {
      es = new EventSource(sseUrl);
    } catch (err) {
      console.error('[SSE] Failed to construct EventSource:', err);
      scheduleReconnect();
      return;
    }
    eventSourceRef.current = es;

    const scheduleReconnect = () => {
      if (isUnmountedRef.current) return;
      teardown();

      const stepIndex = Math.min(retryCountRef.current, BACKOFF_STEPS_MS.length - 1);
      const baseDelay = BACKOFF_STEPS_MS[stepIndex];
      // +/- 20% jitter
      const jitter = baseDelay * 0.2 * (Math.random() * 2 - 1);
      const delay = Math.max(500, Math.round(baseDelay + jitter));

      retryCountRef.current += 1;
      setConnectionStatus(retryCountRef.current > 5 ? 'OFFLINE' : 'RECONNECTING');

      reconnectTimerRef.current = setTimeout(() => {
        if (!isUnmountedRef.current) {
          connect();
        }
      }, delay);
    };

    es.onopen = () => {
      if (isUnmountedRef.current) return;
      retryCountRef.current = 0;
      setConnectionStatus('CONNECTED');
      resetWatchdog(scheduleReconnect);
    };

    es.onerror = () => {
      if (isUnmountedRef.current) return;
      scheduleReconnect();
    };

    const handleEventMessage = (e) => {
      if (isUnmountedRef.current) return;

      // Any received frame resets watchdog
      resetWatchdog(scheduleReconnect);

      try {
        if (!e.data) return;
        const payload = JSON.parse(e.data);
        const eventId = payload.event_id || e.lastEventId;

        // Skip heartbeat internal payloads from event list, but watchdog was reset above
        if (payload.event_type === 'HEARTBEAT') {
          return;
        }

        // 1. Deduplicate by event_id
        if (eventId && seenEventIdsRef.current.has(eventId)) {
          return;
        }
        if (eventId) {
          seenEventIdsRef.current.add(eventId);
          // Bound seen Set size
          if (seenEventIdsRef.current.size > 500) {
            const firstItems = Array.from(seenEventIdsRef.current).slice(0, 200);
            firstItems.forEach((id) => seenEventIdsRef.current.delete(id));
          }
        }

        setLatestEvent(payload);
        setEvents((prev) => [payload, ...prev].slice(0, 50));

        // 2. Handle REST snapshot required signal
        if (payload.event_type === 'REST_SNAPSHOT_REQUIRED') {
          if (onSnapshotRequiredRef.current) {
            onSnapshotRequiredRef.current(payload);
          }
          return;
        }

        // 3. Handle Alert Lifecycle
        if (payload.event_type === 'PAYMENT_GATEWAY_ALERT_TRIGGERED') {
          setActiveAlerts((prev) => {
            const filtered = prev.filter((a) => a.entity !== payload.entity);
            return [payload, ...filtered];
          });
        } else if (payload.event_type === 'PAYMENT_GATEWAY_ALERT_RECOVERED') {
          setActiveAlerts((prev) => prev.filter((a) => a.entity !== payload.entity));
        }

        // 4. Trigger caller targeted invalidation callback
        if (onEventRef.current) {
          onEventRef.current(payload);
        }
      } catch (err) {
        // SSE comments or non-JSON heartbeats reset watchdog and pass through
      }
    };

    // Listen to default message and all defined custom event types
    const eventTypes = [
      'message',
      'HEARTBEAT',
      'RULE_DECISION_EVALUATED',
      'DATASET_READINESS_CHANGED',
      'SIMULATION_STATUS_CHANGED',
      'CART_ABANDONMENT_TRIGGERED',
      'MARKETING_WEBHOOK_OUTCOME',
      'INVENTORY_REORDER_STATE_CHANGED',
      'DELIVERY_COMPLETED',
      'PAYMENT_GATEWAY_ALERT_TRIGGERED',
      'PAYMENT_GATEWAY_ALERT_RECOVERED',
      'PIPELINE_STATUS_CHANGED',
      'REST_SNAPSHOT_REQUIRED',
      'SYSTEM_NOTIFICATION'
    ];

    eventTypes.forEach((type) => {
      es.addEventListener(type, handleEventMessage);
    });
  }, [resetWatchdog, teardown]);

  useEffect(() => {
    isUnmountedRef.current = false;
    connect();

    return () => {
      isUnmountedRef.current = true;
      teardown();
    };
  }, [connect, teardown]);

  const clearAlert = useCallback((entity) => {
    setActiveAlerts((prev) => prev.filter((a) => a.entity !== entity));
  }, []);

  const manualReconnect = useCallback(() => {
    retryCountRef.current = 0;
    connect();
  }, [connect]);

  return {
    connectionStatus,
    isConnected: connectionStatus === 'CONNECTED',
    isLive: connectionStatus === 'CONNECTED', // backward compatibility alias
    latestEvent,
    events,
    activeAlerts,
    reconnect: manualReconnect,
    clearAlert
  };
}

export default useObservabilityEvents;
