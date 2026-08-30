import { useEffect, useRef, useState } from 'react';
import { WsClient } from '../api/ws';
import { getWsUrl } from '../config/server';

/**
 * useWebSocket hook with callback support.
 *
 * IMPORTANT: Use the `onMessage` callback for high-frequency streams (e.g. chat).
 * The old `lastMessage` state pattern loses messages when React batches rapid
 * state updates — the useEffect depending on lastMessage only fires for the
 * last value in a batch, silently dropping intermediate messages.
 *
 * `onReconnect` skips the initial transport connection. `onSubscribed` runs
 * after every server subscription ACK, including the initial one, so callers
 * can safely close the HTTP-snapshot/WebSocket-subscribe gap when needed.
 */
export function useWebSocket(
  channels: string[],
  onMessage?: (msg: Record<string, unknown>) => void,
  onReconnect?: () => void,
  onSubscribed?: (channels: string[]) => void,
) {
  const clientRef = useRef<WsClient | null>(null);
  const callbackRef = useRef(onMessage);
  const reconnectRef = useRef(onReconnect);
  const subscribedRef = useRef(onSubscribed);
  const [lastMessage, setLastMessage] = useState<Record<string, unknown> | null>(null);
  const [isConnected, setIsConnected] = useState(false);

  // Keep callback refs in sync without reconnecting the socket whenever a
  // component renders a fresh callback closure.
  useEffect(() => {
    callbackRef.current = onMessage;
    reconnectRef.current = onReconnect;
    subscribedRef.current = onSubscribed;
  }, [onMessage, onReconnect, onSubscribed]);

  // Serialize channels to avoid re-running effect on every render
  const channelsKey = channels.join(',');

  useEffect(() => {
    const wsUrl = getWsUrl();
    if (!wsUrl) return;
    const client = new WsClient(wsUrl);
    clientRef.current = client;

    client.onMessage((msg) => {
      const parsed = msg as unknown as Record<string, unknown>;
      callbackRef.current?.(parsed);
      // Callback subscribers process messages directly. Updating the legacy
      // state for every event rerenders their entire component tree.
      if (!callbackRef.current) setLastMessage(parsed);
    });

    const removeConnectionHandler = client.onConnectionChange(setIsConnected);
    const removeReconnectHandler = client.onReconnect(() => {
      reconnectRef.current?.();
    });

    const removeSubscriptionHandler = client.onSubscribed((subscribedChannels) => {
      subscribedRef.current?.(subscribedChannels);
    });

    client.connect();
    client.subscribe(channelsKey.split(','));

    return () => {
      removeConnectionHandler();
      removeReconnectHandler();
      removeSubscriptionHandler();
      client.close();
    };
  }, [channelsKey]);

  return { lastMessage, isConnected };
}
