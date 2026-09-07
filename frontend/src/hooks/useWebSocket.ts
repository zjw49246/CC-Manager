import { useEffect, useRef, useState } from 'react';
import { WsClient } from '../api/ws';
import { getWsUrl } from '../config/server';

type Subscriber = {
  channels: Set<string>;
  onMessage?: (msg: Record<string, unknown>) => void;
  onReconnect?: () => void;
  onSubscribed?: (channels: string[]) => void;
  onConnectionChange: (connected: boolean) => void;
};

class SharedWsConnection {
  readonly client: WsClient;
  private subscribers = new Set<Subscriber>();
  private acceptedChannels = new Set<string>();
  private currentChannels = new Set<string>();
  private removeHandlers: Array<() => void> = [];

  constructor(url: string) {
    this.client = new WsClient(url);
    this.removeHandlers.push(
      this.client.onMessage((message) => {
        const channel = message.channel;
        if (typeof channel !== 'string') return;
        for (const subscriber of this.subscribers) {
          if (subscriber.channels.has(channel)) subscriber.onMessage?.(message as unknown as Record<string, unknown>);
        }
      }),
      this.client.onReconnect(() => {
        this.acceptedChannels.clear();
        for (const subscriber of this.subscribers) subscriber.onReconnect?.();
      }),
      this.client.onConnectionChange((connected) => {
        if (!connected) this.acceptedChannels.clear();
        for (const subscriber of this.subscribers) subscriber.onConnectionChange(connected);
      }),
      this.client.onSubscribed((channels) => {
        for (const channel of channels) this.acceptedChannels.add(channel);
        for (const subscriber of this.subscribers) {
          const accepted = channels.filter((channel) => subscriber.channels.has(channel));
          subscriber.onSubscribed?.(accepted);
        }
      }),
    );
    this.client.connect();
  }

  add(subscriber: Subscriber) {
    this.subscribers.add(subscriber);
    subscriber.onConnectionChange(this.client.isConnected());
    const added = [...subscriber.channels].filter((channel) => !this.currentChannels.has(channel));
    for (const channel of subscriber.channels) this.currentChannels.add(channel);
    if (added.length > 0) this.client.subscribe(added);
    const accepted = [...subscriber.channels].filter((channel) => this.acceptedChannels.has(channel));
    if (accepted.length > 0) subscriber.onSubscribed?.(accepted);
  }

  remove(subscriber: Subscriber): boolean {
    this.subscribers.delete(subscriber);
    if (this.subscribers.size === 0) {
      this.dispose();
      return true;
    }
    const aggregate = new Set<string>();
    for (const current of this.subscribers) {
      for (const channel of current.channels) aggregate.add(channel);
    }
    if (aggregate.size === this.currentChannels.size && [...aggregate].every((channel) => this.currentChannels.has(channel))) return false;
    this.currentChannels = aggregate;
    this.acceptedChannels.clear();
    this.client.unsubscribe();
    this.client.subscribe([...aggregate]);
    return false;
  }

  dispose() {
    for (const remove of this.removeHandlers) remove();
    this.removeHandlers = [];
    this.subscribers.clear();
    this.client.close();
  }
}

const sharedConnections = new Map<string, SharedWsConnection>();

function connectionKey(wsUrl: string): string {
  let token = '';
  try { token = localStorage.getItem('cc_token') || ''; } catch { /* storage may be unavailable */ }
  return `${wsUrl}\n${token}`;
}

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
  const callbackRef = useRef(onMessage);
  const reconnectRef = useRef(onReconnect);
  const subscribedRef = useRef(onSubscribed);
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
    const key = connectionKey(wsUrl);
    let shared = sharedConnections.get(key);
    if (!shared) {
      shared = new SharedWsConnection(wsUrl);
      sharedConnections.set(key, shared);
    }
    const subscriber: Subscriber = {
      channels: new Set(channelsKey ? channelsKey.split(',') : []),
      onMessage: (message) => callbackRef.current?.(message),
      onReconnect: () => reconnectRef.current?.(),
      onSubscribed: (accepted) => subscribedRef.current?.(accepted),
      onConnectionChange: setIsConnected,
    };
    shared.add(subscriber);

    return () => {
      if (shared?.remove(subscriber) && sharedConnections.get(key) === shared) {
        sharedConnections.delete(key);
      }
    };
  }, [channelsKey]);

  return { isConnected };
}
