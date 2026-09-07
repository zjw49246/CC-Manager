type WsHandler = (data: { channel: string; data: Record<string, unknown> }) => void;
type ReconnectHandler = () => void;
type ConnectionHandler = (connected: boolean) => void;
type SubscriptionHandler = (channels: string[]) => void;

export class WsClient {
  private ws: WebSocket | null = null;
  private channels: string[] = [];
  private handlers: WsHandler[] = [];
  private reconnectHandlers: ReconnectHandler[] = [];
  private connectionHandlers: ConnectionHandler[] = [];
  private subscriptionHandlers: SubscriptionHandler[] = [];
  private retryDelay = 1000;
  private maxDelay = 30000;
  private url: string;
  private destroyed = false;
  private hasConnectedOnce = false;
  private connected = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(url: string) {
    this.url = url;
  }

  connect() {
    if (this.destroyed) return;
    // /ws 已加 token 认证（worker 模式必须）；浏览器设不了 header，走查询参数
    const token = localStorage.getItem('cc_token') || '';
    const url = token ? `${this.url}${this.url.includes('?') ? '&' : '?'}token=${encodeURIComponent(token)}` : this.url;
    this.ws = new WebSocket(url);
    this.ws.onopen = () => {
      this.retryDelay = 1000;
      if (this.channels.length > 0) {
        this.ws?.send(JSON.stringify({ action: 'subscribe', channels: this.channels }));
      }
      // Notify reconnect handlers (skip the very first connect)
      if (this.hasConnectedOnce) {
        this.reconnectHandlers.forEach((h) => h());
      }
      this.hasConnectedOnce = true;
    };
    this.ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.action === 'subscribed' && Array.isArray(msg.channels)) {
          this.setConnectionState(true);
          this.subscriptionHandlers.forEach((handler) => handler(msg.channels));
        }
        if (msg.channel) {
          this.handlers.forEach((h) => h(msg));
        }
      } catch { /* ignore */ }
    };
    this.ws.onclose = () => {
      this.setConnectionState(false);
      if (this.destroyed) return;
      if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null;
        this.connect();
      }, this.retryDelay);
      this.retryDelay = Math.min(this.retryDelay * 2, this.maxDelay);
    };
  }

  subscribe(channels: string[]) {
    this.channels = [...new Set([...this.channels, ...channels])];
    const unique = [...new Set(channels)];
    if (unique.length > 0 && this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ action: 'subscribe', channels: unique }));
    }
  }

  /**
   * The server treats unsubscribe as clearing every channel on this socket.
   * Shared consumers call this before subscribing the current aggregate.
   */
  unsubscribe() {
    this.channels = [];
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ action: 'unsubscribe' }));
    }
  }

  onMessage(handler: WsHandler) {
    this.handlers.push(handler);
    return () => {
      this.handlers = this.handlers.filter((h) => h !== handler);
    };
  }

  onReconnect(handler: ReconnectHandler) {
    this.reconnectHandlers.push(handler);
    return () => {
      this.reconnectHandlers = this.reconnectHandlers.filter((h) => h !== handler);
    };
  }

  onConnectionChange(handler: ConnectionHandler) {
    this.connectionHandlers.push(handler);
    return () => {
      this.connectionHandlers = this.connectionHandlers.filter((h) => h !== handler);
    };
  }

  onSubscribed(handler: SubscriptionHandler) {
    this.subscriptionHandlers.push(handler);
    return () => {
      this.subscriptionHandlers = this.subscriptionHandlers.filter((h) => h !== handler);
    };
  }

  isConnected() {
    return this.connected;
  }

  private setConnectionState(connected: boolean) {
    if (this.connected === connected) return;
    this.connected = connected;
    this.connectionHandlers.forEach((handler) => handler(connected));
  }

  close() {
    this.destroyed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.setConnectionState(false);
    this.ws?.close();
    this.ws = null;
  }
}
