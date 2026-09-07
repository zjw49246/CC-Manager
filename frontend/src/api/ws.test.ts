import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { WsClient } from './ws';

// Mock WebSocket
class MockWebSocket {
  static OPEN = 1;
  static CONNECTING = 0;
  static CLOSING = 2;
  static CLOSED = 3;

  readyState = MockWebSocket.CONNECTING;
  onopen: (() => void) | null = null;
  onmessage: ((e: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  sent: string[] = [];

  send(data: string) {
    this.sent.push(data);
  }

  close() {
    this.readyState = MockWebSocket.CLOSED;
    // Fire onclose asynchronously like real WebSocket
    setTimeout(() => this.onclose?.(), 0);
  }

  // Helpers for tests
  simulateOpen() {
    this.readyState = MockWebSocket.OPEN;
    this.onopen?.();
  }

  simulateMessage(data: Record<string, unknown>) {
    this.onmessage?.({ data: JSON.stringify(data) });
  }

  simulateClose() {
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.();
  }
}

let mockInstances: MockWebSocket[] = [];

beforeEach(() => {
  mockInstances = [];
  vi.stubGlobal('WebSocket', class extends MockWebSocket {
    constructor(url: string) {
      super();
      void url;
      mockInstances.push(this);
    }
  });
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('WsClient', () => {
  describe('connect and subscribe', () => {
    it('reports connected only after the server acknowledges the subscription', () => {
      const client = new WsClient('ws://test');
      const handler = vi.fn();
      const subscribed = vi.fn();
      client.onConnectionChange(handler);
      client.onSubscribed(subscribed);
      client.subscribe(['task:1']);
      client.connect();

      const ws = mockInstances[0];
      expect(handler).not.toHaveBeenCalled();
      ws.simulateOpen();
      expect(handler).not.toHaveBeenCalled();
      expect(subscribed).not.toHaveBeenCalled();

      ws.simulateMessage({ action: 'subscribed', channels: ['task:1'] });
      expect(handler).toHaveBeenLastCalledWith(true);
      expect(subscribed).toHaveBeenCalledWith(['task:1']);

      ws.simulateClose();
      expect(handler).toHaveBeenLastCalledWith(false);
    });

    it('unregisters a connection-state handler', () => {
      const client = new WsClient('ws://test');
      const handler = vi.fn();
      const unsubscribe = client.onConnectionChange(handler);
      client.connect();
      unsubscribe();
      mockInstances[0].simulateOpen();
      mockInstances[0].simulateMessage({
        action: 'subscribed',
        channels: [],
      });
      expect(handler).not.toHaveBeenCalled();
    });

    it('unregisters a subscription acknowledgement handler', () => {
      const client = new WsClient('ws://test');
      const handler = vi.fn();
      const unsubscribe = client.onSubscribed(handler);
      client.subscribe(['task:1']);
      client.connect();
      unsubscribe();

      mockInstances[0].simulateOpen();
      mockInstances[0].simulateMessage({
        action: 'subscribed',
        channels: ['task:1'],
      });

      expect(handler).not.toHaveBeenCalled();
    });

    it('sends subscribe on open when channels were added before connect', () => {
      const client = new WsClient('ws://test');
      client.subscribe(['task:1']);
      client.connect();

      const ws = mockInstances[0];
      ws.simulateOpen();

      expect(ws.sent).toHaveLength(1);
      expect(JSON.parse(ws.sent[0])).toEqual({
        action: 'subscribe',
        channels: ['task:1'],
      });
    });

    it('sends subscribe immediately when ws is already open', () => {
      const client = new WsClient('ws://test');
      client.connect();
      const ws = mockInstances[0];
      ws.simulateOpen();

      client.subscribe(['task:2']);

      expect(ws.sent).toHaveLength(1);
      expect(JSON.parse(ws.sent[0])).toEqual({
        action: 'subscribe',
        channels: ['task:2'],
      });
    });

    it('deduplicates channels', () => {
      const client = new WsClient('ws://test');
      client.subscribe(['task:1']);
      client.subscribe(['task:1', 'task:2']);
      client.connect();
      const ws = mockInstances[0];
      ws.simulateOpen();

      expect(JSON.parse(ws.sent[0]).channels).toEqual(['task:1', 'task:2']);
    });

    it('clears all server subscriptions before replacing a shared aggregate', () => {
      const client = new WsClient('ws://test');
      client.subscribe(['tasks', 'workers']);
      client.connect();
      const ws = mockInstances[0];
      ws.simulateOpen();

      client.unsubscribe();
      client.subscribe(['tasks']);

      expect(ws.sent.slice(-2).map((message) => JSON.parse(message))).toEqual([
        { action: 'unsubscribe' },
        { action: 'subscribe', channels: ['tasks'] },
      ]);
    });
  });

  describe('message handling', () => {
    it('dispatches messages with channel to handlers', () => {
      const client = new WsClient('ws://test');
      const handler = vi.fn();
      client.onMessage(handler);
      client.connect();
      const ws = mockInstances[0];
      ws.simulateOpen();

      ws.simulateMessage({ channel: 'task:1', data: { event_type: 'message' } });

      expect(handler).toHaveBeenCalledWith({
        channel: 'task:1',
        data: { event_type: 'message' },
      });
    });

    it('ignores messages without channel', () => {
      const client = new WsClient('ws://test');
      const handler = vi.fn();
      client.onMessage(handler);
      client.connect();
      const ws = mockInstances[0];
      ws.simulateOpen();

      ws.simulateMessage({ type: 'subscribed' });

      expect(handler).not.toHaveBeenCalled();
    });

    it('unregisters handler via returned cleanup', () => {
      const client = new WsClient('ws://test');
      const handler = vi.fn();
      const cleanup = client.onMessage(handler);
      client.connect();
      const ws = mockInstances[0];
      ws.simulateOpen();

      cleanup();
      ws.simulateMessage({ channel: 'task:1', data: {} });

      expect(handler).not.toHaveBeenCalled();
    });
  });

  describe('reconnection', () => {
    it('reconnects with exponential backoff on close', () => {
      const client = new WsClient('ws://test');
      client.subscribe(['task:1']);
      client.connect();
      expect(mockInstances).toHaveLength(1);

      const ws1 = mockInstances[0];
      ws1.simulateClose();

      // After 1s (initial delay)
      vi.advanceTimersByTime(1000);
      expect(mockInstances).toHaveLength(2);

      // Second close → 2s delay
      mockInstances[1].simulateClose();
      vi.advanceTimersByTime(1000);
      expect(mockInstances).toHaveLength(2); // not yet
      vi.advanceTimersByTime(1000);
      expect(mockInstances).toHaveLength(3);
    });

    it('resets backoff delay after successful reconnect', () => {
      const client = new WsClient('ws://test');
      client.connect();

      // First disconnect → 1s delay
      mockInstances[0].simulateClose();
      vi.advanceTimersByTime(1000);
      expect(mockInstances).toHaveLength(2);

      // Successful reconnect (open fires)
      mockInstances[1].simulateOpen();

      // Disconnect again → should be 1s (reset), not 2s
      mockInstances[1].simulateClose();
      vi.advanceTimersByTime(1000);
      expect(mockInstances).toHaveLength(3);
    });

    it('re-subscribes to all channels on reconnect', () => {
      const client = new WsClient('ws://test');
      client.subscribe(['task:1', 'system']);
      client.connect();
      mockInstances[0].simulateOpen();

      // Disconnect and reconnect
      mockInstances[0].simulateClose();
      vi.advanceTimersByTime(1000);
      const ws2 = mockInstances[1];
      ws2.simulateOpen();

      expect(JSON.parse(ws2.sent[0])).toEqual({
        action: 'subscribe',
        channels: ['task:1', 'system'],
      });
    });

    it('fires onReconnect handler on reconnect but not on first connect', () => {
      const client = new WsClient('ws://test');
      const reconnectHandler = vi.fn();
      client.onReconnect(reconnectHandler);
      client.connect();

      // First connect - should NOT fire
      mockInstances[0].simulateOpen();
      expect(reconnectHandler).not.toHaveBeenCalled();

      // Disconnect and reconnect - SHOULD fire
      mockInstances[0].simulateClose();
      vi.advanceTimersByTime(1000);
      mockInstances[1].simulateOpen();
      expect(reconnectHandler).toHaveBeenCalledTimes(1);
    });

    it('unregisters reconnect handler via returned cleanup', () => {
      const client = new WsClient('ws://test');
      const reconnectHandler = vi.fn();
      const cleanup = client.onReconnect(reconnectHandler);
      client.connect();
      mockInstances[0].simulateOpen();

      cleanup();

      mockInstances[0].simulateClose();
      vi.advanceTimersByTime(1000);
      mockInstances[1].simulateOpen();
      expect(reconnectHandler).not.toHaveBeenCalled();
    });
  });

  describe('destroyed flag (close)', () => {
    it('does not reconnect after close() is called', () => {
      const client = new WsClient('ws://test');
      client.connect();
      expect(mockInstances).toHaveLength(1);

      client.close();

      // onclose fires asynchronously
      vi.advanceTimersByTime(0);
      // Even after waiting for retry delay, no reconnect
      vi.advanceTimersByTime(5000);
      expect(mockInstances).toHaveLength(1);
    });

    it('does not create new connection if connect() called after close()', () => {
      const client = new WsClient('ws://test');
      client.close();
      client.connect();
      expect(mockInstances).toHaveLength(0);
    });
  });
});
