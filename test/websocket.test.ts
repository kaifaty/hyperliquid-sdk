import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import { WebSocketServer } from 'ws';
import { describe, expect, it, vi } from 'vitest';

import { Stream, type TrackedWalletEvent } from '../src/websocket';

const user = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

describe('WebSocket protocol selection', () => {
  it('uses public wallet subscriptions on one custom endpoint', async () => {
    const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
    await once(server, 'listening');
    const { port } = server.address() as AddressInfo;
    const stream = new Stream(`http://127.0.0.1:${port}/gateway?network=test`, { protocol: 'hyperliquid' });
    const fills = vi.fn();
    const ledger = vi.fn();
    const fillId = stream.userFills(user, fills);
    stream.userNonFundingLedger(user, ledger);
    const connected = once(server, 'connection');
    const commands: unknown[] = [];
    server.on('connection', socket => {
      socket.on('message', data => commands.push(JSON.parse(data.toString())));
    });
    try {
      stream.start();
      const [socket, request] = await connected;
      expect(request.url).toBe('/gateway/ws?network=test');
      await vi.waitFor(() => expect(commands).toEqual([
        { method: 'subscribe', subscription: { type: 'userFills', user } },
        { method: 'subscribe', subscription: { type: 'userNonFundingLedgerUpdates', user } },
      ]));
      const fill = { channel: 'userFills', data: { user, isSnapshot: true, fills: [{ tid: 7 }] } };
      const transfer = { channel: 'userNonFundingLedgerUpdates', data: {
        user, isSnapshot: false, nonFundingLedgerUpdates: [{ hash: 'transfer' }],
      } };
      socket.send(JSON.stringify(fill));
      socket.send(JSON.stringify(transfer));
      await vi.waitFor(() => expect(ledger.mock.calls).toEqual([[transfer]]));
      expect(fills.mock.calls).toEqual([[fill]]);

      stream.unsubscribe(fillId);
      await vi.waitFor(() => expect(commands[2]).toEqual({
        method: 'unsubscribe', subscription: { type: 'userFills', user },
      }));
      socket.send(JSON.stringify(fill));
      const next = { ...transfer, data: { ...transfer.data, nonFundingLedgerUpdates: [{ hash: 'next' }] } };
      socket.send(JSON.stringify(next));
      await vi.waitFor(() => expect(ledger.mock.calls).toEqual([[transfer], [next]]));
      expect(fills.mock.calls).toEqual([[fill]]);
    } finally {
      stream.stop();
      for (const socket of server.clients) socket.terminate();
      await new Promise<void>(resolve => server.close(() => resolve()));
    }
  });

  it('keeps QuickNode detection on custom endpoints without an override', async () => {
    const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
    await once(server, 'listening');
    const { port } = server.address() as AddressInfo;
    const stream = new Stream(`http://127.0.0.1:${port}/token`);
    stream.userFills(user, () => {});
    const command = new Promise(resolve => server.once('connection', (socket, request) => {
      socket.once('message', data => resolve({ path: request.url, body: JSON.parse(data.toString()) }));
    }));
    try {
      stream.start();
      expect(await command).toEqual({
        path: '/token/hypercore/ws',
        body: { jsonrpc: '2.0', id: 1, method: 'hl_subscribe', params: {
          streamType: 'userFills', filters: { user: [user] },
        } },
      });
    } finally {
      stream.stop();
      for (const socket of server.clients) socket.terminate();
      await new Promise<void>(resolve => server.close(() => resolve()));
    }
  });
});

it('resumes the all-wallet feed after the last completed callback, including mid-page disconnects', async () => {
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await once(server, 'listening');
  const { port } = server.address() as AddressInfo;
  const errors = vi.fn();
  const stream = new Stream(`ws://127.0.0.1:${port}/ws`, { protocol: 'hyperliquid', onError: errors });
  const other = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
  const first = { cursor: '9007199254740993', type: 'fill', user, event: { hash: 'builder-fill' } };
  const second = { cursor: '9007199254740994', type: 'ledger', user: other, event: { hash: 'transfer' } };
  const offline = { cursor: '9007199254740996', type: 'fill', user: other, event: { hash: 'offline-fill' } };
  const resumes: { type: string; after: string }[] = [];
  const handled: string[] = [];
  const acks: string[] = [];
  let complete!: () => void;
  const commit = new Promise<void>(resolve => { complete = resolve; });
  let began!: () => void;
  const processing = new Promise<void>(resolve => { began = resolve; });
  let count = 0;
  const reconnected = new Promise<void>(resolve => {
    server.on('connection', socket => {
      if (++count === 2) resolve();
      socket.on('message', data => {
        const req = JSON.parse(data.toString());
        if (req.method === 'ack') { acks.push(req.cursor); return; }
        if (req.method !== 'subscribe' || req.subscription.type !== 'trackedWalletEvents' || 'user' in req.subscription) return;
        resumes.push(req.subscription);
        socket.send(JSON.stringify({ channel: 'trackedWalletEvents', data:
          [first, second, ...(resumes.length === 1 ? [] : [offline])].filter(e => BigInt(e.cursor) > BigInt(req.subscription.after)) }));
      });
    });
  });
  let failOnce = true;
  stream.trackedWalletEvents(async activity => {
    if (activity.cursor === offline.cursor && failOnce) {
      failOnce = false;
      throw new Error('commit failed');
    }
    if (activity.cursor === first.cursor) { began(); await commit; }
    handled.push(activity.event.hash as string);
  });
  try {
    stream.start();
    await processing;
    for (const socket of server.clients) socket.terminate();
    await reconnected;
    expect(resumes).toEqual([{ type: 'trackedWalletEvents', after: '0' }]);
    complete();
    await vi.waitFor(() => expect(acks).toEqual([offline.cursor]), { timeout: 3000 });
    expect(errors).toHaveBeenCalledOnce();
    expect(resumes).toEqual(['0', first.cursor, second.cursor].map(after => ({ type: 'trackedWalletEvents', after })));
    expect(handled).toEqual(['builder-fill', 'transfer', 'offline-fill']);
  } finally {
    complete();
    stream.stop();
    for (const socket of server.clients) socket.terminate();
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});

it('resumes order statuses from the last committed cursor, including an offline cancellation', async () => {
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await once(server, 'listening');
  const { port } = server.address() as AddressInfo;
  const errors = vi.fn();
  const stream = new Stream(`ws://127.0.0.1:${port}/ws`, { protocol: 'hyperliquid', onError: errors });
  const order = { oid: 71, coin: 'ETH', side: 'B', limitPx: '2000', sz: '1', timestamp: 1700000000000 };
  const opened = {
    cursor: '9007199254740993', user, type: 'orderStatus' as const,
    event: { order, status: 'open', statusTimestamp: order.timestamp },
  };
  const canceled = {
    ...opened, cursor: '9007199254740995',
    event: { order, status: 'canceled', statusTimestamp: order.timestamp + 500 },
  };
  const journal = [opened];
  const subscriptions: unknown[] = [];
  const acks: string[] = [];
  const handled: TrackedWalletEvent[] = [];
  let commitOpen!: () => void;
  const pendingOpen = new Promise<void>(resolve => { commitOpen = resolve; });
  const started = vi.fn();

  server.on('connection', socket => {
    socket.on('message', data => {
      const req = JSON.parse(data.toString());
      if (req.method === 'ack') { acks.push(req.cursor); return; }
      if (req.method !== 'subscribe') return;
      subscriptions.push(req.subscription);
      if (!req.subscription.includeOrderStatuses) return;
      const page = journal.filter(row => BigInt(row.cursor) > BigInt(req.subscription.after));
      if (page.length) socket.send(JSON.stringify({ channel: 'trackedWalletEvents', data: page }));
    });
  });
  const after = '9007199254740992';
  stream.trackedWalletEvents(async row => {
    if (row.type === 'orderStatus' && row.event.status === 'open') {
      started();
      await pendingOpen;
    }
    handled.push(row);
  }, after, { includeOrderStatuses: true });
  try {
    stream.start();
    const subscription = { type: 'trackedWalletEvents', after, includeOrderStatuses: true };
    await vi.waitFor(() => expect(subscriptions).toEqual([subscription]));
    await vi.waitFor(() => expect(started).toHaveBeenCalledOnce());
    const reconnect = once(server, 'connection');
    for (const socket of server.clients) socket.terminate();
    journal.push(canceled);
    await reconnect;
    commitOpen();
    await vi.waitFor(() => expect(acks).toEqual([canceled.cursor]));
    expect(subscriptions).toEqual([subscription, { ...subscription, after: opened.cursor }]);
    expect(handled).toEqual([opened, canceled]);
    expect(errors).not.toHaveBeenCalled();
  } finally {
    commitOpen();
    stream.stop();
    for (const socket of server.clients) socket.terminate();
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});
