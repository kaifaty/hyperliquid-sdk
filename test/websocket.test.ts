import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import { WebSocketServer } from 'ws';
import { describe, expect, it, vi } from 'vitest';

import { Stream } from '../src/websocket';

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
