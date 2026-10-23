// Run after cargo build and installing web dependencies:
// node web/tests/client-access.mjs
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { createServer } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import { io } from 'socket.io-client';

const temporary = await mkdtemp(join(tmpdir(), 'ecorun-client-access-'));
const reservation = createServer();
await new Promise((resolve) => reservation.listen(0, '127.0.0.1', resolve));
const port = reservation.address().port;
await new Promise((resolve) => reservation.close(resolve));
const base = `http://127.0.0.1:${port}`;
const database = join(temporary, 'test.sqlite3');
const clusterToken = 'test-only-cluster-token-123456';
const server = spawn(resolve('target/debug/ecorun-logger-server'), [], {
  cwd: temporary,
  env: {
    PATH: process.env.PATH,
    ECORUN_BIND: `127.0.0.1:${port}`,
    ECORUN_DB_PATH: database,
    ECORUN_NODE_ID: 'test-server',
    ECORUN_CLUSTER_TOKEN: clusterToken,
  },
  stdio: ['ignore', 'inherit', 'inherit'],
});
let spawnError;
server.on('error', (error) => { spawnError = error; });
const sockets = [];
function socket(namespace, options = {}) {
  const client = io(`${base}/${namespace}`, {
    autoConnect: false, forceNew: true, reconnection: false,
    transports: ['websocket'], ...options,
  });
  sockets.push(client);
  return client;
}
function event(client, name) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timeout: ${name}`)), 5000);
    client.once(name, (value) => { clearTimeout(timer); resolve(value); });
  });
}
async function rejected(namespace, options) {
  const client = socket(namespace, options);
  const refusal = Promise.race([event(client, 'disconnect'), event(client, 'connect_error')]);
  let admitted = false;
  client.on('room.state', () => { admitted = true; });
  client.on('cluster.sync', () => { admitted = true; });
  client.connect();
  await refusal;
  assert.equal(admitted, false, `${namespace} must reject access`);
  client.disconnect();
}
try {
  let ready = false;
  for (let attempt = 0; attempt < 100; attempt++) {
    if (spawnError) throw spawnError;
    if (server.exitCode !== null) throw new Error(`Server exited: ${server.exitCode}`);
    try { ready = (await fetch(`${base}/healthz`)).ok; } catch {}
    if (ready) break;
    await delay(100);
  }
  assert.ok(ready, 'server starts');
  execFileSync('python3', ['-c', `
import sqlite3, sys
with sqlite3.connect(sys.argv[1]) as db:
    db.executemany('INSERT INTO rooms(id,name,socket_key) VALUES(?,?,?)',
                   [('main','Main','unused-secret'), ('second','Second','other-secret')])
`, database]);

  const map = { center_lat: 35.6, center_lon: 139.7, zoom: 12, plots: [] };
  const put = (room, value) => fetch(`${base}/api/rooms/${room}/map`, {
    method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify(value),
  });
  assert.equal((await put('main', map)).status, 200);
  assert.deepEqual(await (await fetch(`${base}/api/rooms/main/map`)).json(), map);
  assert.equal((await put('main', { ...map, zoom: 25 })).status, 400);
  assert.equal((await put('main', { zoom: 12 })).status, 422);
  assert.equal((await fetch(`${base}/api/rooms/main/telemetry`)).status, 200);
  for (const room of ['missing', 'bad%2Fid', 'x'.repeat(65)]) {
    assert.equal((await fetch(`${base}/api/rooms/${room}/map`)).status, 403);
    assert.equal((await put(room, map)).status, 403);
    assert.equal((await fetch(`${base}/api/rooms/${room}/telemetry`)).status, 403);
  }

  // No auth payload on either browser transport, including reconnection/room switch.
  for (const transports of [['websocket'], ['polling']]) {
    for (const room of ['main', 'second', 'main']) {
      const client = socket(room, { transports });
      const state = event(client, 'room.state');
      const initialMap = event(client, 'map.state');
      const history = event(client, 'telemetry.state');
      client.connect();
      assert.equal((await state).room_id, room);
      await initialMap;
      assert.ok(Array.isArray(await history));
      client.disconnect();
    }
  }
  const reader = socket('main');
  const joined = event(reader, 'room.state');
  reader.connect();
  await joined;
  const writer = socket('main');
  const writerJoined = event(writer, 'room.state');
  writer.connect();
  await writerJoined;
  const changed = event(reader, 'map.state');
  writer.emit('map.update', { ...map, zoom: 13 });
  assert.equal((await changed).zoom, 13);
  assert.equal((await (await fetch(`${base}/api/rooms/main/map`)).json()).zoom, 13);
  const live = event(reader, 'telemetry');
  writer.emit('telemetry.ingest', { source: 'test', data: { speed: 42 } });
  assert.equal((await live).data.speed, 42);
  const history = await (await fetch(`${base}/api/rooms/main/telemetry`)).json();
  assert.ok(history.some((row) => row.source === 'test' && row.data.speed === 42));
  await rejected('missing');
  await rejected('x'.repeat(65));
  await rejected('_cluster');
  await rejected('_cluster', { auth: { node_id: 'peer', token: 'wrong' } });
  const peer = socket('_cluster', { auth: { node_id: 'peer', token: clusterToken } });
  const sync = event(peer, 'cluster.sync');
  peer.connect();
  assert.ok(['main', 'second'].includes((await sync).room));
  console.log('PASS: unauthenticated HTTP/Socket.IO, room switching, maps, telemetry, validation and cluster authentication');
} finally {
  for (const client of sockets) client.disconnect();
  if (server.exitCode === null && !spawnError) {
    const stopped = new Promise((resolve) => server.once('exit', resolve));
    server.kill('SIGTERM');
    await stopped;
  }
  await rm(temporary, { recursive: true, force: true });
}
