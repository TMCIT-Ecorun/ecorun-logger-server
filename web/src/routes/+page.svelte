<script lang="ts">
  import { onMount } from 'svelte';
  import { io, type Socket } from 'socket.io-client';
  import { L, loadLeaflet } from '$lib/local-map';

  type Telemetry = { event_id?: string | null; source: string; timestamp?: string | null; received_at?: string | number; data: Record<string, unknown> };
  const SERVER_SAMPLE_RATE = 8000;
  const SERVER_FRAME_MS = 60;
  const OPUS_SAMPLE_RATES = [8000, 12000, 16000, 24000, 48000];
  const OPUS_FRAME_MS = [20, 40, 60];
  let opusSampleRate = 48000;
  let opusFrameMs = 20;
  let opusBitrate = 8000;
  const RECEIVE_BITRATES = [4000, 6000, 8000, 12000, 16000, 24000, 32000];
  const RECEIVE_FRAME_MS = [20, 40, 60, 120];
  let forwardToLogger = true;
  let receiveBitrate = 8000;
  let receiveFrameMs = 60;
  let roomId = 'main';
  let endpoint = '';
  type LoggerServer = { id: string; name: string; url: string; available: boolean };
  let servers: LoggerServer[] = [];
  let selectedServer = 'local';
  let status = 'disconnected';
  const clientId = crypto.randomUUID();
  let sequence = 0;
  type Participant = { session_id: number | string; client_id?: string | null; muted: boolean; ptt: boolean };
  let participants: Participant[] = [];
  let telemetry: Telemetry[] = [];
  let telemetryVisible = false;
  let errorMessage = '';
  let ptt = false;
  let keyboardPttHeld = false;
  let muted = false;
  let sockets: Socket[] = [];
  let activeSocket: Socket | null = null;
  let activeServerName = '';
  let activeLoggerConnected = false;
  const socketTargets = new Map<Socket, string>();
  const loggerPresence = new Map<Socket, boolean>();
  const loggerPresenceSince = new Map<Socket, number>();
  const LOGGER_SWITCH_STABLE_MS = 3000;
  let mapStateReceived = false;
  let connectedSocketCount = 0;
  const nodeParticipants = new Map<Socket, Map<string, Participant>>();
  const telemetryKeys = new Set<string>();
  const receivedAudioKeys = new Set<string>();

  let audioContext: AudioContext | null = null;
  let mediaStream: MediaStream | null = null;
  let sourceNode: MediaStreamAudioSourceNode | null = null;
  let micGainNode: GainNode | null = null;
  let processor: ScriptProcessorNode | null = null;
  let encoder: AudioEncoder | null = null;
  let captureGeneration = 0;
  let captureSetupPromise: Promise<void> | null = null;
  let decoder: AudioDecoder | null = null;
  let pendingPcm = new Float32Array(0);
  let encodeTimestamp = 0;
  let playAt = 0;
  let gainNode: GainNode | null = null;
  let volume = 1;
  let micVolume = 1;
  let micBoost = 1;
  let inputDevices: MediaDeviceInfo[] = [];
  let outputDevices: MediaDeviceInfo[] = [];
  let selectedInputDevice = '';
  let selectedOutputDevice = '';
  let outputSelectionSupported = false;
  let receivedPacketCount = 0;
  let decodedFrameCount = 0;
  let decodeTimestamp = 0;
  let wakeLock: WakeLockSentinel | null = null;
  let chosenMetrics: string[] = [];
  let metricSelectionInitialized = false;
  let chartTimeRange = '30';
  let customRangeStart = '';
  let customRangeEnd = '';
  let metricSearch = '';
  let historyStatus = '';
  let historyLoading = false;
  $: availableMetrics = Array.from(new Set(telemetry.flatMap((event) => Object.entries(event.data)
    .filter(([, value]) => typeof value === 'number' && Number.isFinite(value))
    .map(([key]) => `${event.source}:${key}`)))).sort();
  $: if (!metricSelectionInitialized && availableMetrics.length) {
    chosenMetrics = availableMetrics.slice(0, 4);
    metricSelectionInitialized = true;
  }
  $: filteredMetricOptions = availableMetrics.filter((name) => name.toLowerCase().includes(metricSearch.toLowerCase()));
  $: chartBounds = timeBounds(telemetry, chartTimeRange, customRangeStart, customRangeEnd);
  $: charts = chosenMetrics.map((metric) => chartData(metric, telemetry, chartBounds));

  function telemetryTime(event: Telemetry): number {
    for (const value of [event.timestamp, event.received_at]) {
      if (value == null) continue;
      const numeric = Number(value);
      const ms = typeof value === 'number' || (/^\d+$/.test(String(value)) && numeric > 1_000_000_000)
        ? (numeric < 1e11 ? numeric * 1000 : numeric) : Date.parse(String(value));
      if (Number.isFinite(ms)) return ms;
    }
    return NaN;
  }

  function timeBounds(events: Telemetry[], range: string, startText: string, endText: string) {
    const times = events.map(telemetryTime).filter(Number.isFinite);
    const latest = Math.max(Date.now(), ...times);
    const earliest = times.length ? Math.min(...times) : latest - 30 * 60_000;
    if (range === 'custom') {
      const start = startText ? Date.parse(startText) : earliest;
      const end = endText ? Date.parse(endText) : latest;
      return { start: Number.isFinite(start) ? start : earliest, end: Math.max(Number.isFinite(end) ? end : latest, (Number.isFinite(start) ? start : earliest) + 1000) };
    }
    if (range === 'all') return { start: earliest, end: Math.max(latest, earliest + 1000) };
    const minutes = Number(range) || 30;
    return { start: latest - minutes * 60_000, end: latest };
  }

  function chartData(metric: string, events: Telemetry[], bounds: { start: number; end: number }) {
    const colon = metric.indexOf(':');
    const source = metric.slice(0, colon);
    const key = metric.slice(colon + 1);
    const entries = events.filter((event) => event.source === source && typeof event.data[key] === 'number')
      .map((event) => ({ time: telemetryTime(event), value: Number(event.data[key]) }))
      .filter((row) => Number.isFinite(row.time) && Number.isFinite(row.value) && row.time >= bounds.start && row.time <= bounds.end)
      .sort((a, b) => a.time - b.time);
    const min = entries.length ? Math.min(...entries.map((row) => row.value)) : 0;
    const max = entries.length ? Math.max(...entries.map((row) => row.value)) : 0;
    const span = max - min || 1;
    const interval = Math.max(1, bounds.end - bounds.start);
    const points = entries.map((row) => `${24 + (row.time - bounds.start) / interval * 452},${136 - (row.value - min) / span * 116}`).join(' ');
    return { metric, points, count: entries.length, latest: entries.length ? entries[entries.length - 1].value : null, min, max };
  }

  function setMetricVisible(metric: string, selected: boolean) {
    chosenMetrics = selected ? [...new Set([...chosenMetrics, metric])] : chosenMetrics.filter((item) => item !== metric);
    metricSelectionInitialized = true;
  }

  function zoomChart(inward: boolean) {
    if (chartTimeRange === 'custom') {
      const bounds = timeBounds(telemetry, chartTimeRange, customRangeStart, customRangeEnd);
      const middle = (bounds.start + bounds.end) / 2;
      const radius = (bounds.end - bounds.start) * (inward ? 0.25 : 1);
      customRangeStart = formatLocalDateTime(middle - radius);
      customRangeEnd = formatLocalDateTime(middle + radius);
      return;
    }
    const minutes = chartTimeRange === 'all' ? 1440 : Number(chartTimeRange);
    chartTimeRange = String(Math.max(1, Math.min(10080, Math.round(minutes * (inward ? 0.5 : 2)))));
  }

  function formatChartTime(time: number) {
    return new Date(time).toLocaleString('ja-JP', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  function formatLocalDateTime(time: number) {
    const date = new Date(time);
    return new Date(time - date.getTimezoneOffset() * 60_000).toISOString().slice(0, 16);
  }
  let preventSleep = true;
  let mediaSessionSupported = false;
  let showConnectionModal = false;
  let showAudioModal = false;
  type Room = { id: string; name: string };
  let rooms: Room[] = [];
  type MapPlot = { id: string; kind: 'point' | 'line'; coordinates: [number, number][]; color: string; label?: string };
  type MapConfig = { center_lat: number; center_lon: number; zoom: number; plots: MapPlot[] };
  let mapConfig: MapConfig = { center_lat: 35.6812, center_lon: 139.7671, zoom: 12, plots: [] };
  let mapInstance: any = null;
  let mapLayers: any[] = [];
  let mapVisible = false;
  let drawingKind: 'point' | 'line' | null = null;
  let drawingCoordinates: [number, number][] = [];
  let mapSaveTimer: ReturnType<typeof setTimeout> | null = null;
  let loggerPositions: { session: string; lat: number; lon: number; data: Record<string, unknown> }[] = [];
  let myPosition: { lat: number; lon: number; accuracy: number } | null = null;
  let locationError = '';
  let shareLocation = false;
  let locationTimer: ReturnType<typeof setInterval> | null = null;

  onMount(() => {
    updateEndpoint();
    try { preventSleep = localStorage.getItem('ecorun-prevent-sleep') !== 'false'; } catch { /* storage may be unavailable */ }
    try {
      forwardToLogger = localStorage.getItem('ecorun-forward-to-logger') !== 'false';
      const storedBitrate = Number(localStorage.getItem('ecorun-receive-bitrate'));
      const storedFrame = Number(localStorage.getItem('ecorun-receive-frame-ms'));
      if (RECEIVE_BITRATES.includes(storedBitrate)) receiveBitrate = storedBitrate;
      if (RECEIVE_FRAME_MS.includes(storedFrame)) receiveFrameMs = storedFrame;
      mapVisible = localStorage.getItem('ecorun-map-visible') === 'true';
      telemetryVisible = localStorage.getItem('ecorun-telemetry-visible') === 'true';

    } catch { /* storage may be unavailable */ }
    mediaSessionSupported = 'mediaSession' in navigator;
    updateMediaSession();
    void initializeMap();
    void refreshServers();
    void refreshRooms();
    outputSelectionSupported = typeof (AudioContext.prototype as AudioContext & { setSinkId?: (id: string) => Promise<void> }).setSinkId === 'function';
    void refreshAudioDevices();
    navigator.mediaDevices?.addEventListener?.('devicechange', refreshAudioDevices);
    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => { document.removeEventListener('visibilitychange', handleVisibilityChange); navigator.mediaDevices?.removeEventListener?.('devicechange', refreshAudioDevices); disconnect(); };
  });

  async function initializeMap() {
    try {
      await loadLeaflet();
      await loadMap();
    } catch (error) {
      console.error('[map] failed to initialize Leaflet', error);
      errorMessage = `地図を初期化できません: ${error}`;
    }
  }

  async function refreshRooms() {
    try {
      const response = await fetch('/api/rooms', { cache: 'no-store' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      rooms = await response.json();
      if (rooms.length && !rooms.some((room) => room.id === roomId)) roomId = rooms[0].id;
    } catch (error) {
      errorMessage = `Room一覧を取得できません: ${error}`;
    }
  }

  async function handleVisibilityChange() {
    if (document.visibilityState === 'visible' && status === 'connected') {
      if (audioContext?.state === 'suspended') await audioContext.resume().catch(() => {});
      if (preventSleep) await acquireWakeLock();
    }
  }

  async function acquireWakeLock() {
    if (!preventSleep || status !== 'connected' || document.visibilityState !== 'visible' || !('wakeLock' in navigator) || wakeLock) return;
    try {
      wakeLock = await navigator.wakeLock.request('screen');
      wakeLock.addEventListener('release', () => { wakeLock = null; });
    } catch (error) { console.warn('[audio diagnostics] screen wake lock unavailable', error); }
  }

  async function setPreventSleep(value: boolean) {
    preventSleep = value;
    try { localStorage.setItem('ecorun-prevent-sleep', String(value)); } catch { /* storage may be unavailable */ }
    if (value && status === 'connected') await acquireWakeLock();
    else await releaseWakeLock();
  }

  function updateMediaSession() {
    if (!('mediaSession' in navigator)) return;
    try {
      navigator.mediaSession.metadata = new MediaMetadata({
        title: status === 'connected' ? `EcoRun Logger · ${roomId || 'main'}` : 'EcoRun Logger',
        artist: ptt ? '音声送信中' : status === 'connected' ? '音声通話に接続中' : '未接続',
        album: 'Voice & telemetry'
      });
      navigator.mediaSession.playbackState = status === 'connected' ? 'playing' : 'paused';
    } catch (error) { console.warn('[media session] could not update playback status', error); }
  }

  async function releaseWakeLock() {
    const lock = wakeLock;
    wakeLock = null;
    if (lock) await lock.release().catch(() => {});
  }

  function isTypingTarget(target: EventTarget | null) {
    return target instanceof HTMLElement && (
      target.isContentEditable ||
      target.closest('input, textarea, select, [contenteditable="true"]') !== null
    );
  }

  function handleKeyDown(event: KeyboardEvent) {
    if (event.code !== 'Space' || event.repeat || isTypingTarget(event.target)) return;
    if (status !== 'connected' || muted) return;
    event.preventDefault();
    if (keyboardPttHeld) return;
    keyboardPttHeld = true;
    void setPtt(true);
  }

  function handleKeyUp(event: KeyboardEvent) {
    if (event.code !== 'Space' || !keyboardPttHeld) return;
    event.preventDefault();
    keyboardPttHeld = false;
    void setPtt(false);
  }

  function releaseKeyboardPtt() {
    if (!keyboardPttHeld) return;
    keyboardPttHeld = false;
    void setPtt(false);
  }

  async function refreshAudioDevices() {
    if (!navigator.mediaDevices?.enumerateDevices) return;
    try {
      const devices = await navigator.mediaDevices.enumerateDevices();
      inputDevices = devices.filter((device) => device.kind === 'audioinput');
      outputDevices = devices.filter((device) => device.kind === 'audiooutput');
      if (!inputDevices.some((device) => device.deviceId === selectedInputDevice)) selectedInputDevice = inputDevices[0]?.deviceId ?? '';
      if (!outputDevices.some((device) => device.deviceId === selectedOutputDevice)) selectedOutputDevice = outputDevices[0]?.deviceId ?? '';
    } catch (error) { console.warn('[audio diagnostics] failed to enumerate audio devices', error); }
  }

  async function selectOutputDevice() {
    if (!audioContext || !outputSelectionSupported) return;
    try {
      await (audioContext as AudioContext & { setSinkId: (id: string) => Promise<void> }).setSinkId(selectedOutputDevice);
    } catch (error) { errorMessage = `スピーカーを切り替えられません: ${error}`; }
  }

  function updateEndpoint() {
    const selected = servers.find((server) => server.id === selectedServer);
    endpoint = (selected?.url || location.origin).replace(/\/$/, '');
  }

  function serverEndpoints() {
    return [...new Set([endpoint, ...servers.filter((server) => server.available).map((server) => server.url || location.origin)]
      .map((url) => url.replace(/\/$/, '')).filter(Boolean))];
  }

  function stopFailover() {
    if (failoverTimer) clearTimeout(failoverTimer);
    failoverTimer = null;
    for (const candidate of failoverSockets) candidate.disconnect();
    failoverSockets = [];
  }

  function scheduleFailover(socket: Socket) {
    if (failoverTimer || socket !== ws) return;
    const generation = connectionGeneration;
    failoverTimer = setTimeout(() => {
      failoverTimer = null;
      if (generation !== connectionGeneration || socket !== ws || socket.connected) return;
      // Keep the original Socket.IO reconnection running while racing other servers.
      for (const candidate of failoverSockets) candidate.disconnect();
      failoverSockets = [];
      for (const url of serverEndpoints().filter((url) => url !== activeEndpoint)) {
        const candidate = io(`${url}/${encodeURIComponent(roomId.trim() || 'main')}`, {
          path: '/socket.io/', transports: ['websocket', 'polling'],
          reconnection: false, timeout: 5000
        });
        failoverSockets.push(candidate);
        candidate.on('connect', () => {
          if (generation !== connectionGeneration || socket.connected || socket !== ws) {
            candidate.disconnect();
            return;
          }
          // Promote the already-connected winner; do not open a second connection.
          socket.disconnect();
          ws = candidate;
          activeEndpoint = url;
          endpoint = url;
          connectionGeneration++;
          stopFailoverExcept(candidate);
          attachPrimaryHandlers(candidate, connectionGeneration);
          void initializeConnectedSession();
        });
      }
      if (generation === connectionGeneration && !socket.connected) scheduleFailover(socket);
    }, 1000);
  }

  function stopFailoverExcept(winner: Socket) {
    if (failoverTimer) clearTimeout(failoverTimer);
    failoverTimer = null;
    for (const candidate of failoverSockets) if (candidate !== winner) candidate.disconnect();
    failoverSockets = [];
  }

  function sendTelemetry(payload: object) {
    if (ws?.connected) ws.emit('telemetry', payload);
  }

  async function refreshServers() {
    try {
      const response = await fetch('/api/servers', { cache: 'no-store' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      servers = await response.json();
      if (!servers.some((server) => server.id === selectedServer)) selectedServer = servers[0]?.id ?? 'local';
      updateEndpoint();
    } catch (error) { errorMessage = `サーバ一覧を取得できません: ${error}`; }
  }

  function logBrowserDiagnostics(stage: string) {
    const diagnostics = {
      stage,
      userAgent: navigator.userAgent,
      platform: navigator.platform,
      secureContext: window.isSecureContext,
      audioEncoder: 'AudioEncoder' in window,
      audioDecoder: 'AudioDecoder' in window,
      audioData: 'AudioData' in window,
      encodedAudioChunk: 'EncodedAudioChunk' in window,
      mediaDevices: !!navigator.mediaDevices,
      getUserMedia: !!navigator.mediaDevices?.getUserMedia,
      audioContext: 'AudioContext' in window,
      serverSampleRate: SERVER_SAMPLE_RATE,
      serverFrameMs: SERVER_FRAME_MS,
      opusSampleRate,
      opusFrameMs,
      opusBitrate,
      frameSamples: Math.round(opusSampleRate * opusFrameMs / 1000)
    };
    //console.info('[audio diagnostics]', diagnostics);
    return diagnostics;
  }

  function sendControl(message: object) {
    const value = message as Record<string, unknown>;
    for (const socket of sockets) {
      if (!socket.connected) continue;
      if (value.type === 'audio.mute') socket.emit('audio.mute', { muted: value.muted });
      else if (value.type === 'audio.ptt') socket.emit('audio.ptt', { active: value.active });
    }
  }

  function sendAudioPreferences(socket?: Socket) {
    for (const peer of socket ? [socket] : sockets) {
      if (!peer.connected) continue;
      peer.emit('audio.forward_logger', { enabled: forwardToLogger });
      peer.emit('audio.receive_config', { bitrate: receiveBitrate, frame_ms: receiveFrameMs });
    }
  }

  function applyAudioPreferences() {
    try {
      localStorage.setItem('ecorun-forward-to-logger', String(forwardToLogger));
      localStorage.setItem('ecorun-receive-bitrate', String(receiveBitrate));
      localStorage.setItem('ecorun-receive-frame-ms', String(receiveFrameMs));
    } catch { /* storage may be unavailable */ }
    sendAudioPreferences();
  }

  async function loadTelemetryHistory() {
    if (historyLoading) return;
    historyLoading = true;
    historyStatus = '';
    activeSocket?.emit('telemetry.request');
    const target = activeSocket ? socketTargets.get(activeSocket) : (endpoint || location.origin);
    const room = roomId.trim() || 'main';
    try {
      const params = new URLSearchParams();
      if (chartTimeRange === 'custom') {
        if (customRangeStart) params.set('since', new Date(customRangeStart).toISOString());
        if (customRangeEnd) params.set('until', new Date(customRangeEnd).toISOString());
      }
      const response = await fetch(`${target}/api/rooms/${encodeURIComponent(room)}/telemetry?${params}`, {
        cache: 'no-store'
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const events = await response.json();
      if (!Array.isArray(events)) throw new Error('履歴の形式が正しくありません');
      if (room === (roomId.trim() || 'main')) {
        for (const event of events) if (event && typeof event.source === 'string' && event.data && typeof event.data === 'object') {
          handleEvent({ type: 'telemetry', ...event });
        }
      }
      historyStatus = `履歴を${events.length}件読み込みました（最大5000件）。`;
    } catch (error) {
      historyStatus = `履歴HTTP取得不可 (${error})。Socket.IOで直近のデータを再取得しました。`;
    } finally {
      historyLoading = false;
    }
  }

  function connectionTargets() {
    const configured = servers.filter((server) => server.available).map((server) => (server.url || location.origin).replace(/\/$/, ''));
    return [...new Set([...configured, ...(endpoint ? [endpoint.replace(/\/$/, '')] : []), location.origin.replace(/\/$/, '')])];
  }

  // Keep all nodes connected for fast failover, but show/play/transmit only
  // through the node that currently has the logger for this room.
  function chooseActiveServer() {
    const connected = sockets.filter((candidate) => candidate.connected);
    const present = connected.filter((candidate) => loggerPresence.get(candidate) === true);
    let next = activeSocket?.connected && (loggerPresence.get(activeSocket) === true || !present.length)
      ? activeSocket : (present[0] ?? connected[0] ?? null);
    // Allow immediate failover after a transport disconnect. If the current
    // transport still works, wait for the other node's logger presence to be
    // stable to prevent LTE flapping from bouncing the pit display.
    if (activeSocket?.connected && next && next !== activeSocket && present.includes(next)
      && Date.now() - (loggerPresenceSince.get(next) ?? Date.now()) < LOGGER_SWITCH_STABLE_MS) {
      next = activeSocket;
    }
    if (activeSocket !== next) {
      activeSocket = next;
      // Preserve already displayed events across failover. The new node's
      // telemetry.state will merge into the same deduplicated timeline.
      receivedAudioKeys.clear();
      if (next) {
        next.emit('telemetry.request');
        next.emit('map.request');
      }
      renderMap();
    }
    const url = activeSocket ? socketTargets.get(activeSocket) : undefined;
    activeServerName = servers.find((server) => (server.url || location.origin).replace(/\/$/, '') === url)?.name ?? (url || '未接続');
    activeLoggerConnected = !!activeSocket && loggerPresence.get(activeSocket) === true;
  }

  async function connect() {
    if (sockets.length) disconnect();
    errorMessage = '';
    status = 'connecting';
    logBrowserDiagnostics('connect');
    try {
      sockets = connectionTargets().map((target) => {
        const socket = io(`${target}/${encodeURIComponent(roomId.trim() || 'main')}`, {
          // Cross-origin standby hosts can be reached directly over WebSocket;
          // HTTP polling would additionally require CORS preflight handling.
          path: '/socket.io/', transports: ['websocket'],
          reconnection: true, reconnectionAttempts: Infinity,
          reconnectionDelay: 500, reconnectionDelayMax: 10000, randomizationFactor: 0.5,
        });
        socketTargets.set(socket, target);
        socket.on('connect', async () => {
          connectedSocketCount += 1;
          socket.emit('client.identify', { client_id: clientId });
          socket.emit('audio.mute', { muted });
          socket.emit('audio.ptt', { active: ptt && !muted });
          sendAudioPreferences(socket);
          status = 'connected';
          chooseActiveServer();
          // If the standby missed edits while offline, copy the active
          // room map to it when it becomes reachable again.
          if (mapStateReceived && activeSocket !== socket && activeSocket?.connected) {
            socket.emit('map.update', mapConfig);
          }
          updateMediaSession();
          errorMessage = '';
          if (connectedSocketCount === 1) {
            await setupDecoder();
            await setupAudioCapture();
            if (shareLocation) startLocationUpdates();
          }
          if (preventSleep) await acquireWakeLock();
        });
        socket.on('connect_error', (error) => {
          if (!sockets.some((candidate) => candidate.connected)) status = 'reconnecting';
          errorMessage = `Socket.IO接続エラー: ${error.message}`;
        });
        socket.on('disconnect', (reason) => {
          connectedSocketCount = Math.max(0, connectedSocketCount - 1);
          nodeParticipants.delete(socket);
          loggerPresence.delete(socket);
          loggerPresenceSince.delete(socket);
          chooseActiveServer();
          refreshParticipants();
          if (connectedSocketCount === 0) {
            stopLocationUpdates();
            status = 'reconnecting'; stopAudioCapture(); ptt = false; keyboardPttHeld = false;
            updateMediaSession(); void releaseWakeLock(); decoder?.close(); decoder = null;
          }
          if (reason === 'io server disconnect' && connectedSocketCount === 0) errorMessage = 'すべてのLoggerサーバーが接続を終了しました。';
        });
        socket.on('hello', (event) => handleEvent({ type: 'hello', ...event }));
        socket.on('logger.status', (event: { connected?: boolean }) => {
          const present = event.connected === true;
          if (present && loggerPresence.get(socket) !== true) loggerPresenceSince.set(socket, Date.now());
          if (!present) loggerPresenceSince.delete(socket);
          loggerPresence.set(socket, present);
          chooseActiveServer();
        });
        socket.on('audio.config', (event) => handleEvent({ type: 'audio.config', ...event }));
        socket.on('room.state', (event: { participants?: Participant[] }) => {
          nodeParticipants.set(socket, new Map((event.participants ?? []).map((participant) => [String(participant.session_id), participant])));
          refreshParticipants();
        });
        socket.on('telemetry.state', (events: Telemetry[]) => { if (activeSocket === socket) events.forEach((event) => handleEvent({ type: 'telemetry', ...event })); });
        socket.on('telemetry', (event) => { if (activeSocket === socket) handleEvent({ type: 'telemetry', ...event }); });
        socket.on('map.state', (event) => { if (activeSocket === socket) { mapConfig = event; mapStateReceived = true; renderMap(); } });
        socket.on('participant.joined', (participant: Participant) => updateNodeParticipant(socket, participant));
        socket.on('participant.updated', (participant: Participant) => updateNodeParticipant(socket, participant));
        socket.on('participant.left', ({ session_id }: { session_id: string }) => {
          nodeParticipants.get(socket)?.delete(String(session_id));
          refreshParticipants();
        });
        socket.on('audio.packet', async (senderId: string, senderSequence: number, packet: ArrayBuffer | Uint8Array | Blob) => {
          if (activeSocket !== socket) return;
          const key = `${senderId}:${senderSequence}`;
          if (receivedAudioKeys.has(key)) return;
          receivedAudioKeys.add(key);
          if (receivedAudioKeys.size > 4096) receivedAudioKeys.clear();
          let bytes: Uint8Array;
          if (packet instanceof Blob) bytes = new Uint8Array(await packet.arrayBuffer());
          else if (packet instanceof ArrayBuffer) bytes = new Uint8Array(packet);
          else bytes = packet;
          receivedPacketCount++;
          await decodePacket(bytes);
        });
        socket.on('error', (event) => handleEvent({ type: 'error', ...event }));
        return socket;
      });

    } catch (error) { status = 'disconnected'; errorMessage = String(error); }
  }

  function updateNodeParticipant(socket: Socket, participant: Participant) {
    if (!nodeParticipants.has(socket)) nodeParticipants.set(socket, new Map());
    nodeParticipants.get(socket)!.set(String(participant.session_id), participant);
    refreshParticipants();
  }

  function refreshParticipants() {
    const unique = new Map<string, Participant>();
    for (const members of nodeParticipants.values()) {
      for (const participant of members.values()) {
        const key = participant.client_id || String(participant.session_id);
        const existing = unique.get(key);
        if (!existing || (participant.client_id && !existing.client_id)) unique.set(key, participant);
      }
    }
    participants = [...unique.values()];
  }

  function handleEvent(event: any) {
    if (event.type === 'hello') { /* Connection is identified by the stable client ID. */ }
    if (event.type === 'telemetry') {
      const item: Telemetry = { event_id: event.event_id, source: event.source ?? 'unknown', timestamp: event.timestamp, received_at: event.received_at ?? Date.now(), data: event.data ?? {} };
      const key = item.event_id ?? `${item.source}:${item.timestamp ?? ''}:${JSON.stringify(item.data)}`;
      if (!telemetryKeys.has(key)) {
        telemetryKeys.add(key);
        if (telemetryKeys.size > 1000) telemetryKeys.clear();
        if (telemetryVisible) telemetry = [item, ...telemetry].slice(0, 5000);
        const position = findPosition(item.data);
        if (position) {
          const session = String(item.data.session_id ?? item.data.id ?? item.source);
          loggerPositions = [...loggerPositions.filter((entry) => entry.session !== session), { session, ...position, data: item.data }];
          renderMap();
        }

      }
    }
    if (event.type === 'error') errorMessage = event.message ?? event.code ?? 'server error';
  }

  function findPosition(data: Record<string, unknown>) {
    const normalized = new Map(Object.entries(data).map(([key, value]) => [key.replace(/[^a-z\d]/gi, '').toLowerCase(), value]));
    const lat = Number(normalized.get('lat') ?? normalized.get('latitude') ?? normalized.get('gpslat') ?? normalized.get('gnsslat'));
    const lon = Number(normalized.get('lon') ?? normalized.get('lng') ?? normalized.get('longitude') ?? normalized.get('gpslon') ?? normalized.get('gnsslon'));

    return Number.isFinite(lat) && Number.isFinite(lon) && Math.abs(lat) <= 90 && Math.abs(lon) <= 180 ? { lat, lon } : null;
  }

  async function loadMap() {
    try {
      const response = await fetch(`/api/rooms/${encodeURIComponent(roomId.trim() || 'main')}/map`, { cache: 'no-store' });
      if (response.ok) { mapConfig = await response.json(); renderMap(); }
    } catch (error) { console.warn('[map] failed to load map config', error); }
  }

  function scheduleMapSave() {
    if (mapSaveTimer) clearTimeout(mapSaveTimer);
    mapSaveTimer = setTimeout(() => void saveMap(), 350);
  }

  async function saveMap() {
    if (!mapInstance) return;
    const center = mapInstance.getCenter();
    mapConfig = { ...mapConfig, center_lat: center.lat, center_lon: center.lng, zoom: mapInstance.getZoom() };
    mapStateReceived = true;
    if (sockets.some((socket) => socket.connected)) {
      for (const socket of sockets) if (socket.connected) socket.emit('map.update', mapConfig);
      return;
    }
    try {
      await fetch(`/api/rooms/${encodeURIComponent(roomId.trim() || 'main')}/map`, { method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify(mapConfig) });
    } catch (error) { console.warn('[map] failed to save map config', error); }
  }

  function renderMap() {
    if (!mapInstance) return;
    mapInstance.setView([mapConfig.center_lat, mapConfig.center_lon], mapConfig.zoom, { animate: false });
    mapLayers.forEach((layer) => layer.remove()); mapLayers = [];
    for (const plot of mapConfig.plots) {
      const points = plot.coordinates.map(([lat, lon]) => [lat, lon]);
      const layer = plot.kind === 'line'
        ? L.polyline(points, { color: plot.color, weight: 5 })
        : L.circleMarker(points[0], { color: plot.color, radius: 7, fillOpacity: 0.9 });
      if (plot.label) layer.bindTooltip(plot.label);
      layer.addTo(mapInstance); mapLayers.push(layer);
    }
    for (const position of loggerPositions) {
      const layer = L.circleMarker([position.lat, position.lon], { color: '#58a6ff', radius: 8, fillOpacity: 0.95 }).bindTooltip(`Logger ${position.session}`);
      layer.addTo(mapInstance); mapLayers.push(layer);
    }
    if (myPosition) {
      const layer = L.circleMarker([myPosition.lat, myPosition.lon], { color: '#f2cc60', radius: 9, fillOpacity: 0.95 }).bindTooltip(`自分の現在地（精度 ±${Math.round(myPosition.accuracy)} m）`);
      layer.addTo(mapInstance); mapLayers.push(layer);
    }
    if (drawingCoordinates.length) {
      const preview = drawingKind === 'line' ? L.polyline(drawingCoordinates, { dashArray: '6 6' }) : L.circleMarker(drawingCoordinates[0], { radius: 6 });
      preview.addTo(mapInstance); mapLayers.push(preview);
    }
  }

  function startDrawing(kind: 'point' | 'line') { drawingKind = kind; drawingCoordinates = []; renderMap(); }
  function toggleMap() {
    mapVisible = !mapVisible;
    try { localStorage.setItem('ecorun-map-visible', String(mapVisible)); } catch { /* storage may be unavailable */ }
    if (mapVisible) setTimeout(() => mapInstance?.invalidateSize(), 0);
  }
  function toggleTelemetry() {
    telemetryVisible = !telemetryVisible;
    if (!telemetryVisible) telemetry = [];
    try { localStorage.setItem('ecorun-telemetry-visible', String(telemetryVisible)); } catch { /* storage may be unavailable */ }
  }
  function deletePlot(id: string) {
    mapConfig = { ...mapConfig, plots: mapConfig.plots.filter((plot) => plot.id !== id) };
    renderMap();
    scheduleMapSave();
  }
  function finishDrawing() {
    if (!drawingKind || !drawingCoordinates.length) return;
    mapConfig = { ...mapConfig, plots: [...mapConfig.plots, { id: crypto.randomUUID(), kind: drawingKind, coordinates: drawingCoordinates, color: '#58a6ff' }] };
    drawingKind = null; drawingCoordinates = []; renderMap(); scheduleMapSave();
  }

  function initMap(element: HTMLElement) {
    if (mapInstance) return;
    void loadLeaflet().then(() => {
      if (mapInstance) return;
      mapInstance = L.map(element).setView([mapConfig.center_lat, mapConfig.center_lon], mapConfig.zoom);
      L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19, attribution: '&copy; OpenStreetMap contributors' }).addTo(mapInstance);
      mapInstance.on('moveend zoomend', scheduleMapSave);
      mapInstance.on('click', (event: any) => {
        if (!drawingKind) return;
        drawingCoordinates = [...drawingCoordinates, [event.latlng.lat, event.latlng.lng]];
        if (drawingKind === 'point') finishDrawing(); else renderMap();
      });
      renderMap();
    }).catch((error) => {
      console.error('[map] failed to initialize Leaflet', error);
      errorMessage = `地図を初期化できません: ${error}`;
    });
  }

  async function setupDecoder() {
    if (!('AudioDecoder' in window) || !('EncodedAudioChunk' in window)) {
      console.error('[audio diagnostics] cannot initialize decoder: WebCodecs decode API missing', { audioDecoder: 'AudioDecoder' in window, encodedAudioChunk: 'EncodedAudioChunk' in window });
      errorMessage = '受信音声を再生できません: AudioDecoder / EncodedAudioChunk APIがありません';
      return;
    }
    audioContext ??= new AudioContext();
    if (selectedOutputDevice && outputSelectionSupported) await selectOutputDevice();
    gainNode ??= audioContext.createGain();
    gainNode.gain.value = volume;
    gainNode.connect(audioContext.destination);
    console.info('[audio diagnostics] decoder audio output initialized', { audioContextState: audioContext.state, sampleRate: audioContext.sampleRate, destinationMaxChannelCount: audioContext.destination.maxChannelCount, gain: gainNode.gain.value });
    decoder = new AudioDecoder({
      output: (audioData) => {
        decodedFrameCount++;
        const samples = new Float32Array(audioData.numberOfFrames);
        audioData.copyTo(samples, { planeIndex: 0, format: 'f32-planar' });
        let peak = 0; let sumSquares = 0;
        for (const sample of samples) { const abs = Math.abs(sample); if (abs > peak) peak = abs; sumSquares += sample * sample; }
        const rms = samples.length ? Math.sqrt(sumSquares / samples.length) : 0;
        //console.debug('[audio diagnostics] decoder output PCM', { frameNumber: decodedFrameCount, frames: audioData.numberOfFrames, channels: audioData.numberOfChannels, sampleRate: audioData.sampleRate, timestamp: audioData.timestamp, duration: audioData.duration, peak, rms, audioContextState: audioContext?.state, volume });
        if (!audioContext || audioContext.state !== 'running') console.warn('[audio diagnostics] decoded audio cannot be heard while AudioContext is not running', { state: audioContext?.state });
        const buffer = audioContext!.createBuffer(1, samples.length, audioData.sampleRate);
        buffer.copyToChannel(samples, 0);
        const node = audioContext!.createBufferSource();
        node.buffer = buffer; node.connect(gainNode!);
        const now = audioContext!.currentTime;
        if (playAt < now || playAt - now > 0.24) playAt = now + 0.04;
        const scheduledAt = playAt;
        node.onended = () => node.disconnect();
        node.start(scheduledAt); playAt += buffer.duration;
        //console.debug('[audio diagnostics] scheduled playback', { scheduledAt, currentTime: now, delaySeconds: scheduledAt - now, duration: buffer.duration, gain: gainNode!.gain.value });
        audioData.close();
      },
      error: (error) => { console.error('[audio diagnostics] Opus decoder error', { error, decoderState: decoder?.state, receivedPacketCount, decodedFrameCount }); errorMessage = `Opus decode error: ${error}`; }
    });
    try {
      const config: AudioDecoderConfig = { codec: 'opus', sampleRate: SERVER_SAMPLE_RATE, numberOfChannels: 1 };
      if (typeof AudioDecoder.isConfigSupported === 'function') {
        const support = await AudioDecoder.isConfigSupported(config);
        console.info('[audio diagnostics] Opus decoder config support', { config, supported: support.supported, returnedConfig: support.config });
        if (!support.supported) throw new Error(`AudioDecoder config unsupported: ${JSON.stringify(config)}`);
      }
      decoder.configure(config);
      receivedPacketCount = 0; decodedFrameCount = 0; decodeTimestamp = 0; playAt = 0;
      console.info('[audio diagnostics] decoder configured', { state: decoder.state, config });
    } catch (error) {
      //console.error('[audio diagnostics] failed to configure decoder', error);
      errorMessage = `受信Opusデコーダー初期化エラー: ${error}`;
    }
  }

  async function decodePacket(packet: Uint8Array) {
    if (!packet.byteLength) { console.warn('[audio diagnostics] received empty audio packet'); return; }
    if (!decoder || decoder.state !== 'configured') {
      console.warn('[audio diagnostics] dropping received audio packet: decoder unavailable/not configured', { byteLength: packet.byteLength, decoderState: decoder?.state ?? 'missing' });
      return;
    }
    try {
      decoder.decode(new EncodedAudioChunk({ type: 'key', timestamp: decodeTimestamp, data: packet }));
      //console.debug('[audio diagnostics] packet queued for decode', { byteLength: packet.byteLength, timestamp: decodeTimestamp, decodeQueueSize: decoder.decodeQueueSize });
      // The server adapts packet duration to the receiver's requested frame
      // size. The Opus decoder itself determines the actual decoded samples.
      decodeTimestamp += receiveFrameMs * 1_000;
    } catch (error) {
      console.error('[audio diagnostics] decoder.decode threw', { error, byteLength: packet.byteLength, decoderState: decoder.state });
      errorMessage = `Opus packet decode failed: ${error}`;
    }
  }

  async function setPtt(value: boolean) {
    if (value && (!sockets.some((socket) => socket.connected) || muted || !mediaStream || !encoder || encoder.state !== 'configured')) return;
    ptt = value;
    updateMediaSession();
    sendControl({ type: 'audio.ptt', active: value });
    if (mediaStream) mediaStream.getAudioTracks().forEach((track) => { track.enabled = value; });
    if (value && audioContext?.state === 'suspended') await audioContext.resume();
  }

  async function applyAudioEncoderSettings() {
    if (ptt) await setPtt(false);
    if (status !== 'connected') return;
    stopAudioCapture();
    await setupAudioCapture();
  }

  function startLocationUpdates() {
    if (locationTimer || !navigator.geolocation) return;
    const sendLocation = () => navigator.geolocation.getCurrentPosition((position) => {
      myPosition = { lat: position.coords.latitude, lon: position.coords.longitude, accuracy: position.coords.accuracy };
      locationError = '';
      renderMap();
      if (activeSocket?.connected) activeSocket.emit('telemetry', { source: 'web-client', timestamp: new Date(position.timestamp).toISOString(), data: { latitude: position.coords.latitude, longitude: position.coords.longitude, accuracy_m: position.coords.accuracy } });

    }, (error) => {
      locationError = `現在地を取得できません: ${error.message}`;
    }, { enableHighAccuracy: false, maximumAge: 30000, timeout: 10000 });
    sendLocation();
    locationTimer = setInterval(sendLocation, 60000);
  }

  function stopLocationUpdates() {
    if (locationTimer) clearInterval(locationTimer);
    locationTimer = null;
  }

  function setShareLocation(value: boolean) {
    shareLocation = value;
    if (value && status === 'connected') startLocationUpdates();
    else {
      stopLocationUpdates();
      myPosition = null;
      locationError = '';
      renderMap();
    }
  }

  function showMyLocation() {
    if (!navigator.geolocation) {
      locationError = 'このブラウザでは位置情報を利用できません';
      return;
    }
    navigator.geolocation.getCurrentPosition((position) => {
      myPosition = { lat: position.coords.latitude, lon: position.coords.longitude, accuracy: position.coords.accuracy };
      locationError = '';
      mapInstance?.setView([myPosition.lat, myPosition.lon], Math.max(mapInstance.getZoom(), 14));
      renderMap();
    }, (error) => {
      locationError = `現在地を取得できません: ${error.message}`;
    }, { enableHighAccuracy: false, maximumAge: 30000, timeout: 10000 });
  }

  function setMute(value: boolean) {
    muted = value;
    sendControl({ type: 'audio.mute', muted: value });
    if (value) {
      ptt = false;
      stopAudioCapture();
    } else if (status === 'connected') {
      void setupAudioCapture();
    }
  }

  function setVolume(value: number) {
    volume = value;
    if (gainNode) gainNode.gain.value = value;
  }

  function setMicVolume(value: number) {
    micVolume = value;
    if (micGainNode) micGainNode.gain.value = micVolume * micBoost;
  }

  function setMicBoost(value: number) {
    micBoost = value;
    if (micGainNode) micGainNode.gain.value = micVolume * micBoost;
  }

  async function changeInputDevice() {
    if (ptt) await setPtt(false);
    if (status === 'connected') {
      stopAudioCapture();
      await setupAudioCapture();
    }
  }

  async function setupAudioCapture() {
    if (!sockets.some((socket) => socket.connected)) return;
    if (captureSetupPromise) { await captureSetupPromise; return; }
    if (!sockets.some((socket) => socket.connected) || encoder) return;
    const generation = captureGeneration;
    const setup = (async () => {
      const diagnostics = logBrowserDiagnostics('setup-audio-capture');
      if (!('AudioEncoder' in window) || !('AudioData' in window) || !('EncodedAudioChunk' in window)) {
        console.warn('[audio diagnostics] WebCodecs API missing; cannot start encoder', diagnostics);
        errorMessage = `WebCodecs APIが不足しているためOpus送信を開始できません (AudioEncoder: ${diagnostics.audioEncoder}, AudioData: ${diagnostics.audioData}, EncodedAudioChunk: ${diagnostics.encodedAudioChunk}, secureContext: ${diagnostics.secureContext})`;
        ptt = false; return;
      }
      audioContext ??= new AudioContext();
      await audioContext.resume();
      if (!sockets.some((socket) => socket.connected)) return;
      console.info('[audio diagnostics] AudioContext ready', { state: audioContext.state, sampleRate: audioContext.sampleRate });
      const stream = await navigator.mediaDevices.getUserMedia({ audio: { ...(selectedInputDevice ? { deviceId: { exact: selectedInputDevice } } : {}), channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true } });
      if (!sockets.some((socket) => socket.connected) || generation !== captureGeneration) {
        stream.getTracks().forEach((track) => track.stop());
        return;
      }
      mediaStream = stream;
      stream.getAudioTracks().forEach((track) => { track.enabled = false; });
      await refreshAudioDevices();
      if (!sockets.some((socket) => socket.connected) || generation !== captureGeneration || mediaStream !== stream) {
        stream.getTracks().forEach((track) => track.stop());
        if (mediaStream === stream) mediaStream = null;
        return;
      }
      console.info('[audio diagnostics] microphone acquired', stream.getAudioTracks().map((track) => ({ label: track.label, enabled: track.enabled, muted: track.muted, readyState: track.readyState, settings: track.getSettings() })));
      sourceNode = audioContext.createMediaStreamSource(stream);
      micGainNode = audioContext.createGain();
      micGainNode.gain.value = micVolume * micBoost;
      processor = audioContext.createScriptProcessor(2048, 1, 1);
      const nextEncoder = new AudioEncoder({
        output: (chunk) => {
          const data = new Uint8Array(chunk.byteLength); chunk.copyTo(data);
          //console.debug('[audio diagnostics] encoded Opus chunk', { byteLength: chunk.byteLength, timestamp: chunk.timestamp, duration: chunk.duration, type: chunk.type });
          if (sockets.some((socket) => socket.connected)) {
            const packetSequence = sequence++;
            if (activeSocket?.connected) activeSocket.emit('audio.packet', { client_id: clientId, sequence: packetSequence, packet: data });
          }
        },
        error: (error) => { console.error('[audio diagnostics] Opus encoder error', error); errorMessage = `Opus encode error: ${error}`; }
      });
      const frameSamples = Math.round(opusSampleRate * opusFrameMs / 1000);
      const encoderConfig: AudioEncoderConfig = {
        codec: 'opus',
        sampleRate: opusSampleRate,
        numberOfChannels: 1,
        bitrate: opusBitrate,
        // Ask the native Opus encoder for the exact packet duration instead
        // of relying on the implementation to infer it from AudioData size.
        opus: { frameDuration: opusFrameMs * 1000, application: 'voip' }
      } as AudioEncoderConfig;
      if (typeof AudioEncoder.isConfigSupported === 'function') {
        const support = await AudioEncoder.isConfigSupported(encoderConfig);
        console.info('[audio diagnostics] Opus encoder config support', { config: encoderConfig, supported: support.supported, returnedConfig: support.config });
        if (!support.supported) throw new Error(`AudioEncoder config unsupported: ${JSON.stringify(encoderConfig)}`);
      } else {
        console.warn('[audio diagnostics] AudioEncoder.isConfigSupported unavailable');
      }
      if (!sockets.some((socket) => socket.connected) || generation !== captureGeneration) {
        nextEncoder.close();
        stream.getTracks().forEach((track) => track.stop());
        return;
      }
      try {
        nextEncoder.configure(encoderConfig);
      } catch (error) {
        // Some browser/OS combinations report a configuration as supported
        // and still reject it during configure(). Do not leave an unusable
        // encoder behind, and make the failure visible without an uncaught
        // promise rejection.
        nextEncoder.close();
        throw new Error(`Opus encoder configure failed (${opusSampleRate} Hz, ${opusFrameMs} ms, ${opusBitrate} bps): ${error}`);
      }
      encoder = nextEncoder;
      console.info('[audio diagnostics] encoder configured', { state: nextEncoder.state, encodeQueueSize: nextEncoder.encodeQueueSize });
      pendingPcm = new Float32Array(0); encodeTimestamp = 0;
      processor.onaudioprocess = (event) => {
        if (generation !== captureGeneration || !ptt || encoder !== nextEncoder || nextEncoder.state !== 'configured') return;
        const downsampled = resample(event.inputBuffer.getChannelData(0), event.inputBuffer.sampleRate, opusSampleRate);
        const merged = new Float32Array(pendingPcm.length + downsampled.length);
        merged.set(pendingPcm); merged.set(downsampled, pendingPcm.length);
        let offset = 0;
        while (merged.length - offset >= frameSamples) {
          const frame = merged.slice(offset, offset + frameSamples);
          const data = new AudioData({ format: 'f32', sampleRate: opusSampleRate, numberOfFrames: frameSamples, numberOfChannels: 1, timestamp: encodeTimestamp, data: frame });
          try {
            nextEncoder.encode(data);
            encodeTimestamp += opusFrameMs * 1_000;
          } catch (error) {
            console.error('[audio diagnostics] Opus encoder encode threw', {
              error,
              sampleRate: opusSampleRate,
              frameMs: opusFrameMs,
              bitrate: opusBitrate,
              frameSamples,
              encoderState: nextEncoder.state
            });
            errorMessage = `Opus encode failed: ${error}`;
          } finally {
            try { data.close(); } catch { /* encoder may have consumed/closed it */ }
          }
          offset += frameSamples;
        }
        pendingPcm = merged.slice(offset);
      };
      sourceNode.connect(micGainNode); micGainNode.connect(processor); processor.connect(audioContext.destination);
    })();
    captureSetupPromise = setup;
    try {
      await setup;
    } catch (error) {
      console.error('[audio diagnostics] failed to initialize microphone', error);
      errorMessage = `マイクを初期化できません: ${error}`;
      if (generation === captureGeneration) stopAudioCapture();
      ptt = false;
    } finally {
      if (captureSetupPromise === setup) captureSetupPromise = null;
    }
  }

  function resample(input: Float32Array, fromRate: number, toRate: number) {
    if (fromRate === toRate) return input.slice();
    const length = Math.max(1, Math.round(input.length * toRate / fromRate));
    const output = new Float32Array(length); const ratio = fromRate / toRate;
    for (let i = 0; i < length; i++) { const p = i * ratio; const left = Math.floor(p); const right = Math.min(left + 1, input.length - 1); const f = p - left; output[i] = input[left] * (1 - f) + input[right] * f; }
    return output;
  }

  function stopAudioCapture() {
    captureGeneration += 1;
    ptt = false;
    processor?.disconnect(); sourceNode?.disconnect(); micGainNode?.disconnect(); processor = null; sourceNode = null; micGainNode = null;
    mediaStream?.getTracks().forEach((track) => track.stop()); mediaStream = null;
    encoder?.close(); encoder = null; pendingPcm = new Float32Array(0);
  }

  function disconnect() {
    keyboardPttHeld = false; ptt = false; sendControl({ type: 'audio.ptt', active: false });
    stopAudioCapture(); stopLocationUpdates(); void releaseWakeLock();
    sockets.forEach((socket) => socket.disconnect()); sockets = []; nodeParticipants.clear();
    activeSocket = null; activeServerName = ''; activeLoggerConnected = false; socketTargets.clear(); loggerPresence.clear(); loggerPresenceSince.clear(); mapStateReceived = false;
    participants = []; connectedSocketCount = 0; receivedAudioKeys.clear(); telemetryKeys.clear();
    telemetry = []; loggerPositions = []; metricSelectionInitialized = false; chosenMetrics = []; historyStatus = '';
    decoder?.close(); decoder = null; status = 'disconnected'; updateMediaSession();

  }

  function joinRoom() {
    if (!rooms.some((room) => room.id === roomId.trim())) {
      errorMessage = '存在しないroomには接続できません。';
      return;
    }
    updateEndpoint();
    connect();
    void loadMap();
  }
</script>

<svelte:window on:keydown={handleKeyDown} on:keyup={handleKeyUp} on:blur={releaseKeyboardPtt} />

<svelte:head>
  <title>EcoRun Logger</title>
</svelte:head>

<main>
  <header><div><p class="eyebrow">ECORUN LOGGER</p><h1>Voice & telemetry</h1><p class="room-subtitle">Room: <strong>{roomId}</strong> · 表示・通話先: <strong>{activeServerName || '未接続'}</strong> {#if status === 'connected'}({activeLoggerConnected ? 'ロガー接続中' : 'ロガー待機中 / Podium直接送信の可能性'}){/if}</p></div><div class="header-actions"><span class:online={status === 'connected'} class="status">{status}</span><button class="secondary compact" on:click={() => showConnectionModal = true}>Connection</button><button class="secondary compact" on:click={() => showAudioModal = true}>Audio</button></div></header>
  {#if errorMessage}<p class="error top-error">{errorMessage}</p>{/if}
  <section class="grid">
    <article class="card ptt-card"><div><p class="eyebrow">PUSH TO TALK</p><h2>{muted ? 'Muted' : ptt ? 'Transmitting' : 'Ready'}</h2><p>Client {clientId.slice(0, 8)} · {participants.length} participant{participants.length === 1 ? '' : 's'}</p></div><button class:active={ptt} class="ptt" disabled={status !== 'connected' || muted} on:pointerdown={() => setPtt(true)} on:pointerup={() => setPtt(false)} on:pointercancel={() => setPtt(false)} on:pointerleave={() => setPtt(false)}> {ptt ? 'RELEASE' : 'HOLD TO TALK'} </button><button class="secondary" disabled={status !== 'connected'} on:click={() => setMute(!muted)}>{muted ? 'Unmute' : 'Mute'}</button><small>Spaceキーでも送信できます</small></article>
    <article class="card"><p class="eyebrow">ROOM</p><h2>{roomId}</h2>{#if participants.length}<ul class="participants">{#each participants as participant}<li><span class:me={participant.client_id === clientId}>● {participant.client_id ? `Client ${participant.client_id.slice(0, 8)}` : `Session ${participant.session_id}`}</span>{participant.muted ? ' · muted' : participant.ptt ? ' · talking' : ' · ready'}</li>{/each}</ul>{:else}<p class="muted">参加者なし</p>{/if}
      <p class="hint">Audio settings are available from the Audio button.</p>
    </article>
  </section>
  <section class="card map-card"><div class="section-title"><div><p class="eyebrow">LIVE MAP</p><h2>Logger positions & plots</h2></div><div class="map-actions"><button class="secondary compact" aria-expanded={mapVisible} on:click={toggleMap}>{mapVisible ? 'マップを非表示' : 'マップを表示'}</button>{#if mapVisible}<button class="secondary compact" on:click={showMyLocation}>現在地</button><button class="secondary compact" on:click={() => startDrawing('point')}>Add point</button><button class="secondary compact" on:click={() => startDrawing('line')}>Draw line</button>{#if drawingKind}<button class="compact" on:click={finishDrawing}>Finish</button><button class="secondary compact" on:click={() => { drawingKind = null; drawingCoordinates = []; renderMap(); }}>Cancel</button>{/if}{/if}</div></div><div class="map" class:map-hidden={!mapVisible} use:initMap></div>{#if mapVisible}<p class="hint">現在地は「位置情報を1分ごとに送信する」を有効にすると地図上に表示されます。「現在地」で一度だけ取得して地図を移動することもできます。</p>{#if locationError}<p class="error">{locationError}</p>{/if}{#if mapConfig.plots.length}<div class="plot-list"><p class="hint">保存済みの線・ポイント（削除するとルーム内の全員に反映されます）</p>{#each mapConfig.plots as plot, index (plot.id)}<div class="plot-row"><span>{plot.kind === 'line' ? '線' : 'ポイント'} {index + 1}{plot.label ? ` · ${plot.label}` : ''} <small>({plot.coordinates.length}地点)</small></span><button class="secondary compact" aria-label={`${plot.kind === 'line' ? '線' : 'ポイント'} ${index + 1}を削除`} on:click={() => deletePlot(plot.id)}>削除</button></div>{/each}</div>{/if}{/if}</section>
  <section class="card telemetry"><div class="map-actions"><button class="secondary compact" aria-expanded={telemetryVisible} on:click={toggleTelemetry}>{telemetryVisible ? 'メトリクスを非表示' : 'メトリクスを表示'}</button></div>{#if telemetryVisible}
    <div class="section-title"><div><p class="eyebrow">TELEMETRY</p><h2>Metrics timeline</h2></div><span>{telemetry.length} 件保持</span></div>
    <div class="chart-controls">
      <label>表示期間
        <select bind:value={chartTimeRange}>
          <option value="5">直近5分</option><option value="30">直近30分</option><option value="120">直近2時間</option>
          <option value="1440">直近24時間</option><option value="all">保持中の全期間</option><option value="custom">日時指定</option>
        </select>
      </label>
      <button class="secondary compact" on:click={() => zoomChart(true)} aria-label="時間軸を拡大">拡大 +</button>
      <button class="secondary compact" on:click={() => zoomChart(false)} aria-label="時間軸を縮小">縮小 −</button>
      <button class="secondary compact" on:click={loadTelemetryHistory} disabled={historyLoading}>{historyLoading ? '取得中…' : '履歴検索 / 更新'}</button>
    </div>
    {#if chartTimeRange === 'custom'}
      <div class="chart-controls date-controls">
        <label>開始日時<input type="datetime-local" bind:value={customRangeStart} /></label>
        <label>終了日時<input type="datetime-local" bind:value={customRangeEnd} /></label>
      </div>
    {/if}
    {#if historyStatus}<p class="hint" role="status">{historyStatus}</p>{/if}
    {#if availableMetrics.length}
      <div class="metric-picker">
        <label>表示項目を検索<input type="search" bind:value={metricSearch} placeholder="例: Speed, Latitude" /></label>
        <div class="metric-options">
          {#each filteredMetricOptions as metric (metric)}
            <label class="metric-option"><input type="checkbox" checked={chosenMetrics.includes(metric)} on:change={(event) => setMetricVisible(metric, (event.currentTarget as HTMLInputElement).checked)} />{metric}</label>
          {/each}
        </div>
      </div>
      {#if charts.length}
        <div class="charts">
          {#each charts as chart (chart.metric)}
            <article class="chart">
              <div class="section-title"><strong>{chart.metric}</strong><span>{chart.latest ?? '—'} · {chart.count}点</span></div>
              <svg viewBox="0 0 500 150" preserveAspectRatio="none" role="img" aria-label={`${chart.metric} 時系列グラフ`}>
                <line x1="24" y1="20" x2="476" y2="20" stroke="#30363d" />
                <line x1="24" y1="78" x2="476" y2="78" stroke="#30363d" />
                <line x1="24" y1="136" x2="476" y2="136" stroke="#58606d" />
                <polyline fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round" points={chart.points} />
              </svg>
              <div class="chart-axis"><time>{formatChartTime(chartBounds.start)}</time><time>{formatChartTime(chartBounds.end)}</time></div>
              <p class="chart-values">最小 {chart.count ? chart.min : '—'} / 最大 {chart.count ? chart.max : '—'}{chart.count ? '' : ' · この期間に記録なし'}</p>
            </article>
          {/each}
        </div>
      {:else}<p class="muted">表示項目を選択してください</p>{/if}
    {:else}<p class="muted">数値telemetryイベントを待っています</p>{/if}
    {#if telemetry.length}<div class="events">{#each telemetry.slice(0, 12) as event}<article><strong>{event.source}</strong><time>{event.timestamp ?? event.received_at ?? 'no timestamp'}</time><pre>{JSON.stringify(event.data, null, 2)}</pre></article>{/each}</div>{/if}
  {/if}</section>


  {#if showConnectionModal}<div class="modal-backdrop" role="presentation" on:click={(event) => { if (event.currentTarget === event.target) showConnectionModal = false; }}><section class="modal" role="dialog" aria-modal="true"><div class="section-title"><div><p class="eyebrow">CONNECTION</p><h2>Room & server</h2></div><button class="secondary compact" on:click={() => showConnectionModal = false}>Close</button></div><label>Logger server<select bind:value={selectedServer} on:change={updateEndpoint}>{#each servers as server}<option value={server.id} disabled={!server.available}>{server.name} {server.available ? '' : '(unavailable)'}</option>{/each}</select></label><label>Server URL<input bind:value={endpoint} /></label><label>Room<select bind:value={roomId}>{#each rooms as room}<option value={room.id}>{room.name} ({room.id})</option>{/each}</select></label><div class="row"><button on:click={joinRoom}>{status === 'connected' ? 'Switch room' : 'Connect'}</button>{#if status === 'connected' || status === 'reconnecting'}<button class="secondary" on:click={disconnect}>Disconnect</button>{/if}</div><label class="setting"><input type="checkbox" checked={preventSleep} on:change={(event) => setPreventSleep((event.currentTarget as HTMLInputElement).checked)} /> 接続中は画面スリープを防止する</label><label class="setting"><input type="checkbox" checked={shareLocation} on:change={(event) => setShareLocation((event.currentTarget as HTMLInputElement).checked)} /> 位置情報を1分ごとに送信する</label><p class="hint">{mediaSessionSupported ? 'Media Session API対応ブラウザです。' : 'Media Session APIは利用できません。'} 位置情報はブラウザの許可が必要です。</p></section></div>{/if}
  {#if showAudioModal}
    <div class="modal-backdrop" role="presentation" on:click={(event) => { if (event.currentTarget === event.target) showAudioModal = false; }}>
      <section class="modal" role="dialog" aria-modal="true">
        <div class="section-title"><div><p class="eyebrow">AUDIO</p><h2>Voice settings</h2></div><button class="secondary compact" on:click={() => showAudioModal = false}>Close</button></div>
        <label class="setting"><input type="checkbox" bind:checked={forwardToLogger} on:change={applyAudioPreferences} />自分の音声をロガーにも送る</label>
        <p class="hint">OFFでもピット内の通話には参加できます。接続先の全ノードへ設定を同期します。</p>
        <label class="volume">Speaker volume ({Math.round(volume * 100)}%)<input type="range" min="0" max="2" step="0.05" value={volume} on:input={(event) => setVolume(Number((event.currentTarget as HTMLInputElement).value))} /></label>
        <label class="volume">受信Opusビットレート（希望）<select bind:value={receiveBitrate} on:change={applyAudioPreferences}>{#each RECEIVE_BITRATES as bitrate}<option value={bitrate}>{bitrate / 1000} kbps</option>{/each}</select></label>
        <label class="volume">受信Opusパケット長（希望）<select bind:value={receiveFrameMs} on:change={applyAudioPreferences}>{#each RECEIVE_FRAME_MS as frameMs}<option value={frameMs}>{frameMs} ms</option>{/each}</select></label>
        <p class="hint">受信音質はサーバーへの希望値です。実際のビットレート・パケット長は接続先サーバーの再エンコード処理に依存します。</p>
        <label class="volume">Microphone<select class="device-select" bind:value={selectedInputDevice} on:change={changeInputDevice}><option value="">Default microphone</option>{#each inputDevices as device}<option value={device.deviceId}>{device.label || `Microphone ${device.deviceId.slice(0, 8)}`}</option>{/each}</select></label>
        <label class="volume">Mic volume ({Math.round(micVolume * 100)}%)<input type="range" min="0" max="2" step="0.05" value={micVolume} on:input={(event) => setMicVolume(Number((event.currentTarget as HTMLInputElement).value))} /></label>
        <label class="volume">Mic boost ({micBoost.toFixed(1)}×)<input type="range" min="1" max="4" step="0.25" value={micBoost} on:input={(event) => setMicBoost(Number((event.currentTarget as HTMLInputElement).value))} /></label>
        <label class="volume">送信Opus sample rate<select bind:value={opusSampleRate} on:change={applyAudioEncoderSettings}>{#each OPUS_SAMPLE_RATES as rate}<option value={rate}>{rate / 1000} kHz</option>{/each}</select></label>
        <label class="volume">送信Opus frame duration<select bind:value={opusFrameMs} on:change={applyAudioEncoderSettings}>{#each OPUS_FRAME_MS as frameMs}<option value={frameMs}>{frameMs} ms</option>{/each}</select></label>
        <label class="volume">送信Opus bitrate<select bind:value={opusBitrate} on:change={applyAudioEncoderSettings}>{#each RECEIVE_BITRATES as bitrate}<option value={bitrate}>{bitrate / 1000} kbps</option>{/each}</select></label>
        <label class="volume">Speaker output{#if outputSelectionSupported}<select bind:value={selectedOutputDevice} on:change={selectOutputDevice}><option value="">Default speaker</option>{#each outputDevices as device}<option value={device.deviceId}>{device.label || `Speaker ${device.deviceId.slice(0, 8)}`}</option>{/each}</select>{:else}<select disabled><option>Switching unsupported</option></select>{/if}</label>
        <p class="hint">送信設定はWebCodecsのOpusエンコーダーに適用します。受信はOpusデコーダーで再生します。</p>
      </section>
    </div>
  {/if}
</main>

<style>
  :global(*) { box-sizing: border-box; } :global(body) { margin: 0; min-width: 320px; background: #0d1117; color: #e6edf3; font-family: Inter, ui-sans-serif, system-ui, sans-serif; } :global(button), :global(input), :global(select) { font: inherit; }
  main { width: min(960px, calc(100% - 32px)); margin: 0 auto; padding: 28px 0 56px; } header, .row, .section-title { display: flex; align-items: center; justify-content: space-between; gap: 12px; } header { margin-bottom: 20px; } h1, h2, p { margin: 0; } h1 { font-size: clamp(28px, 6vw, 44px); letter-spacing: -.04em; } h2 { font-size: 20px; }
  .eyebrow { font-size: 11px; font-weight: 800; letter-spacing: .14em; color: #8b949e; margin-bottom: 7px; } .status { border: 1px solid #30363d; border-radius: 999px; padding: 6px 10px; color: #8b949e; font-size: 12px; } .status.online { color: #3fb950; border-color: #238636; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 16px; padding: 18px; margin-bottom: 16px; } label { display: grid; gap: 7px; color: #8b949e; font-size: 12px; flex: 1; } input, select { width: 100%; background: #0d1117; border: 1px solid #30363d; border-radius: 9px; color: #e6edf3; padding: 10px 11px; outline: none; } input:focus, select:focus { border-color: #58a6ff; } input[type="range"] { padding: 0; } .setting { display: flex; flex-direction: row; align-items: center; gap: 9px; margin: 12px 0; } .setting input { width: auto; }
  button { border: 0; border-radius: 10px; padding: 10px 15px; background: #238636; color: white; font-weight: 700; cursor: pointer; } button:disabled { opacity: .45; cursor: not-allowed; } button.secondary { background: #30363d; } button.compact { padding: 7px 10px; font-size: 12px; } .error { color: #ff7b72; margin-top: 12px; font-size: 13px; } .top-error { margin-bottom: 16px; }
  .header-actions, .map-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; } .room-subtitle { color: #8b949e; margin-top: 4px; font-size: 13px; }
  .grid { display: grid; grid-template-columns: 1.3fr 1fr; gap: 16px; } .ptt-card { min-height: 260px; display: grid; grid-template-columns: 1fr 190px; gap: 20px; align-items: center; } .ptt { width: 180px; height: 180px; border-radius: 50%; background: #21262d; border: 5px solid #30363d; } .ptt.active { background: #da3633; border-color: #ff7b72; transform: scale(.98); } .ptt-card small { grid-column: 1 / -1; color: #8b949e; }
  .participants { list-style: none; padding: 0; margin: 18px 0 0; display: grid; gap: 8px; color: #58a6ff; } .participants .me { font-weight: 800; } .muted { color: #8b949e; margin-top: 14px; } .volume { margin-top: 20px; } .hint { color: #8b949e; font-size: 11px; line-height: 1.5; margin-top: 14px; } .section-title > span { color: #8b949e; font-size: 12px; } .events { display: grid; gap: 10px; margin-top: 16px; } .events article { border: 1px solid #30363d; border-radius: 10px; padding: 12px; } .events time { color: #8b949e; font-size: 11px; margin-left: 10px; } pre { overflow: auto; margin: 10px 0 0; color: #c9d1d9; font-size: 11px; }
  .chart-controls { display: flex; align-items: end; gap: 10px; flex-wrap: wrap; margin-top: 14px; } .chart-controls label { min-width: 170px; max-width: 320px; } .chart-controls button { flex-shrink: 0; }
  .metric-picker { margin-top: 14px; } .metric-picker > label { max-width: 380px; } .metric-options { display: flex; gap: 7px; flex-wrap: wrap; max-height: 145px; overflow: auto; margin-top: 10px; } .metric-option { display: flex; flex: 0 1 auto; align-items: center; gap: 6px; max-width: 100%; padding: 6px 9px; border: 1px solid #30363d; border-radius: 8px; overflow-wrap: anywhere; } .metric-option input { width: auto; }
  .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(310px, 1fr)); gap: 10px; margin-top: 16px; } .chart { border: 1px solid #30363d; border-radius: 10px; padding: 12px; min-width: 0; } .chart .section-title { align-items: start; } .chart strong { overflow-wrap: anywhere; font-size: 13px; } .chart svg { width: 100%; height: 150px; margin-top: 8px; color: #58a6ff; background: #0d1117; border-radius: 6px; } .chart-axis { display: flex; justify-content: space-between; gap: 10px; color: #8b949e; font-size: 10px; } .chart-values { margin-top: 8px; font-size: 11px; color: #8b949e; }
  .map-card { overflow: hidden; } .map { height: min(58vh, 520px); min-height: 320px; border-radius: 12px; overflow: hidden; margin-top: 14px; background: #0d1117; } .modal-backdrop { position: fixed; inset: 0; z-index: 2000; background: rgb(0 0 0 / 65%); display: grid; place-items: center; padding: 18px; } .modal { width: min(560px, 100%); max-height: 90vh; overflow: auto; background: #161b22; border: 1px solid #30363d; border-radius: 16px; padding: 20px; box-shadow: 0 20px 60px rgb(0 0 0 / 40%); } .modal > label { margin-top: 14px; }
  @media (max-width: 700px) { main { width: min(100% - 20px, 960px); padding-top: 16px; } .grid, .ptt-card { grid-template-columns: 1fr; } .ptt { justify-self: center; } .row { flex-wrap: wrap; } .header-actions { justify-content: flex-end; } .map { min-height: 300px; } }
  .map.map-hidden { display: none; } .plot-list { margin-top: 16px; } .plot-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 9px 0; border-bottom: 1px solid #30363d; font-size: 13px; } .plot-row small { color: #8b949e; }
  .telemetry-count { color: #8b949e; font-size: 12px; }
  @media (max-width: 700px) { main { width: min(100% - 20px, 960px); padding-top: 16px; } .grid, .ptt-card { grid-template-columns: 1fr; } .ptt { justify-self: center; } .row { flex-wrap: wrap; } .row label { min-width: 100%; } .header-actions { justify-content: flex-end; } .map { min-height: 300px; } }

</style>
