<script lang="ts">
  import { onMount } from 'svelte';
  import { io, type Socket } from 'socket.io-client';

  type Telemetry = { source: string; timestamp?: string; data: Record<string, unknown> };
  const SAMPLE_RATE = 8000;
  const FRAME_SAMPLES = 480;
  let roomId = 'main';
  let endpoint = '';
  type LoggerServer = { id: string; name: string; url: string; available: boolean };
  let servers: LoggerServer[] = [];
  let selectedServer = 'local';
  let status = 'disconnected';
  let sessionId: number | null = null;
  type Participant = { session_id: number | string; muted: boolean; ptt: boolean };
  let participants: Participant[] = [];
  let telemetry: Telemetry[] = [];
  let errorMessage = '';
  let ptt = false;
  let keyboardPttHeld = false;
  let muted = false;
  let ws: Socket | null = null;
  let audioContext: AudioContext | null = null;
  let mediaStream: MediaStream | null = null;
  let sourceNode: MediaStreamAudioSourceNode | null = null;
  let micGainNode: GainNode | null = null;
  let processor: ScriptProcessorNode | null = null;
  let encoder: AudioEncoder | null = null;
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
  let preventSleep = true;
  let mediaSessionSupported = false;

  onMount(() => {
    updateEndpoint();
    try { preventSleep = localStorage.getItem('ecorun-prevent-sleep') !== 'false'; } catch { /* storage may be unavailable */ }
    mediaSessionSupported = 'mediaSession' in navigator;
    updateMediaSession();
    void refreshServers();
    outputSelectionSupported = typeof (AudioContext.prototype as AudioContext & { setSinkId?: (id: string) => Promise<void> }).setSinkId === 'function';
    void refreshAudioDevices();
    navigator.mediaDevices?.addEventListener?.('devicechange', refreshAudioDevices);
    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => { document.removeEventListener('visibilitychange', handleVisibilityChange); navigator.mediaDevices?.removeEventListener?.('devicechange', refreshAudioDevices); disconnect(); };
  });

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
      sampleRate: SAMPLE_RATE,
      frameSamples: FRAME_SAMPLES
    };
    //console.info('[audio diagnostics]', diagnostics);
    return diagnostics;
  }

  function sendControl(message: object) {
    if (!ws?.connected) return;
    const value = message as Record<string, unknown>;
    if (value.type === 'audio.mute') ws.emit('audio.mute', { muted: value.muted });
    else if (value.type === 'audio.ptt') ws.emit('audio.ptt', { active: value.active });
  }

  async function connect() {
    if (ws) disconnect();
    errorMessage = '';
    status = 'connecting';
    updateEndpoint();
    logBrowserDiagnostics('connect');
    try {
      ws = io(`${endpoint}/${encodeURIComponent(roomId.trim() || 'main')}`, {
        path: '/socket.io/', transports: ['websocket', 'polling'],
        reconnection: true, reconnectionAttempts: Infinity,
        reconnectionDelay: 500, reconnectionDelayMax: 10000, randomizationFactor: 0.5,
      });
      ws.on('connect', async () => {
        status = 'connected';
        updateMediaSession();
        errorMessage = '';
        sessionId = null; participants = [];
        await setupDecoder();
        await setupAudioCapture();
        if (preventSleep) await acquireWakeLock();
      });
      ws.on('connect_error', (error) => { status = 'reconnecting'; errorMessage = `Socket.IO接続エラー: ${error.message}`; });
      ws.on('disconnect', (reason) => {
        status = 'reconnecting'; stopAudioCapture(); ptt = false; keyboardPttHeld = false;
        updateMediaSession();
        void releaseWakeLock();
        decoder?.close(); decoder = null;
        if (reason === 'io server disconnect') errorMessage = 'サーバーが接続を終了しました。再接続してください';
      });
      ws.on('hello', (event) => handleEvent({ type: 'hello', ...event }));
      ws.on('audio.config', (event) => handleEvent({ type: 'audio.config', ...event }));
      ws.on('room.state', (event) => handleEvent({ type: 'room.state', ...event }));
      ws.on('participant.joined', (participant) => updateParticipant(participant));
      ws.on('participant.updated', (participant) => updateParticipant(participant));
      ws.on('participant.left', ({ session_id }) => { participants = participants.filter((p) => String(p.session_id) !== String(session_id)); });
      ws.on('audio.packet', async (packet: ArrayBuffer | Uint8Array | Blob) => {
        let bytes: Uint8Array;
        if (packet instanceof Blob) bytes = new Uint8Array(await packet.arrayBuffer());
        else if (packet instanceof ArrayBuffer) bytes = new Uint8Array(packet);
        else bytes = packet;
        receivedPacketCount++;
        await decodePacket(bytes);
      });
      ws.on('error', (event) => handleEvent({ type: 'error', ...event }));
    } catch (error) { status = 'disconnected'; errorMessage = String(error); }
  }

  function updateParticipant(participant: Participant) {
    participants = [...participants.filter((p) => String(p.session_id) !== String(participant.session_id)), participant];
  }

  function handleEvent(event: any) {
    if (event.type === 'hello') sessionId = event.session_id ?? null;
    if (event.type === 'room.state') participants = event.participants ?? [];
    if (event.type === 'telemetry') telemetry = [{ source: event.source ?? 'unknown', timestamp: event.timestamp, data: event.data ?? {} }, ...telemetry].slice(0, 12);
    if (event.type === 'error') errorMessage = event.message ?? event.code ?? 'server error';
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
      const config: AudioDecoderConfig = { codec: 'opus', sampleRate: SAMPLE_RATE, numberOfChannels: 1 };
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
      decodeTimestamp += 60_000;
    } catch (error) {
      console.error('[audio diagnostics] decoder.decode threw', { error, byteLength: packet.byteLength, decoderState: decoder.state });
      errorMessage = `Opus packet decode failed: ${error}`;
    }
  }

  async function setPtt(value: boolean) {
    if (value && (!ws?.connected || muted || !mediaStream || !encoder || encoder.state !== 'configured')) return;
    ptt = value;
    updateMediaSession();
    sendControl({ type: 'audio.ptt', active: value });
    if (mediaStream) mediaStream.getAudioTracks().forEach((track) => { track.enabled = value; });
    if (value && audioContext?.state === 'suspended') await audioContext.resume();
  }

  function setMute(value: boolean) {
    muted = value;
    sendControl({ type: 'audio.mute', muted: value });
    if (value) {
      ptt = false;
      stopAudioCapture();
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
    if (!ws?.connected) return;
    const diagnostics = logBrowserDiagnostics('setup-audio-capture');
    if (!('AudioEncoder' in window) || !('AudioData' in window) || !('EncodedAudioChunk' in window)) {
      console.warn('[audio diagnostics] WebCodecs API missing; cannot start encoder', diagnostics);
      errorMessage = `WebCodecs APIが不足しているためOpus送信を開始できません (AudioEncoder: ${diagnostics.audioEncoder}, AudioData: ${diagnostics.audioData}, EncodedAudioChunk: ${diagnostics.encodedAudioChunk}, secureContext: ${diagnostics.secureContext})`;
      ptt = false; return;
    }
    try {
      audioContext ??= new AudioContext();
      await audioContext.resume();
      if (!ws?.connected) return;
      console.info('[audio diagnostics] AudioContext ready', { state: audioContext.state, sampleRate: audioContext.sampleRate });
      const stream = await navigator.mediaDevices.getUserMedia({ audio: { ...(selectedInputDevice ? { deviceId: { exact: selectedInputDevice } } : {}), channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true } });
      if (!ws?.connected) {
        stream.getTracks().forEach((track) => track.stop());
        return;
      }
      mediaStream = stream;
      stream.getAudioTracks().forEach((track) => { track.enabled = false; });
      await refreshAudioDevices();
      if (!ws?.connected || mediaStream !== stream) {
        stream.getTracks().forEach((track) => track.stop());
        if (mediaStream === stream) mediaStream = null;
        return;
      }
      console.info('[audio diagnostics] microphone acquired', stream.getAudioTracks().map((track) => ({ label: track.label, enabled: track.enabled, muted: track.muted, readyState: track.readyState, settings: track.getSettings() })));
      sourceNode = audioContext.createMediaStreamSource(stream);
      micGainNode = audioContext.createGain();
      micGainNode.gain.value = micVolume * micBoost;
      processor = audioContext.createScriptProcessor(2048, 1, 1);
      encoder = new AudioEncoder({
        output: (chunk) => {
          const data = new Uint8Array(chunk.byteLength); chunk.copyTo(data);
          //console.debug('[audio diagnostics] encoded Opus chunk', { byteLength: chunk.byteLength, timestamp: chunk.timestamp, duration: chunk.duration, type: chunk.type });
          if (ws?.connected) ws.emit('audio.packet', data);
        },
        error: (error) => { console.error('[audio diagnostics] Opus encoder error', error); errorMessage = `Opus encode error: ${error}`; }
      });
      const encoderConfig: AudioEncoderConfig = { codec: 'opus', sampleRate: SAMPLE_RATE, numberOfChannels: 1, bitrate: 8000 };
      if (typeof AudioEncoder.isConfigSupported === 'function') {
        const support = await AudioEncoder.isConfigSupported(encoderConfig);
        console.info('[audio diagnostics] Opus encoder config support', { config: encoderConfig, supported: support.supported, returnedConfig: support.config });
        if (!support.supported) throw new Error(`AudioEncoder config unsupported: ${JSON.stringify(encoderConfig)}`);
      } else {
        console.warn('[audio diagnostics] AudioEncoder.isConfigSupported unavailable');
      }
      encoder.configure(encoderConfig);
      console.info('[audio diagnostics] encoder configured', { state: encoder.state, encodeQueueSize: encoder.encodeQueueSize });
      pendingPcm = new Float32Array(0); encodeTimestamp = 0;
      processor.onaudioprocess = (event) => {
        if (!ptt || !encoder || encoder.state !== 'configured') return;
        const downsampled = resample(event.inputBuffer.getChannelData(0), event.inputBuffer.sampleRate, SAMPLE_RATE);
        const merged = new Float32Array(pendingPcm.length + downsampled.length);
        merged.set(pendingPcm); merged.set(downsampled, pendingPcm.length);
        let offset = 0;
        while (merged.length - offset >= FRAME_SAMPLES) {
          const frame = merged.slice(offset, offset + FRAME_SAMPLES);
          const data = new AudioData({ format: 'f32', sampleRate: SAMPLE_RATE, numberOfFrames: FRAME_SAMPLES, numberOfChannels: 1, timestamp: encodeTimestamp, data: frame });
          encoder.encode(data); data.close(); encodeTimestamp += 60_000; offset += FRAME_SAMPLES;
        }
        pendingPcm = merged.slice(offset);
      };
      sourceNode.connect(micGainNode); micGainNode.connect(processor); processor.connect(audioContext.destination);
    } catch (error) { console.error('[audio diagnostics] failed to initialize microphone', error); errorMessage = `マイクを初期化できません: ${error}`; stopAudioCapture(); ptt = false; }
  }

  function resample(input: Float32Array, fromRate: number, toRate: number) {
    if (fromRate === toRate) return input.slice();
    const length = Math.max(1, Math.round(input.length * toRate / fromRate));
    const output = new Float32Array(length); const ratio = fromRate / toRate;
    for (let i = 0; i < length; i++) { const p = i * ratio; const left = Math.floor(p); const right = Math.min(left + 1, input.length - 1); const f = p - left; output[i] = input[left] * (1 - f) + input[right] * f; }
    return output;
  }

  function stopAudioCapture() {
    ptt = false;
    processor?.disconnect(); sourceNode?.disconnect(); micGainNode?.disconnect(); processor = null; sourceNode = null; micGainNode = null;
    mediaStream?.getTracks().forEach((track) => track.stop()); mediaStream = null;
    encoder?.close(); encoder = null; pendingPcm = new Float32Array(0);
  }

  function disconnect() {
    keyboardPttHeld = false; ptt = false; sendControl({ type: 'audio.ptt', active: false }); stopAudioCapture(); void releaseWakeLock(); ws?.disconnect(); ws = null; status = 'disconnected'; updateMediaSession();
  }

  function joinRoom() {
    if (!roomId.trim()) return;
    updateEndpoint();
    if (status === 'connected' || status === 'reconnecting') connect();
  }
</script>

<svelte:window on:keydown={handleKeyDown} on:keyup={handleKeyUp} on:blur={releaseKeyboardPtt} />

<svelte:head>
  <title>EcoRun Logger</title>
</svelte:head>

<main>
  <header><div><p class="eyebrow">ECORUN LOGGER</p><h1>Voice & telemetry</h1></div><span class:online={status === 'connected'} class="status">{status}</span></header>
  <section class="card connection">
    <label>Logger server<select bind:value={selectedServer} on:change={updateEndpoint}>{#each servers as server}<option value={server.id} disabled={!server.available}>{server.name} {server.available ? '' : '(unavailable)'}</option>{/each}</select></label>
    <label>Server URL <input bind:value={endpoint} /></label>
    <div class="row"><label>Room <input bind:value={roomId} maxlength="64" /></label><button on:click={joinRoom} disabled={!roomId.trim()}>Join / switch</button>{#if status === 'connected' || status === 'reconnecting'}<button class="secondary" on:click={disconnect}>Disconnect</button>{:else}<button on:click={connect}>Connect</button>{/if}</div>
    <label class="setting"><input type="checkbox" checked={preventSleep} on:change={(event) => setPreventSleep((event.currentTarget as HTMLInputElement).checked)} /> 接続中は画面スリープを防止する（対応ブラウザのみ）</label>
    <p class="hint">{mediaSessionSupported ? 'メディアセッションAPI対応：Androidなどのメディア表示に接続状態を通知します。表示可否はOS・ブラウザに依存します。' : 'このブラウザはメディアセッションAPIに対応していません。'}</p>
    {#if errorMessage}<p class="error">{errorMessage}</p>{/if}
  </section>
  <section class="grid">
    <article class="card ptt-card"><div><p class="eyebrow">PUSH TO TALK</p><h2>{muted ? 'Muted' : ptt ? 'Transmitting' : 'Ready'}</h2><p>Session {sessionId ?? '—'} · {participants.length} participant{participants.length === 1 ? '' : 's'}</p></div><button class:active={ptt} class="ptt" disabled={status !== 'connected' || muted} on:pointerdown={() => setPtt(true)} on:pointerup={() => setPtt(false)} on:pointercancel={() => setPtt(false)} on:pointerleave={() => setPtt(false)}> {ptt ? 'RELEASE' : 'HOLD TO TALK'} </button><button class="secondary" disabled={status !== 'connected'} on:click={() => setMute(!muted)}>{muted ? 'Unmute' : 'Mute'}</button><small>Spaceキーでも送信できます</small></article>
    <article class="card"><p class="eyebrow">ROOM</p><h2>{roomId}</h2>{#if participants.length}<ul class="participants">{#each participants as participant}<li><span class:me={participant.session_id === sessionId}>● Session {participant.session_id}</span>{participant.muted ? ' · muted' : participant.ptt ? ' · talking' : ' · ready'}</li>{/each}</ul>{:else}<p class="muted">参加者なし</p>{/if}
      <label class="volume">スピーカー音量 ({Math.round(volume * 100)}%)<input type="range" min="0" max="2" step="0.05" value={volume} on:input={(event) => setVolume(Number((event.currentTarget as HTMLInputElement).value))} /></label>
      <label class="volume">マイク<select class="device-select" aria-label="マイク選択" bind:value={selectedInputDevice} on:change={changeInputDevice}><option value="">既定のマイク</option>{#each inputDevices as device}<option value={device.deviceId}>{device.label || `マイク ${device.deviceId.slice(0, 8)}`}</option>{/each}</select></label>
      <label class="volume">マイク音量 ({Math.round(micVolume * 100)}%)<input type="range" min="0" max="2" step="0.05" value={micVolume} on:input={(event) => setMicVolume(Number((event.currentTarget as HTMLInputElement).value))} /></label>
      <label class="volume">マイク増幅 ({micBoost.toFixed(1)}×)<input type="range" min="1" max="4" step="0.25" value={micBoost} on:input={(event) => setMicBoost(Number((event.currentTarget as HTMLInputElement).value))} /></label>
      <label class="volume">スピーカー選択{#if outputSelectionSupported}<select bind:value={selectedOutputDevice} on:change={selectOutputDevice}><option value="">既定のスピーカー</option>{#each outputDevices as device}<option value={device.deviceId}>{device.label || `スピーカー ${device.deviceId.slice(0, 8)}`}</option>{/each}</select>{:else}<select disabled><option>このブラウザでは切替非対応</option></select>{/if}</label>
      <p class="hint">マイク音量・増幅は送信前の音声に適用されます。増幅しすぎると音割れすることがあります。</p>
    </article>
  </section>
  <section class="card telemetry"><div class="section-title"><div><p class="eyebrow">TELEMETRY</p><h2>Latest events</h2></div><span>{telemetry.length}</span></div>{#if telemetry.length}<div class="events">{#each telemetry as event}<article><strong>{event.source}</strong><time>{event.timestamp ?? 'no timestamp'}</time><pre>{JSON.stringify(event.data, null, 2)}</pre></article>{/each}</div>{:else}<p class="muted">telemetryイベントを待っています</p>{/if}</section>
</main>

<style>
  :global(*) { box-sizing: border-box; } :global(body) { margin: 0; min-width: 320px; background: #0d1117; color: #e6edf3; font-family: Inter, ui-sans-serif, system-ui, sans-serif; } :global(button), :global(input), :global(select) { font: inherit; }
  main { width: min(960px, calc(100% - 32px)); margin: 0 auto; padding: 28px 0 56px; } header, .row, .section-title { display: flex; align-items: center; justify-content: space-between; gap: 12px; } header { margin-bottom: 20px; } h1, h2, p { margin: 0; } h1 { font-size: clamp(28px, 6vw, 44px); letter-spacing: -.04em; } h2 { font-size: 20px; }
  .eyebrow { font-size: 11px; font-weight: 800; letter-spacing: .14em; color: #8b949e; margin-bottom: 7px; } .status { border: 1px solid #30363d; border-radius: 999px; padding: 6px 10px; color: #8b949e; font-size: 12px; } .status.online { color: #3fb950; border-color: #238636; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 16px; padding: 18px; margin-bottom: 16px; } label { display: grid; gap: 7px; color: #8b949e; font-size: 12px; flex: 1; } input, select { width: 100%; background: #0d1117; border: 1px solid #30363d; border-radius: 9px; color: #e6edf3; padding: 10px 11px; outline: none; } input:focus, select:focus { border-color: #58a6ff; } input[type="range"] { padding: 0; } .connection > label { margin-bottom: 12px; } .connection label.setting { display: flex; flex-direction: row; align-items: center; gap: 9px; margin: 8px 0 12px; } .setting input { width: auto; }
  button { border: 0; border-radius: 10px; padding: 10px 15px; background: #238636; color: white; font-weight: 700; cursor: pointer; } button:disabled { opacity: .45; cursor: not-allowed; } button.secondary { background: #30363d; } .error { color: #ff7b72; margin-top: 12px; font-size: 13px; }
  .grid { display: grid; grid-template-columns: 1.3fr 1fr; gap: 16px; } .ptt-card { min-height: 260px; display: grid; grid-template-columns: 1fr 190px; gap: 20px; align-items: center; } .ptt { width: 180px; height: 180px; border-radius: 50%; background: #21262d; border: 5px solid #30363d; } .ptt.active { background: #da3633; border-color: #ff7b72; transform: scale(.98); } .ptt-card small { grid-column: 1 / -1; color: #8b949e; }
  .participants { list-style: none; padding: 0; margin: 18px 0 0; display: grid; gap: 8px; color: #58a6ff; } .participants .me { font-weight: 800; } .muted { color: #8b949e; margin-top: 14px; } .volume { margin-top: 20px; } .hint { color: #8b949e; font-size: 11px; line-height: 1.5; margin-top: 14px; } .section-title > span { color: #8b949e; font-size: 12px; } .events { display: grid; gap: 10px; margin-top: 16px; } .events article { border: 1px solid #30363d; border-radius: 10px; padding: 12px; } .events time { color: #8b949e; font-size: 11px; margin-left: 10px; } pre { overflow: auto; margin: 10px 0 0; color: #c9d1d9; font-size: 11px; }
  @media (max-width: 700px) { main { width: min(100% - 20px, 960px); padding-top: 16px; } .grid, .ptt-card { grid-template-columns: 1fr; } .ptt { justify-self: center; } .row { flex-wrap: wrap; } .row label { min-width: 100%; } }
</style>
