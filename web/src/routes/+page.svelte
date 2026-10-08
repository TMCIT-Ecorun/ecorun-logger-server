<script lang="ts">
  import { onMount } from 'svelte';

  type Telemetry = { source: string; timestamp?: string; data: Record<string, unknown> };
  const SAMPLE_RATE = 8000;
  const FRAME_SAMPLES = 480;
  let roomId = 'main';
  let endpoint = '';
  let status = 'disconnected';
  let sessionId: number | null = null;
  type Participant = { session_id: number; muted: boolean; ptt: boolean };
  let participants: Participant[] = [];
  let telemetry: Telemetry[] = [];
  let errorMessage = '';
  let ptt = false;
  let muted = false;
  let ws: WebSocket | null = null;
  let audioContext: AudioContext | null = null;
  let mediaStream: MediaStream | null = null;
  let sourceNode: MediaStreamAudioSourceNode | null = null;
  let processor: ScriptProcessorNode | null = null;
  let encoder: AudioEncoder | null = null;
  let decoder: AudioDecoder | null = null;
  let pendingPcm = new Float32Array(0);
  let encodeTimestamp = 0;
  let playAt = 0;
  let gainNode: GainNode | null = null;
  let volume = 1;

  onMount(() => {
    updateEndpoint();
    return disconnect;
  });

  function updateEndpoint() {
    endpoint = `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws/audio/${encodeURIComponent(roomId.trim() || 'main')}`;
  }

  function sendControl(message: object) {
    if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message));
  }

  async function connect() {
    if (ws) disconnect();
    errorMessage = '';
    status = 'connecting';
    updateEndpoint();
    try {
      ws = new WebSocket(endpoint);
      ws.binaryType = 'arraybuffer';
      ws.onopen = async () => {
        status = 'connected';
        sendControl({ type: 'hello', protocol: 2, client_id: 'web-pwa' });
        sendControl({ type: 'room.join', room_id: roomId });
        await setupDecoder();
      };
      ws.onmessage = async (event) => {
        if (typeof event.data === 'string') handleEvent(JSON.parse(event.data));
        else await decodePacket(new Uint8Array(event.data));
      };
      ws.onerror = () => { errorMessage = 'WebSocket接続でエラーが発生しました'; };
      ws.onclose = () => {
        status = 'disconnected'; ws = null; stopAudioCapture();
        decoder?.close(); decoder = null;
      };
    } catch (error) { status = 'disconnected'; errorMessage = String(error); }
  }

  function handleEvent(event: any) {
    if (event.type === 'hello') sessionId = event.session_id ?? null;
    if (event.type === 'room.state') participants = event.participants ?? [];
    if (event.type === 'telemetry') telemetry = [{ source: event.source ?? 'unknown', timestamp: event.timestamp, data: event.data ?? {} }, ...telemetry].slice(0, 12);
    if (event.type === 'error') errorMessage = event.message ?? event.code ?? 'server error';
  }

  async function setupDecoder() {
    if (!('AudioDecoder' in window)) return;
    audioContext ??= new AudioContext();
    gainNode ??= audioContext.createGain();
    gainNode.gain.value = volume;
    gainNode.connect(audioContext.destination);
    decoder = new AudioDecoder({
      output: (audioData) => {
        const samples = new Float32Array(audioData.numberOfFrames);
        audioData.copyTo(samples, { planeIndex: 0 });
        const buffer = audioContext!.createBuffer(1, samples.length, audioData.sampleRate);
        buffer.copyToChannel(samples, 0);
        const node = audioContext!.createBufferSource();
        node.buffer = buffer; node.connect(gainNode!);
        const now = audioContext!.currentTime;
        if (playAt < now || playAt - now > 0.24) playAt = now + 0.04;
        node.start(playAt); playAt += buffer.duration; audioData.close();
      },
      error: (error) => { errorMessage = `Opus decode error: ${error}`; }
    });
    decoder.configure({ codec: 'opus', sampleRate: SAMPLE_RATE, numberOfChannels: 1 });
  }

  async function decodePacket(packet: Uint8Array) {
    if (!decoder || decoder.state !== 'configured') return;
    decoder.decode(new EncodedAudioChunk({ type: 'key', timestamp: encodeTimestamp, data: packet }));
  }

  async function setPtt(value: boolean) {
    ptt = value;
    sendControl({ type: 'audio.ptt', active: value });
    if (value) await startAudioCapture(); else stopAudioCapture();
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

  async function startAudioCapture() {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    if (!('AudioEncoder' in window)) { errorMessage = 'このブラウザはWebCodecsのOpus送信に対応していません'; ptt = false; return; }
    try {
      audioContext ??= new AudioContext(); await audioContext.resume();
      mediaStream = await navigator.mediaDevices.getUserMedia({ audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true } });
      sourceNode = audioContext.createMediaStreamSource(mediaStream);
      processor = audioContext.createScriptProcessor(2048, 1, 1);
      encoder = new AudioEncoder({
        output: (chunk) => { const data = new Uint8Array(chunk.byteLength); chunk.copyTo(data); if (ws?.readyState === WebSocket.OPEN) ws.send(data); },
        error: (error) => { errorMessage = `Opus encode error: ${error}`; }
      });
      encoder.configure({ codec: 'opus', sampleRate: SAMPLE_RATE, numberOfChannels: 1, bitrate: 8000 });
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
      sourceNode.connect(processor); processor.connect(audioContext.destination);
    } catch (error) { errorMessage = `マイクを開始できません: ${error}`; stopAudioCapture(); ptt = false; }
  }

  function resample(input: Float32Array, fromRate: number, toRate: number) {
    if (fromRate === toRate) return input.slice();
    const length = Math.max(1, Math.round(input.length * toRate / fromRate));
    const output = new Float32Array(length); const ratio = fromRate / toRate;
    for (let i = 0; i < length; i++) { const p = i * ratio; const left = Math.floor(p); const right = Math.min(left + 1, input.length - 1); const f = p - left; output[i] = input[left] * (1 - f) + input[right] * f; }
    return output;
  }

  function stopAudioCapture() {
    processor?.disconnect(); sourceNode?.disconnect(); processor = null; sourceNode = null;
    mediaStream?.getTracks().forEach((track) => track.stop()); mediaStream = null;
    encoder?.close(); encoder = null; pendingPcm = new Float32Array(0);
  }

  function disconnect() {
    ptt = false; sendControl({ type: 'audio.ptt', active: false }); stopAudioCapture(); sendControl({ type: 'room.leave' }); ws?.close(); ws = null; status = 'disconnected';
  }

  function joinRoom() {
    if (!roomId.trim()) return;
    updateEndpoint();
    sendControl({ type: 'room.join', room_id: roomId.trim() });
  }
</script>

<svelte:head>
  <title>EcoRun Logger</title>
  <link rel="manifest" href="/manifest.webmanifest" />
  <link rel="icon" href="/icon.svg" />
</svelte:head>

<main>
  <header><div><p class="eyebrow">ECORUN LOGGER</p><h1>Voice & telemetry</h1></div><span class:online={status === 'connected'} class="status">{status}</span></header>
  <section class="card connection">
    <label>WebSocket endpoint <input bind:value={endpoint} /></label>
    <div class="row"><label>Room <input bind:value={roomId} maxlength="64" /></label><button on:click={joinRoom} disabled={status !== 'connected'}>Join</button>{#if status === 'connected'}<button class="secondary" on:click={disconnect}>Disconnect</button>{:else}<button on:click={connect}>Connect</button>{/if}</div>
    {#if errorMessage}<p class="error">{errorMessage}</p>{/if}
  </section>
  <section class="grid">
    <article class="card ptt-card"><div><p class="eyebrow">PUSH TO TALK</p><h2>{muted ? 'Muted' : ptt ? 'Transmitting' : 'Ready'}</h2><p>Session {sessionId ?? '—'} · {participants.length} participant{participants.length === 1 ? '' : 's'}</p></div><button class:active={ptt} class="ptt" disabled={status !== 'connected' || muted} on:pointerdown={() => setPtt(true)} on:pointerup={() => setPtt(false)} on:pointercancel={() => setPtt(false)} on:pointerleave={() => setPtt(false)}> {ptt ? 'RELEASE' : 'HOLD TO TALK'} </button><button class="secondary" disabled={status !== 'connected'} on:click={() => setMute(!muted)}>{muted ? 'Unmute' : 'Mute'}</button><small>Spaceキーでも送信できます</small></article>
    <article class="card"><p class="eyebrow">ROOM</p><h2>{roomId}</h2>{#if participants.length}<ul class="participants">{#each participants as participant}<li><span class:me={participant.session_id === sessionId}>● Session {participant.session_id}</span>{participant.muted ? ' · muted' : participant.ptt ? ' · talking' : ' · ready'}</li>{/each}</ul>{:else}<p class="muted">参加者なし</p>{/if}<label class="volume">Volume <input type="range" min="0" max="1" step="0.05" value={volume} on:input={(event) => setVolume(Number((event.currentTarget as HTMLInputElement).value))} /></label></article>
  </section>
  <section class="card telemetry"><div class="section-title"><div><p class="eyebrow">TELEMETRY</p><h2>Latest events</h2></div><span>{telemetry.length}</span></div>{#if telemetry.length}<div class="events">{#each telemetry as event}<article><strong>{event.source}</strong><time>{event.timestamp ?? 'no timestamp'}</time><pre>{JSON.stringify(event.data, null, 2)}</pre></article>{/each}</div>{:else}<p class="muted">telemetryイベントを待っています</p>{/if}</section>
</main>

<style>
  :global(*) { box-sizing: border-box; } :global(body) { margin: 0; min-width: 320px; background: #0d1117; color: #e6edf3; font-family: Inter, ui-sans-serif, system-ui, sans-serif; } :global(button), :global(input) { font: inherit; }
  main { width: min(960px, calc(100% - 32px)); margin: 0 auto; padding: 28px 0 56px; } header, .row, .section-title { display: flex; align-items: center; justify-content: space-between; gap: 12px; } header { margin-bottom: 20px; } h1, h2, p { margin: 0; } h1 { font-size: clamp(28px, 6vw, 44px); letter-spacing: -.04em; } h2 { font-size: 20px; }
  .eyebrow { font-size: 11px; font-weight: 800; letter-spacing: .14em; color: #8b949e; margin-bottom: 7px; } .status { border: 1px solid #30363d; border-radius: 999px; padding: 6px 10px; color: #8b949e; font-size: 12px; } .status.online { color: #3fb950; border-color: #238636; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 16px; padding: 18px; margin-bottom: 16px; } label { display: grid; gap: 7px; color: #8b949e; font-size: 12px; flex: 1; } input { width: 100%; background: #0d1117; border: 1px solid #30363d; border-radius: 9px; color: #e6edf3; padding: 10px 11px; outline: none; } input:focus { border-color: #58a6ff; } .connection > label { margin-bottom: 12px; }
  button { border: 0; border-radius: 10px; padding: 10px 15px; background: #238636; color: white; font-weight: 700; cursor: pointer; } button:disabled { opacity: .45; cursor: not-allowed; } button.secondary { background: #30363d; } .error { color: #ff7b72; margin-top: 12px; font-size: 13px; }
  .grid { display: grid; grid-template-columns: 1.3fr 1fr; gap: 16px; } .ptt-card { min-height: 260px; display: grid; grid-template-columns: 1fr 190px; gap: 20px; align-items: center; } .ptt { width: 180px; height: 180px; border-radius: 50%; background: #21262d; border: 5px solid #30363d; } .ptt.active { background: #da3633; border-color: #ff7b72; transform: scale(.98); } .ptt-card small { grid-column: 1 / -1; color: #8b949e; }
  .participants { list-style: none; padding: 0; margin: 18px 0 0; display: grid; gap: 8px; color: #58a6ff; } .participants .me { font-weight: 800; } .muted { color: #8b949e; margin-top: 14px; } .volume { margin-top: 20px; } .section-title > span { color: #8b949e; font-size: 12px; } .events { display: grid; gap: 10px; margin-top: 16px; } .events article { border: 1px solid #30363d; border-radius: 10px; padding: 12px; } .events time { color: #8b949e; font-size: 11px; margin-left: 10px; } pre { overflow: auto; margin: 10px 0 0; color: #c9d1d9; font-size: 11px; }
  @media (max-width: 700px) { main { width: min(100% - 20px, 960px); padding-top: 16px; } .grid, .ptt-card { grid-template-columns: 1fr; } .ptt { justify-self: center; } .row { flex-wrap: wrap; } .row label { min-width: 100%; } }
</style>
