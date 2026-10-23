"""One Discord account bridges each enabled EcoRun room to its configured VC.

Run as a companion process on the Logger host. No message-content intent needed.
"""
import asyncio
import logging
import os
import queue
import sqlite3
import threading
from collections import deque

import discord
from discord.ext import voice_recv
import numpy as np
import opuslib
import socketio
from scipy.signal import resample_poly

LOG = logging.getLogger('discord_bridge')
RATE = 8000
FRAME = 480  # 60 ms, mono
VOICE_FRAME = 960  # 20 ms, stereo at 48 kHz
SILENCE = bytes(VOICE_FRAME * 2 * 2)


class Playback(discord.AudioSource):
    def __init__(self):
        self.frames = queue.Queue(maxsize=24)
        self.pending = bytearray()

    def read(self):
        while len(self.pending) < len(SILENCE):
            try:
                self.pending.extend(self.frames.get_nowait())
            except queue.Empty:
                break
        if len(self.pending) < len(SILENCE):
            self.pending.extend(bytes(len(SILENCE) - len(self.pending)))
        chunk = bytes(self.pending[:len(SILENCE)])
        del self.pending[:len(SILENCE)]
        return chunk

    def is_opus(self):
        return False

    def push(self, pcm):
        try:
            self.frames.put_nowait(pcm)
        except queue.Full:
            # Never build seconds of stale audio during network stalls.
            self.pending.clear()
            while not self.frames.empty():
                try:
                    self.frames.get_nowait()
                except queue.Empty:
                    break


class Receive(voice_recv.AudioSink):
    def __init__(self, bridge):
        super().__init__()
        self.bridge = bridge
        self.buffers = {}
        self.encoders = {}
        self.lock = threading.Lock()

    def wants_opus(self):
        return False

    def write(self, user, data):
        if user is None or user.bot or not data.pcm:
            return
        # data.pcm: 48 kHz, signed i16, 2-channel interleaved, 20 ms.
        stereo = np.frombuffer(data.pcm, dtype='<i2').reshape(-1, 2)
        mono = stereo.astype(np.float32).mean(axis=1)
        down = resample_poly(mono, 1, 6).clip(-32768, 32767).astype('<i2')
        with self.lock:
            key = user.id
            buf = self.buffers.setdefault(key, deque())
            buf.extend(down.tolist())
            encoder = self.encoders.setdefault(key, opuslib.Encoder(RATE, 1, opuslib.APPLICATION_VOIP))
            while len(buf) >= FRAME:
                frame = np.array([buf.popleft() for _ in range(FRAME)], dtype='<i2')
                packet = encoder.encode(frame.tobytes(), FRAME)
                self.bridge.send_from_thread(packet)

    def cleanup(self):
        with self.lock:
            self.buffers.clear()
            self.encoders.clear()


class RoomBridge:
    def __init__(self, bot, room, channel_id, key):
        self.bot = bot
        self.room = room
        self.channel_id = int(channel_id)
        self.key = key
        self.socket = socketio.AsyncClient(reconnection=True, logger=False)
        self.playback = Playback()
        self.decoder = opuslib.Decoder(RATE, 1)
        self.voice = None
        self.loop = asyncio.get_running_loop()

        @self.socket.on('audio.packet', namespace=f'/{room}')
        def on_audio(packet):
            try:
                decoded = self.decoder.decode(bytes(packet), FRAME * 2, False)
                mono = np.frombuffer(decoded, dtype='<i2')
                stereo = np.repeat(resample_poly(mono, 6, 1).clip(-32768, 32767).astype('<i2')[:, None], 2, axis=1)
                self.playback.push(stereo.astype('<i2').tobytes())
            except Exception:
                LOG.exception('Discord playback decode failed: %s', room)

    def send_from_thread(self, packet):
        if self.socket.connected:
            asyncio.run_coroutine_threadsafe(self.socket.emit('audio.packet', packet, namespace=f'/{self.room}'), self.loop)

    async def start(self):
        channel = self.bot.get_channel(self.channel_id)
        if not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
            raise ValueError(f'Channel {self.channel_id} is missing or is not a VC')
        # Discord allows one voice connection per guild per bot token.
        if channel.guild.voice_client is not None:
            raise ValueError(f'Bot already connected to a VC in guild {channel.guild.id}')
        ns = f'/{self.room}'
        @self.socket.event(namespace=ns)
        async def connect():
            await self.socket.emit('audio.ptt', {'active': True}, namespace=ns)
        await self.socket.connect(os.environ.get('ECORUN_DISCORD_SERVER_URL', 'http://127.0.0.1:3000'), namespaces=[ns], auth={'token': self.key}, wait_timeout=10)
        try:
            self.voice = await channel.connect(cls=voice_recv.VoiceRecvClient, timeout=20)
            self.voice.play(self.playback)
            self.voice.listen(Receive(self))
        except Exception:
            await self.socket.disconnect()
            raise
        LOG.info('Connected room %s to Discord VC %s', self.room, self.channel_id)

    async def stop(self):
        if self.voice:
            try:
                if self.voice.is_listening():
                    self.voice.stop_listening()
                if self.voice.is_playing():
                    self.voice.stop()
                await self.voice.disconnect(force=True)
            finally:
                self.voice = None
        if self.socket.connected:
            await self.socket.disconnect()


class BridgeBot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents(guilds=True, voice_states=True))
        self.bridges = {}
        self.poll_task = None

    async def setup_hook(self):
        self.poll_task = asyncio.create_task(self.watch())

    async def close(self):
        if self.poll_task:
            self.poll_task.cancel()
        for bridge in list(self.bridges.values()):
            await bridge.stop()
        await super().close()

    def configs(self):
        db = os.environ.get('ECORUN_DB_PATH', 'ecorun.sqlite3')
        with sqlite3.connect(f'file:{db}?mode=ro', uri=True) as connection:
            return {room: (channel, key) for room, channel, key in connection.execute(
                'SELECT id,discord_channel_id,socket_key FROM rooms WHERE discord_channel_id IS NOT NULL'
            )}

    async def watch(self):
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                configs = await asyncio.to_thread(self.configs)
                for room, bridge in list(self.bridges.items()):
                    if configs.get(room) != (str(bridge.channel_id), bridge.key) or not bridge.voice or not bridge.voice.is_connected():
                        await bridge.stop()
                        del self.bridges[room]
                for room, (channel, key) in configs.items():
                    if room in self.bridges:
                        continue
                    bridge = RoomBridge(self, room, channel, key)
                    try:
                        await bridge.start()
                        self.bridges[room] = bridge
                    except Exception as error:
                        LOG.warning('Cannot connect room %s: %s', room, error)
                        await bridge.stop()
            except Exception:
                LOG.exception('Discord config polling failed')
            await asyncio.sleep(5)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    token = os.environ.get('ECORUN_DISCORD_BOT_TOKEN')
    if not token:
        raise SystemExit('ECORUN_DISCORD_BOT_TOKEN is required')
    BridgeBot().run(token, log_handler=None)
