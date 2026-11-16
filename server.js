const express = require('express');
const http = require('http');
const path = require('path');
const { spawn } = require('child_process');
const { Readable } = require('stream');
const crypto = require('crypto');
const cors = require('cors');
const yts = require('yt-search');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const HEARTBEAT_INTERVAL_MS = 5000;

const PIPED_INSTANCES = [
  'https://pipedapi.kavin.rocks',
  'https://pipedapi.tokhmi.xyz',
  'https://pipedapi.moomoo.me',
  'https://piped-api.garudalinux.org'
];

const INVIDIOUS_INSTANCES = [
  'https://inv.nadeko.net',
  'https://invidious.nerdvpn.de',
  'https://yt.artemislena.eu',
  'https://invidious.privacyredirect.com'
];

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Serve an inline SVG icon for PWA installability without external assets
app.get('/icon.svg', (req, res) => {
  res.setHeader('Content-Type', 'image/svg+xml');
  res.setHeader('Cache-Control', 'public, max-age=86400');
  res.send(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
    <rect width="512" height="512" rx="112" fill="#0f0f0f"/>
    <circle cx="256" cy="256" r="180" fill="#ff0033"/>
    <polygon points="212,172 348,256 212,340" fill="#ffffff"/>
  </svg>`);
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

/**
 * In-memory room store.
 * Key: roomCode (uppercase string)
 */
const rooms = new Map();

function sanitizeRoomCode(code) {
  return String(code || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9_-]/g, '')
    .slice(0, 24);
}

function sanitizeDisplayName(name) {
  const cleaned = String(name || '').trim().slice(0, 30);
  return cleaned || 'Listener';
}

function isValidVideoId(id) {
  return typeof id === 'string' && /^[a-zA-Z0-9_-]{11}$/.test(id);
}

function getOrCreateRoom(roomCode) {
  if (!rooms.has(roomCode)) {
    rooms.set(roomCode, {
      id: roomCode,
      currentTrack: null,
      queue: [],
      isPlaying: false,
      position: 0,
      lastUpdated: Date.now(),
      users: new Map(),
      messages: []
    });
  }
  return rooms.get(roomCode);
}

function getLivePosition(room) {
  if (!room.currentTrack) return 0;
  let pos = room.position;
  if (room.isPlaying) {
    pos += (Date.now() - room.lastUpdated) / 1000;
  }
  if (room.currentTrack.seconds && room.currentTrack.seconds > 0) {
    pos = Math.min(pos, room.currentTrack.seconds);
  }
  return Math.max(0, pos);
}

function buildTrackPayload(track) {
  return {
    instanceId: track.instanceId || crypto.randomUUID(),
    videoId: track.videoId,
    title: String(track.title || 'Unknown Title').slice(0, 200),
    artist: String(track.artist || 'Unknown Artist').slice(0, 120),
    duration: String(track.duration || '0:00').slice(0, 20),
    seconds: Number(track.seconds) || 0,
    thumbnail: String(track.thumbnail || '')
  };
}

function serializeRoomState(room) {
  return {
    roomId: room.id,
    currentTrack: room.currentTrack,
    queue: room.queue,
    isPlaying: room.isPlaying,
    position: getLivePosition(room),
    serverTime: Date.now(),
    users: Array.from(room.users.values()),
    messages: room.messages
  };
}

function advanceQueue(room) {
  if (room.queue.length > 0) {
    const next = room.queue.shift();
    room.currentTrack = buildTrackPayload(next);
    room.isPlaying = true;
    room.position = 0;
    room.lastUpdated = Date.now();
  } else {
    room.currentTrack = null;
    room.isPlaying = false;
    room.position = 0;
    room.lastUpdated = Date.now();
  }
  io.to(room.id).emit('room-state', serializeRoomState(room));
}

// YouTube Music Search Endpoint
app.get('/api/search', async (req, res) => {
  const q = String(req.query.q || '').trim();
  if (!q) {
    return res.status(400).json({ error: 'Missing query parameter q' });
  }

  try {
    const searchQuery = q.toLowerCase().includes('music') || q.toLowerCase().includes('audio')
      ? q
      : `${q} audio`;
    const result = await yts(searchQuery);
    const videos = (result.videos || [])
      .filter((v) => isValidVideoId(v.videoId) && v.seconds > 0)
      .slice(0, 20)
      .map((v) => ({
        videoId: v.videoId,
        title: v.title,
        artist: (v.author && v.author.name) || 'Unknown Artist',
        duration: v.timestamp || '0:00',
        seconds: v.seconds || 0,
        thumbnail: v.thumbnail || v.image || `https://i.ytimg.com/vi/${v.videoId}/hqdefault.jpg`
      }));

    res.json({ results: videos });
  } catch (err) {
    console.error('[Search Error]', err.message);
    res.status(500).json({ error: 'Failed to search YouTube Music catalog' });
  }
});

async function fetchFallbackAudioUrl(videoId, externalSignal) {
  // Try Piped public instances first
  for (const baseUrl of PIPED_INSTANCES) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 4500);
    const onExternalAbort = () => controller.abort();
    if (externalSignal) externalSignal.addEventListener('abort', onExternalAbort, { once: true });

    try {
      const resp = await fetch(`${baseUrl}/streams/${videoId}`, {
        signal: controller.signal,
        headers: { 'User-Agent': 'ListenTogether/1.0' }
      });
      if (!resp.ok) continue;
      const data = await resp.json();
      const audioStreams = (data.audioStreams || []).filter((s) => s.url);
      if (audioStreams.length === 0) continue;

      const mp4Streams = audioStreams.filter((s) =>
        (s.mimeType && s.mimeType.includes('audio/mp4')) || s.format === 'M4A'
      );
      const pool = mp4Streams.length > 0 ? mp4Streams : audioStreams;
      pool.sort((a, b) => (b.bitrate || 0) - (a.bitrate || 0));
      return {
        url: pool[0].url,
        contentType: pool[0].mimeType ? pool[0].mimeType.split(';')[0] : 'audio/mp4'
      };
    } catch (_) {
      // Try next instance
    } finally {
      clearTimeout(timeout);
      if (externalSignal) externalSignal.removeEventListener('abort', onExternalAbort);
    }
  }

  // Try Invidious public instances as secondary fallback
  for (const baseUrl of INVIDIOUS_INSTANCES) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 4500);
    const onExternalAbort = () => controller.abort();
    if (externalSignal) externalSignal.addEventListener('abort', onExternalAbort, { once: true });

    try {
      const resp = await fetch(`${baseUrl}/api/v1/videos/${videoId}`, {
        signal: controller.signal,
        headers: { 'User-Agent': 'ListenTogether/1.0' }
      });
      if (!resp.ok) continue;
      const data = await resp.json();
      const formats = (data.adaptiveFormats || []).filter(
        (f) => f.type && f.type.startsWith('audio/') && f.url
      );
      if (formats.length === 0) continue;

      const mp4Formats = formats.filter((f) => f.type.includes('audio/mp4'));
      const pool = mp4Formats.length > 0 ? mp4Formats : formats;
      pool.sort((a, b) => (Number(b.bitrate) || 0) - (Number(a.bitrate) || 0));
      return {
        url: pool[0].url,
        contentType: pool[0].type.split(';')[0]
      };
    } catch (_) {
      // Try next instance
    } finally {
      clearTimeout(timeout);
      if (externalSignal) externalSignal.removeEventListener('abort', onExternalAbort);
    }
  }

  return null;
}

async function streamFromFallback(videoId, req, res) {
  const abortController = new AbortController();
  const onClose = () => abortController.abort();
  req.on('close', onClose);

  try {
    const resolved = await fetchFallbackAudioUrl(videoId, abortController.signal);
    if (!resolved) {
      if (!res.headersSent) {
        res.status(502).json({ error: 'All stream sources failed' });
      }
      return;
    }

    const upstreamHeaders = {
      'User-Agent': 'Mozilla/5.0 (compatible; ListenTogether/1.0)'
    };
    if (req.headers.range) {
      upstreamHeaders.Range = req.headers.range;
    }

    const upstream = await fetch(resolved.url, {
      headers: upstreamHeaders,
      signal: abortController.signal
    });

    if (!upstream.ok && upstream.status !== 206) {
      if (!res.headersSent) {
        res.status(502).json({ error: 'Fallback stream returned an error' });
      }
      return;
    }

    res.status(upstream.status);
    res.setHeader('Content-Type', resolved.contentType || 'audio/mp4');
    res.setHeader('Cache-Control', 'no-store');
    const contentLength = upstream.headers.get('content-length');
    const contentRange = upstream.headers.get('content-range');
    const acceptRanges = upstream.headers.get('accept-ranges');
    if (contentLength) res.setHeader('Content-Length', contentLength);
    if (contentRange) res.setHeader('Content-Range', contentRange);
    if (acceptRanges) res.setHeader('Accept-Ranges', acceptRanges);

    if (upstream.body) {
      const nodeStream = Readable.fromWeb(upstream.body);
      nodeStream.on('error', () => {
        if (!res.writableEnded) res.end();
      });
      nodeStream.pipe(res);
    } else {
      res.end();
    }
  } catch (err) {
    if (err.name !== 'AbortError' && !res.headersSent) {
      res.status(500).json({ error: 'Fallback proxy error' });
    }
  } finally {
    req.off('close', onClose);
  }
}

// iOS & Android Audio Proxy Endpoint
app.get('/api/stream/:videoId', (req, res) => {
  const { videoId } = req.params;
  if (!isValidVideoId(videoId)) {
    return res.status(400).json({ error: 'Invalid YouTube videoId' });
  }

  const youtubeUrl = `https://www.youtube.com/watch?v=${videoId}`;
  const ytdlp = spawn(
    'yt-dlp',
    ['-f', 'bestaudio[ext=m4a]/bestaudio', '-o', '-', '--no-playlist', '--no-warnings', youtubeUrl],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );

  let clientClosed = false;
  let startedStreaming = false;
  let fallbackTriggered = false;
  let stderrBuffer = '';

  const cleanupProcess = () => {
    if (!ytdlp.killed) {
      ytdlp.kill('SIGKILL');
    }
  };

  req.on('close', () => {
    clientClosed = true;
    cleanupProcess();
  });

  ytdlp.stderr.on('data', (chunk) => {
    stderrBuffer += chunk.toString();
    if (stderrBuffer.length > 4096) {
      stderrBuffer = stderrBuffer.slice(-4096);
    }
  });

  ytdlp.stdout.on('data', (chunk) => {
    if (clientClosed || fallbackTriggered) return;

    if (!startedStreaming) {
      startedStreaming = true;
      res.status(200);
      res.setHeader('Content-Type', 'audio/mp4');
      res.setHeader('Accept-Ranges', 'none');
      res.setHeader('Cache-Control', 'no-store');
    }
    if (!res.write(chunk)) {
      ytdlp.stdout.pause();
      res.once('drain', () => {
        if (!clientClosed && !ytdlp.killed) {
          ytdlp.stdout.resume();
        }
      });
    }
  });

  ytdlp.on('error', async (err) => {
    console.error('[yt-dlp spawn error]', err.message);
    if (!startedStreaming && !clientClosed && !fallbackTriggered) {
      fallbackTriggered = true;
      await streamFromFallback(videoId, req, res);
    } else if (!res.writableEnded) {
      res.end();
    }
  });

  ytdlp.on('close', async (code) => {
    if (clientClosed) return;

    if (!startedStreaming && !fallbackTriggered) {
      const isBotCheck = /bot|sign in|403|unavailable|confirm/i.test(stderrBuffer);
      console.warn(`[yt-dlp exited ${code}] Bot check / stream failure for ${videoId}. Triggering fallback. Details: ${stderrBuffer.trim()}`);
      fallbackTriggered = true;
      if (isBotCheck || code !== 0) {
        await streamFromFallback(videoId, req, res);
      } else if (!res.headersSent) {
        res.status(502).json({ error: 'Empty audio stream' });
      }
      return;
    }

    if (!res.writableEnded) {
      res.end();
    }
  });
});

// Socket.IO Real-Time Sync & Chat
io.on('connection', (socket) => {
  let currentRoomId = null;

  socket.on('join-room', ({ roomCode, displayName }) => {
    const cleanRoom = sanitizeRoomCode(roomCode);
    if (!cleanRoom) {
      socket.emit('error-toast', { message: 'Please enter a valid room code.' });
      return;
    }

    // Leave any previous room
    if (currentRoomId && rooms.has(currentRoomId)) {
      const prevRoom = rooms.get(currentRoomId);
      prevRoom.users.delete(socket.id);
      socket.leave(currentRoomId);
      io.to(currentRoomId).emit('users-updated', Array.from(prevRoom.users.values()));
    }

    currentRoomId = cleanRoom;
    const room = getOrCreateRoom(cleanRoom);
    const user = {
      id: socket.id,
      name: sanitizeDisplayName(displayName)
    };

    room.users.set(socket.id, user);
    socket.join(cleanRoom);

    // Send exact live state including recalculated position
    socket.emit('room-state', serializeRoomState(room));
    io.to(cleanRoom).emit('users-updated', Array.from(room.users.values()));
  });

  socket.on('play-track', ({ track }) => {
    if (!currentRoomId || !rooms.has(currentRoomId)) return;
    if (!track || !isValidVideoId(track.videoId)) return;

    const room = rooms.get(currentRoomId);
    room.currentTrack = buildTrackPayload(track);
    room.isPlaying = true;
    room.position = 0;
    room.lastUpdated = Date.now();

    io.to(room.id).emit('room-state', serializeRoomState(room));
  });

  socket.on('add-to-queue', ({ track }) => {
    if (!currentRoomId || !rooms.has(currentRoomId)) return;
    if (!track || !isValidVideoId(track.videoId)) return;

    const room = rooms.get(currentRoomId);
    const payload = buildTrackPayload(track);

    if (!room.currentTrack) {
      room.currentTrack = payload;
      room.isPlaying = true;
      room.position = 0;
      room.lastUpdated = Date.now();
    } else {
      if (room.queue.length < 100) {
        room.queue.push(payload);
      }
    }

    io.to(room.id).emit('room-state', serializeRoomState(room));
  });

  socket.on('remove-from-queue', ({ instanceId }) => {
    if (!currentRoomId || !rooms.has(currentRoomId)) return;
    const room = rooms.get(currentRoomId);
    room.queue = room.queue.filter((item) => item.instanceId !== instanceId);
    io.to(room.id).emit('room-state', serializeRoomState(room));
  });

  socket.on('next-track', (payload = {}) => {
    if (!currentRoomId || !rooms.has(currentRoomId)) return;
    const room = rooms.get(currentRoomId);
    if (!room.currentTrack) return;

    // Deduplicate simultaneous "ended" triggers from multiple clients
    if (
      payload &&
      payload.expectedInstanceId &&
      room.currentTrack.instanceId !== payload.expectedInstanceId
    ) {
      return;
    }

    advanceQueue(room);
  });

  socket.on('sync-action', ({ action, position }) => {
    if (!currentRoomId || !rooms.has(currentRoomId)) return;
    const room = rooms.get(currentRoomId);
    if (!room.currentTrack) return;

    const now = Date.now();
    const livePos = getLivePosition(room);

    if (action === 'play') {
      room.position = typeof position === 'number' && position >= 0 ? position : livePos;
      room.isPlaying = true;
      room.lastUpdated = now;
    } else if (action === 'pause') {
      room.position = typeof position === 'number' && position >= 0 ? position : livePos;
      room.isPlaying = false;
      room.lastUpdated = now;
    } else if (action === 'seek') {
      const targetPos = Math.max(0, Number(position) || 0);
      room.position = room.currentTrack.seconds > 0
        ? Math.min(targetPos, room.currentTrack.seconds)
        : targetPos;
      room.lastUpdated = now;
    } else {
      return;
    }

    io.to(room.id).emit('sync-update', {
      isPlaying: room.isPlaying,
      position: getLivePosition(room),
      serverTime: now,
      action
    });
  });

  socket.on('heartbeat-sync', () => {
    if (!currentRoomId || !rooms.has(currentRoomId)) return;
    const room = rooms.get(currentRoomId);
    socket.emit('heartbeat-sync', {
      currentTrack: room.currentTrack,
      isPlaying: room.isPlaying,
      position: getLivePosition(room),
      serverTime: Date.now()
    });
  });

  socket.on('send-chat', ({ text }) => {
    if (!currentRoomId || !rooms.has(currentRoomId)) return;
    const room = rooms.get(currentRoomId);
    const user = room.users.get(socket.id);
    const cleanText = String(text || '').trim().slice(0, 500);
    if (!cleanText || !user) return;

    const message = {
      id: crypto.randomUUID(),
      userId: socket.id,
      sender: user.name,
      text: cleanText,
      timestamp: Date.now()
    };

    room.messages.push(message);
    if (room.messages.length > 100) {
      room.messages.shift();
    }

    io.to(room.id).emit('chat-message', message);
  });

  socket.on('disconnect', () => {
    if (currentRoomId && rooms.has(currentRoomId)) {
      const room = rooms.get(currentRoomId);
      room.users.delete(socket.id);
      io.to(currentRoomId).emit('users-updated', Array.from(room.users.values()));

      if (room.users.size === 0 && !room.isPlaying && room.queue.length === 0) {
        rooms.delete(currentRoomId);
      }
    }
  });
});

// Server-side 5-second heartbeat loop to keep all clients within tight sync
setInterval(() => {
  const now = Date.now();
  for (const [roomId, room] of rooms.entries()) {
    if (room.users.size === 0) continue;

    if (room.currentTrack) {
      const livePos = getLivePosition(room);
      if (
        room.isPlaying &&
        room.currentTrack.seconds > 0 &&
        livePos >= room.currentTrack.seconds + 1.5
      ) {
        advanceQueue(room);
        continue;
      }

      io.to(roomId).emit('heartbeat-sync', {
        currentTrack: room.currentTrack,
        isPlaying: room.isPlaying,
        position: livePos,
        serverTime: now
      });
    }
  }
}, HEARTBEAT_INTERVAL_MS);

server.listen(PORT, () => {
  console.log(`ListenTogether server running on http://localhost:${PORT}`);
});
