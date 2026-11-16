const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Innertube, UniversalCache } = require('youtubei.js');
const { spawn } = require('child_process');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

let yt = null;
async function initInnerTube() {
  try {
    yt = await Innertube.create({
      cache: new UniversalCache(false),
      generate_session_locally: true
    });
    console.log('InnerTube (YT Music) initialized');
  } catch (e) {
    console.error('InnerTube init failed:', e.message);
  }
}
initInnerTube();

const rooms = {};

function getOrCreateRoom(roomId) {
  if (!rooms[roomId]) {
    rooms[roomId] = {
      currentTrack: null,
      queue: [],
      isPlaying: false,
      position: 0,
      lastUpdated: Date.now(),
      users: {},
      messages: []
    };
  }
  return rooms[roomId];
}

function getExactPosition(room) {
  if (!room.isPlaying) return room.position;
  return room.position + (Date.now() - room.lastUpdated) / 1000;
}

// 1. Live Search Suggestions (As you type, just like YT Music)
app.get('/api/suggestions', async (req, res) => {
  const q = req.query.q;
  if (!q) return res.json([]);
  try {
    if (yt) {
      const suggestions = await yt.music.getSearchSuggestions(q);
      const list = [];
      for (const section of suggestions) {
        if (section.contents) {
          for (const item of section.contents) {
            if (item.suggestion?.text) list.push(item.suggestion.text);
          }
        }
      }
      if (list.length > 0) return res.json(list.slice(0, 8));
    }
    // Fallback to Google/YouTube autocomplete endpoint
    const resp = await fetch(`https://suggestqueries.google.com/complete/search?client=firefox&ds=yt&q=${encodeURIComponent(q)}`);
    const data = await resp.json();
    res.json((data[1] || []).slice(0, 8));
  } catch (err) {
    res.json([]);
  }
});

// 2. YouTube Music Catalog Search (Songs & Videos)
app.get('/api/search', async (req, res) => {
  const query = req.query.q;
  if (!query) return res.json([]);
  try {
    if (!yt) await initInnerTube();
    const search = await yt.music.search(query, { type: 'song' });
    let items = search.results || [];

    // Fallback to general search if song-only shelf is empty
    if (items.length === 0) {
      const gen = await yt.search(query);
      items = gen.videos || [];
    }

    const tracks = items.slice(0, 20).map(item => {
      const id = item.id || item.video_id;
      const title = item.title?.text || item.title || 'Unknown Title';
      const artist = item.artists?.map(a => a.name).join(', ') || item.author?.name || 'YouTube Music';
      const duration = item.duration?.text || '';
      const thumbs = item.thumbnail?.contents || item.thumbnails || [];
      const thumbnail = thumbs[0]?.url || `https://i.ytimg.com/vi/${id}/hqdefault.jpg`;
      return { id, title, author: artist, duration, thumbnail };
    }).filter(t => t.id);

    res.json(tracks);
  } catch (err) {
    console.error('Search error:', err.message);
    res.status(500).json({ error: 'Search failed' });
  }
});

// 3. Fetch Lyrics from YT Music
app.get('/api/lyrics/:videoId', async (req, res) => {
  try {
    if (!yt) await initInnerTube();
    const info = await yt.music.getLyrics(req.params.videoId);
    res.json({ lyrics: info?.description?.text || 'No lyrics available for this track.' });
  } catch (e) {
    res.json({ lyrics: 'Lyrics unavailable for this track.' });
  }
});

// 4. Multi-Fallback Audio Streamer (Bypasses Render Datacenter Blocks)
const PIPED_INSTANCES = [
  'https://pipedapi.kavin.rocks',
  'https://pipedapi.tokhmi.xyz',
  'https://pipedapi.syncpundit.io',
  'https://api.piped.projectsegfau.lt'
];

async function getPipedAudioUrl(videoId) {
  for (const base of PIPED_INSTANCES) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 4000);
      const res = await fetch(`${base}/streams/${videoId}`, { signal: controller.signal });
      clearTimeout(timeout);
      if (!res.ok) continue;
      const data = await res.json();
      if (data.audioStreams && data.audioStreams.length > 0) {
        // Prefer m4a for iOS Safari compatibility
        const m4a = data.audioStreams.find(s => s.mimeType?.includes('mp4') || s.format === 'M4A');
        return (m4a || data.audioStreams[0]).url;
      }
    } catch (_) {}
  }
  return null;
}

app.get('/api/stream/:videoId', async (req, res) => {
  const { videoId } = req.params;

  // Strategy A: Try Piped API Audio Stream Proxy (Bypasses YT Datacenter IP Blocks)
  const pipedUrl = await getPipedAudioUrl(videoId);
  if (pipedUrl) {
    try {
      const headers = {};
      if (req.headers.range) headers.Range = req.headers.range;
      const upstream = await fetch(pipedUrl, { headers });

      res.status(upstream.status);
      res.setHeader('Content-Type', upstream.headers.get('content-type') || 'audio/mp4');
      if (upstream.headers.get('content-length')) {
        res.setHeader('Content-Length', upstream.headers.get('content-length'));
      }
      if (upstream.headers.get('content-range')) {
        res.setHeader('Content-Range', upstream.headers.get('content-range'));
      }
      res.setHeader('Accept-Ranges', 'bytes');

      const { Readable } = require('stream');
      return Readable.fromWeb(upstream.body).pipe(res);
    } catch (e) {
      console.error('Piped proxy failed, falling back to yt-dlp:', e.message);
    }
  }

  // Strategy B: Fallback to yt-dlp
  const url = `https://www.youtube.com/watch?v=${videoId}`;
  const ytdlp = spawn('yt-dlp', [
    '-f', 'bestaudio[ext=m4a]/bestaudio',
    '--extractor-args', 'youtube:player_client=android,web',
    '-o', '-',
    '--quiet',
    url
  ]);

  res.setHeader('Content-Type', 'audio/mp4');
  res.setHeader('Transfer-Encoding', 'chunked');
  ytdlp.stdout.pipe(res);

  ytdlp.stderr.on('data', d => console.error('yt-dlp:', d.toString()));
  req.on('close', () => ytdlp.kill());
});

// 5. Real-Time Sync & Chat
io.on('connection', (socket) => {
  let currentRoomId = null;

  socket.on('join-room', ({ roomId, username }) => {
    currentRoomId = roomId;
    socket.join(roomId);
    const room = getOrCreateRoom(roomId);
    room.users[socket.id] = username || 'Guest';

    socket.emit('room-state', {
      currentTrack: room.currentTrack,
      queue: room.queue,
      isPlaying: room.isPlaying,
      position: getExactPosition(room),
      messages: room.messages,
      users: Object.values(room.users)
    });

    io.to(roomId).emit('users-update', Object.values(room.users));
    io.to(roomId).emit('chat-message', {
      system: true,
      text: `${room.users[socket.id]} joined the room.`
    });
  });

  socket.on('play-track', ({ roomId, track }) => {
    const room = getOrCreateRoom(roomId);
    room.currentTrack = track;
    room.isPlaying = true;
    room.position = 0;
    room.lastUpdated = Date.now();
    io.to(roomId).emit('track-changed', { track, isPlaying: true, position: 0 });
  });

  socket.on('add-to-queue', ({ roomId, track }) => {
    const room = getOrCreateRoom(roomId);
    if (!room.currentTrack) {
      room.currentTrack = track;
      room.isPlaying = true;
      room.position = 0;
      room.lastUpdated = Date.now();
      io.to(roomId).emit('track-changed', { track, isPlaying: true, position: 0 });
    } else {
      room.queue.push(track);
      io.to(roomId).emit('queue-updated', room.queue);
    }
  });

  socket.on('next-track', ({ roomId }) => {
    const room = getOrCreateRoom(roomId);
    if (room.queue.length > 0) {
      const next = room.queue.shift();
      room.currentTrack = next;
      room.isPlaying = true;
      room.position = 0;
      room.lastUpdated = Date.now();
      io.to(roomId).emit('queue-updated', room.queue);
      io.to(roomId).emit('track-changed', { track: next, isPlaying: true, position: 0 });
    }
  });

  socket.on('sync-action', ({ roomId, action, position }) => {
    const room = getOrCreateRoom(roomId);
    room.position = position;
    room.lastUpdated = Date.now();
    room.isPlaying = action === 'play';
    socket.to(roomId).emit('sync-update', { action, position });
  });

  socket.on('send-chat', ({ roomId, text }) => {
    const room = getOrCreateRoom(roomId);
    const msg = {
      user: room.users[socket.id] || 'Guest',
      text,
      time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    };
    room.messages.push(msg);
    if (room.messages.length > 100) room.messages.shift();
    io.to(roomId).emit('chat-message', msg);
  });

  socket.on('disconnect', () => {
    if (currentRoomId && rooms[currentRoomId]) {
      const room = rooms[currentRoomId];
      const name = room.users[socket.id];
      delete room.users[socket.id];
      io.to(currentRoomId).emit('users-update', Object.values(room.users));
      if (name) {
        io.to(currentRoomId).emit('chat-message', {
          system: true,
          text: `${name} left the room.`
        });
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`ListenTogether v2 running on port ${PORT}`));
