require('dotenv').config();
const WebSocket = require('ws');
const https = require('https');
const http = require('http');

// ============================================================
// CONFIGURATION
// ============================================================

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const ACTIVEPIECES_WEBHOOK_URL = process.env.ACTIVEPIECES_WEBHOOK_URL;
const PORT = process.env.PORT || 80;

if (!DISCORD_TOKEN) {
  console.error('ERROR: DISCORD_TOKEN is missing');
  process.exit(1);
}

if (!ACTIVEPIECES_WEBHOOK_URL) {
  console.error('ERROR: ACTIVEPIECES_WEBHOOK_URL is missing');
  process.exit(1);
}

// ============================================================
// WEB SERVER - required by Railway for health checks on port 80
// ============================================================

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      service: 'discord-receipt-bot',
      timestamp: new Date().toISOString(),
      discord: ws && ws.readyState === WebSocket.OPEN ? 'connected' : 'connecting',
      uptime: process.uptime()
    }));
    return;
  }

  if (req.url === '/status') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'running',
      discord_connected: ws && ws.readyState === WebSocket.OPEN,
      reconnect_attempts: reconnectAttempts,
      uptime_seconds: Math.floor(process.uptime()),
      memory_mb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024)
    }));
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not found' }));
});

server.listen(PORT, '0.0.0.0', () => {
  log(`Web server running on port ${PORT}`);
  log(`Health check: http://localhost:${PORT}/health`);
});

server.on('error', (err) => {
  log(`Web server error: ${err.message}`);
  if (err.code === 'EADDRINUSE') {
    log(`Port ${PORT} already in use`);
    process.exit(1);
  }
});

// ============================================================
// LOGGING
// ============================================================

function log(message) {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

// ============================================================
// SEND TO ACTIVEPIECES
// ============================================================

function sendToActivepieces(data) {
  const payload = JSON.stringify(data);

  try {
    const url = new URL(ACTIVEPIECES_WEBHOOK_URL);
    const isHttps = url.protocol === 'https:';
    const requester = isHttps ? https : http;

    const options = {
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: url.pathname + url.search,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload)
      }
    };

    const req = requester.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        log(`Activepieces responded: ${res.statusCode}`);
        if (res.statusCode !== 200) {
          log(`Response body: ${body}`);
        }
      });
    });

    req.on('error', err => log(`Send error: ${err.message}`));
    req.setTimeout(10000, () => {
      log('Request timed out');
      req.destroy();
    });

    req.write(payload);
    req.end();

  } catch (err) {
    log(`Failed to send: ${err.message}`);
  }
}

// ============================================================
// DISCORD GATEWAY
// ============================================================

function getGatewayUrl() {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'discord.com',
      path: '/api/v10/gateway/bot',
      method: 'GET',
      headers: {
        'Authorization': `Bot ${DISCORD_TOKEN}`,
        'Content-Type': 'application/json'
      }
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.url) resolve(parsed.url);
          else reject(new Error(`No gateway URL: ${data}`));
        } catch (e) {
          reject(new Error(`Parse error: ${data}`));
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(10000, () => { req.destroy(); });
    req.end();
  });
}

// ============================================================
// DISCORD WEBSOCKET STATE
// ============================================================

let ws;
let heartbeatInterval;
let sequence = null;
let sessionId = null;
let resumeGatewayUrl = null;
let reconnectAttempts = 0;
let lastHeartbeatAck = true;

// ============================================================
// HEARTBEAT
// ============================================================

function startHeartbeat(interval) {
  if (heartbeatInterval) clearInterval(heartbeatInterval);

  heartbeatInterval = setInterval(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;

    if (!lastHeartbeatAck) {
      log('No heartbeat ACK - connection dead, reconnecting...');
      ws.terminate();
      return;
    }

    lastHeartbeatAck = false;
    ws.send(JSON.stringify({ op: 1, d: sequence }));
  }, interval);
}

// ============================================================
// IDENTIFY AND RESUME
// ============================================================

function identify() {
  log('Identifying with Discord...');
  ws.send(JSON.stringify({
    op: 2,
    d: {
      token: DISCORD_TOKEN,
      intents: 33281,
      properties: {
        os: 'linux',
        browser: 'receipt-bot',
        device: 'receipt-bot'
      }
    }
  }));
}

function resume() {
  log('Resuming Discord session...');
  ws.send(JSON.stringify({
    op: 6,
    d: {
      token: DISCORD_TOKEN,
      session_id: sessionId,
      seq: sequence
    }
  }));
}

// ============================================================
// MAIN CONNECTION
// ============================================================

async function connect(isResume = false) {
  try {
    let gatewayUrl;

    if (isResume && resumeGatewayUrl) {
      gatewayUrl = resumeGatewayUrl;
    } else {
      log('Getting Discord gateway URL...');
      gatewayUrl = await getGatewayUrl();
    }

    const wsUrl = `${gatewayUrl}?v=10&encoding=json`;
    log(`Connecting to Discord: ${wsUrl}`);

    ws = new WebSocket(wsUrl);

    ws.on('open', () => {
      log('Discord WebSocket opened');
      reconnectAttempts = 0;
      lastHeartbeatAck = true;
    });

    ws.on('message', (data) => {
      try {
        const payload = JSON.parse(data.toString());

        if (payload.s !== null && payload.s !== undefined) {
          sequence = payload.s;
        }

        switch (payload.op) {
          case 10:
            log(`Hello received - heartbeat every ${payload.d.heartbeat_interval}ms`);
            startHeartbeat(payload.d.heartbeat_interval);
            if (isResume && sessionId) resume();
            else identify();
            break;

          case 11:
            lastHeartbeatAck = true;
            break;

          case 9:
            log('Session invalid - reconnecting');
            sessionId = null;
            sequence = null;
            setTimeout(() => connect(payload.d), payload.d ? 1000 : 5000);
            break;

          case 7:
            log('Discord requested reconnect');
            if (ws) ws.close(4000);
            setTimeout(() => connect(true), 500);
            break;

          case 0:
            handleEvent(payload);
            break;
        }

      } catch (err) {
        log(`Message error: ${err.message}`);
      }
    });

    ws.on('close', (code) => {
      log(`WebSocket closed: ${code}`);
      if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
        heartbeatInterval = null;
      }

      const fatal = [4004, 4010, 4011, 4012, 4013, 4014];
      if (fatal.includes(code)) {
        log(`Fatal code ${code} - check DISCORD_TOKEN`);
        process.exit(1);
      }

      reconnectAttempts++;
      const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
      log(`Reconnecting in ${delay}ms (attempt ${reconnectAttempts})`);
      setTimeout(() => connect(!!sessionId && code !== 1000), delay);
    });

    ws.on('error', (err) => {
      log(`WebSocket error: ${err.message}`);
    });

  } catch (err) {
    log(`Connection failed: ${err.message}`);
    reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
    setTimeout(() => connect(false), delay);
  }
}

// ============================================================
// EVENT HANDLER
// ============================================================

function handleEvent(payload) {
  switch (payload.t) {
    case 'READY':
      sessionId = payload.d.session_id;
      resumeGatewayUrl = payload.d.resume_gateway_url;
      log(`Bot READY: ${payload.d.user.username}`);
      break;

    case 'RESUMED':
      log('Session resumed');
      break;

    case 'MESSAGE_CREATE':
      handleMessage(payload.d);
      break;
  }
}

// ============================================================
// MESSAGE HANDLER
// ============================================================

function handleMessage(message) {
  if (message.author && message.author.bot) return;
  if (!message.content && (!message.attachments || message.attachments.length === 0)) return;

  log(`Message from ${message.author.username}: ${message.content || '[attachment]'}`);

  const images = (message.attachments || []).filter(att => {
    const name = (att.filename || '').toLowerCase();
    return name.endsWith('.jpg') ||
           name.endsWith('.jpeg') ||
           name.endsWith('.png') ||
           name.endsWith('.webp') ||
           name.endsWith('.gif') ||
           name.endsWith('.bmp') ||
           (att.content_type && att.content_type.startsWith('image/'));
  });

  const data = {
    messageId: message.id,
    content: message.content || '',
    authorId: message.author.id,
    authorUsername: message.author.username,
    authorGlobalName: message.author.global_name || message.author.username,
    channelId: message.channel_id,
    guildId: message.guild_id || null,
    timestamp: message.timestamp,
    hasAttachment: images.length > 0,
    attachmentUrl: images.length > 0 ? images[0].url : null,
    attachmentFilename: images.length > 0 ? images[0].filename : null,
    attachmentContentType: images.length > 0 ? (images[0].content_type || 'image/jpeg') : null,
    attachmentSize: images.length > 0 ? images[0].size : null,
    allAttachments: images.map(a => ({
      url: a.url,
      filename: a.filename,
      size: a.size,
      content_type: a.content_type || 'image/jpeg'
    }))
  };

  log(`Forwarding - hasAttachment: ${data.hasAttachment}`);
  if (data.hasAttachment) log(`File: ${data.attachmentFilename}`);

  sendToActivepieces(data);
}

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

process.on('SIGTERM', () => {
  log('SIGTERM - shutting down');
  if (ws) ws.close(1000);
  server.close(() => process.exit(0));
});

process.on('SIGINT', () => {
  log('SIGINT - shutting down');
  if (ws) ws.close(1000);
  server.close(() => process.exit(0));
});

process.on('uncaughtException', (err) => {
  log(`Uncaught exception: ${err.message}`);
});

process.on('unhandledRejection', (reason) => {
  log(`Unhandled rejection: ${reason}`);
});

// ============================================================
// START
// ============================================================

log('='.repeat(50));
log('Discord Receipt Bot Starting');
log('='.repeat(50));
log(`Port: ${PORT}`);
log(`Token preview: ${DISCORD_TOKEN.substring(0, 15)}...`);
log(`Webhook preview: ${ACTIVEPIECES_WEBHOOK_URL.substring(0, 60)}...`);
log('='.repeat(50));

connect(false);
