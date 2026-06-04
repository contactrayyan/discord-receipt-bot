require('dotenv').config();
const WebSocket = require('ws');
const https = require('https');
const http = require('http');

// ============================================================
// CONFIGURATION - reads from environment variables
// ============================================================

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const ACTIVEPIECES_WEBHOOK_URL = process.env.ACTIVEPIECES_WEBHOOK_URL;
const PORT = process.env.PORT || 80;

// Safety checks - stop immediately if required values missing
if (!DISCORD_TOKEN) {
  console.error('ERROR: DISCORD_TOKEN is missing from environment variables');
  process.exit(1);
}

if (!ACTIVEPIECES_WEBHOOK_URL) {
  console.error('ERROR: ACTIVEPIECES_WEBHOOK_URL is missing from environment variables');
  process.exit(1);
}

// ============================================================
// WEB SERVER - required by Railway for health checks on port 80
// ============================================================

const server = http.createServer((req, res) => {
  const url = req.url;
  const method = req.method;

  // Health check endpoint - Railway pings this to verify service is alive
  if (url === '/health' || url === '/' ) {
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

  // Status endpoint - shows more details
  if (url === '/status') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'running',
      discord_connected: ws && ws.readyState === WebSocket.OPEN,
      reconnect_attempts: reconnectAttempts,
      sequence: sequence,
      has_session: !!sessionId,
      uptime_seconds: Math.floor(process.uptime()),
      memory_mb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024)
    }));
    return;
  }

  // All other routes return 404
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not found' }));
});

// Start the web server on PORT 80
server.listen(PORT, '0.0.0.0', () => {
  log(`Web server running on port ${PORT}`);
  log(`Health check available at http://localhost:${PORT}/health`);
});

// Handle web server errors
server.on('error', (err) => {
  log(`Web server error: ${err.message}`);
  if (err.code === 'EADDRINUSE') {
    log(`Port ${PORT} is already in use`);
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
          log(`Activepieces response body: ${body}`);
        }
      });
    });

    req.on('error', (err) => {
      log(`Error sending to Activepieces: ${err.message}`);
    });

    req.setTimeout(10000, () => {
      log('Activepieces request timed out after 10 seconds');
      req.destroy();
    });

    req.write(payload);
    req.end();

  } catch (err) {
    log(`Failed to send to Activepieces: ${err.message}`);
  }
}

// ============================================================
// DISCORD GATEWAY - get WebSocket URL from Discord API
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
          if (parsed.url) {
            resolve(parsed.url);
          } else {
            reject(new Error(`No gateway URL in response: ${data}`));
          }
        } catch (e) {
          reject(new Error(`Failed to parse gateway response: ${data}`));
        }
      });
    });

    req.on('error', reject);

    req.setTimeout(10000, () => {
      log('Gateway URL request timed out');
      req.destroy();
    });

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
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
  }

  heartbeatInterval = setInterval(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return;
    }

    if (!lastHeartbeatAck) {
      log('No heartbeat ACK received - connection may be dead, reconnecting...');
      ws.terminate();
      return;
    }

    lastHeartbeatAck = false;
    ws.send(JSON.stringify({ op: 1, d: sequence }));
  }, interval);
}

// ============================================================
// DISCORD IDENTIFY
// ============================================================

function identify() {
  log('Sending identify payload to Discord...');
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

// ============================================================
// DISCORD RESUME
// ============================================================

function resume() {
  log('Sending resume payload to Discord...');
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
// MAIN DISCORD CONNECTION FUNCTION
// ============================================================

async function connect(isResume = false) {
  try {
    let gatewayUrl;

    if (isResume && resumeGatewayUrl) {
      gatewayUrl = resumeGatewayUrl;
      log('Using cached resume gateway URL');
    } else {
      log('Fetching fresh Discord gateway URL...');
      gatewayUrl = await getGatewayUrl();
    }

    const wsUrl = `${gatewayUrl}?v=10&encoding=json`;
    log(`Connecting to Discord WebSocket: ${wsUrl}`);

    ws = new WebSocket(wsUrl);

    ws.on('open', () => {
      log('Discord WebSocket connection opened successfully');
      reconnectAttempts = 0;
      lastHeartbeatAck = true;
    });

    ws.on('message', (data) => {
      try {
        const payload = JSON.parse(data.toString());

        // Update sequence number whenever Discord sends it
        if (payload.s !== null && payload.s !== undefined) {
          sequence = payload.s;
        }

        switch (payload.op) {

          // Opcode 10: Hello - Discord sends this first
          case 10:
            log(`Hello received - heartbeat interval: ${payload.d.heartbeat_interval}ms`);
            startHeartbeat(payload.d.heartbeat_interval);
            if (isResume && sessionId) {
              resume();
            } else {
              identify();
            }
            break;

          // Opcode 11: Heartbeat ACK - Discord confirming it received our heartbeat
          case 11:
            lastHeartbeatAck = true;
            break;

          // Opcode 9: Invalid Session - need to reconnect fresh
          case 9:
            log('Invalid session received from Discord');
            sessionId = null;
            sequence = null;
            const canResume = payload.d;
            const delay = canResume ? 1000 : 5000;
            log(`Reconnecting in ${delay}ms, canResume: ${canResume}`);
            setTimeout(() => connect(canResume), delay);
            break;

          // Opcode 7: Reconnect - Discord wants us to reconnect
          case 7:
            log('Discord requesting reconnect');
            if (ws) ws.close(4000, 'Discord requested reconnect');
            setTimeout(() => connect(true), 500);
            break;

          // Opcode 0: Dispatch - actual events like messages
          case 0:
            handleEvent(payload);
            break;

          default:
            break;
        }

      } catch (err) {
        log(`Error processing Discord message: ${err.message}`);
      }
    });

    ws.on('close', (code, reason) => {
      const reasonStr = reason ? reason.toString() : 'unknown';
      log(`Discord WebSocket closed - code: ${code}, reason: ${reasonStr}`);

      if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
        heartbeatInterval = null;
      }

      // These codes mean something is fundamentally wrong
      // Do not try to reconnect automatically
      const fatalCodes = [4004, 4010, 4011, 4012, 4013, 4014];
      if (fatalCodes.includes(code)) {
        log(`Fatal Discord error code: ${code}`);
        log('Check your DISCORD_TOKEN and bot permissions');
        log('Bot will not reconnect automatically for fatal errors');
        process.exit(1);
      }

      // For all other codes, try to reconnect
      reconnectAttempts++;
      const backoffDelay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
      log(`Scheduling reconnect attempt ${reconnectAttempts} in ${backoffDelay}ms`);

      // Try to resume session if we have one, otherwise fresh connect
      const shouldResume = !!sessionId && code !== 1000;
      setTimeout(() => connect(shouldResume), backoffDelay);
    });

    ws.on('error', (err) => {
      log(`Discord WebSocket error: ${err.message}`);
      // The close event will fire after this and handle reconnection
    });

  } catch (err) {
    log(`Failed to connect to Discord: ${err.message}`);
    reconnectAttempts++;
    const backoffDelay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
    log(`Retrying connection in ${backoffDelay}ms`);
    setTimeout(() => connect(false), backoffDelay);
  }
}

// ============================================================
// DISCORD EVENT HANDLER
// ============================================================

function handleEvent(payload) {
  const eventType = payload.t;
  const eventData = payload.d;

  switch (eventType) {

    case 'READY':
      sessionId = eventData.session_id;
      resumeGatewayUrl = eventData.resume_gateway_url;
      log(`Bot is READY - logged in as: ${eventData.user.username}#${eventData.user.discriminator || '0'}`);
      log(`Session ID: ${sessionId.substring(0, 10)}...`);
      log(`Shard: ${eventData.shard ? JSON.stringify(eventData.shard) : 'none'}`);
      break;

    case 'RESUMED':
      log('Discord session resumed successfully');
      break;

    case 'MESSAGE_CREATE':
      handleMessage(eventData);
      break;

    // Ignore all other events
    default:
      break;
  }
}

// ============================================================
// MESSAGE HANDLER
// ============================================================

function handleMessage(message) {
  // Ignore messages from bots including our own bot
  if (message.author && message.author.bot) {
    return;
  }

  // Ignore messages with no content and no attachments
  if (!message.content && (!message.attachments || message.attachments.length === 0)) {
    return;
  }

  log(`Message received from ${message.author.username} in channel ${message.channel_id}`);
  if (message.content) {
    log(`Content: ${message.content.substring(0, 100)}`);
  }

  // Filter to only image attachments
  const imageAttachments = (message.attachments || []).filter(att => {
    const filename = (att.filename || '').toLowerCase();
    const isImageExtension = filename.endsWith('.jpg') ||
                             filename.endsWith('.jpeg') ||
                             filename.endsWith('.png') ||
                             filename.endsWith('.webp') ||
                             filename.endsWith('.gif') ||
                             filename.endsWith('.bmp') ||
                             filename.endsWith('.tiff');
    const isImageContentType = att.content_type &&
                               att.content_type.startsWith('image/');
    return isImageExtension || isImageContentType;
  });

  // Build the data object to forward to Activepieces
  const dataToSend = {
    messageId: message.id,
    content: message.content || '',
    authorId: message.author.id,
    authorUsername: message.author.username,
    authorGlobalName: message.author.global_name || message.author.username,
    channelId: message.channel_id,
    guildId: message.guild_id || null,
    timestamp: message.timestamp,
    hasAttachment: imageAttachments.length > 0,
    attachmentUrl: imageAttachments.length > 0 ? imageAttachments[0].url : null,
    attachmentFilename: imageAttachments.length > 0 ? imageAttachments[0].filename : null,
    attachmentContentType: imageAttachments.length > 0 ? (imageAttachments[0].content_type || 'image/jpeg') : null,
    attachmentSize: imageAttachments.length > 0 ? imageAttachments[0].size : null,
    allAttachments: imageAttachments.map(att => ({
      url: att.url,
      filename: att.filename,
      size: att.size,
      content_type: att.content_type || 'image/jpeg',
      width: att.width || null,
      height: att.height || null
    }))
  };

  log(`Forwarding to Activepieces - hasAttachment: ${dataToSend.hasAttachment}`);
  if (dataToSend.hasAttachment) {
    log(`Attachment: ${dataToSend.attachmentFilename} (${dataToSend.attachmentContentType})`);
  }

  sendToActivepieces(dataToSend);
}

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

process.on('SIGTERM', () => {
  log('SIGTERM received - shutting down gracefully');
  if (ws) ws.close(1000, 'Service shutting down');
  server.close(() => {
    log('Web server closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  log('SIGINT received - shutting down gracefully');
  if (ws) ws.close(1000, 'Service shutting down');
  server.close(() => {
    log('Web server closed');
    process.exit(0);
  });
});

process.on('uncaughtException', (err) => {
  log(`Uncaught exception: ${err.message}`);
  log(err.stack);
  // Don't exit - keep running
});

process.on('unhandledRejection', (reason) => {
  log(`Unhandled rejection: ${reason}`);
  // Don't exit - keep running
});

// ============================================================
// START EVERYTHING
// ============================================================

log('='.repeat(50));
log('Discord Receipt Bot Starting');
log('='.repeat(50));
log(`Port: ${PORT}`);
log(`Token preview: ${DISCORD_TOKEN.substring(0, 15)}...`);
log(`Webhook preview: ${ACTIVEPIECES_WEBHOOK_URL.substring(0, 60)}...`);
log('='.repeat(50));

// Connect to Discord WebSocket
// Web server already started above at server.listen()
connect(false);
