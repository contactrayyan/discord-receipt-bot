require('dotenv').config();
const WebSocket = require('ws');
const https = require('https');
const http = require('http');

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const ACTIVEPIECES_WEBHOOK_URL = process.env.ACTIVEPIECES_WEBHOOK_URL;

if (!DISCORD_TOKEN) {
  console.error('ERROR: DISCORD_TOKEN is missing');
  process.exit(1);
}

if (!ACTIVEPIECES_WEBHOOK_URL) {
  console.error('ERROR: ACTIVEPIECES_WEBHOOK_URL is missing');
  process.exit(1);
}

const DISCORD_API = 'https://discord.com/api/v10';
const INTENTS = 33281; // GUILDS + GUILD_MESSAGES + MESSAGE_CONTENT

let ws = null;
let heartbeatInterval = null;
let reconnectTimer = null;
let sequence = null;
let sessionId = null;
let resumeGatewayUrl = null;
let reconnectAttempts = 0;
let shuttingDown = false;

function log(message) {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT EXCEPTION:', err);
});

process.on('unhandledRejection', (err) => {
  console.error('UNHANDLED REJECTION:', err);
});

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  log('Shutting down...');
  clearReconnectTimer();
  stopHeartbeat();

  try {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.close(1000, 'Shutdown');
    }
  } catch (_) {}

  setTimeout(() => process.exit(0), 1000);
}

function clearReconnectTimer() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
}

function stopHeartbeat() {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
    heartbeatInterval = null;
  }
}

function scheduleReconnect(useResume = true) {
  if (shuttingDown) return;
  clearReconnectTimer();
  reconnectAttempts += 1;
  const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
  log(`Reconnecting in ${delay}ms (attempt ${reconnectAttempts})`);
  reconnectTimer = setTimeout(() => connect(useResume), delay);
}

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
        'Content-Length': Buffer.byteLength(payload),
      },
    };

    const req = requester.request(options, (res) => {
      let responseData = '';
      res.on('data', (chunk) => (responseData += chunk));
      res.on('end', () => {
        log(`Activepieces responded: ${res.statusCode}`);
      });
    });

    req.on('error', (error) => {
      log(`Error sending to Activepieces: ${error.message}`);
    });

    req.write(payload);
    req.end();
  } catch (error) {
    log(`Failed to send to Activepieces: ${error.message}`);
  }
}

function getGatewayUrl() {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'discord.com',
      path: '/api/v10/gateway/bot',
      method: 'GET',
      headers: {
        Authorization: `Bot ${DISCORD_TOKEN}`,
        'Content-Type': 'application/json',
      },
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        if (res.statusCode !== 200) {
          return reject(new Error(`Gateway HTTP ${res.statusCode}: ${data}`));
        }

        try {
          const parsed = JSON.parse(data);
          if (parsed && parsed.url) {
            resolve(parsed.url);
          } else {
            reject(new Error(`Gateway response missing url: ${data}`));
          }
        } catch (e) {
          reject(new Error(`Gateway parse error: ${data}`));
        }
      });
    });

    req.on('error', reject);
    req.end();
  });
}

function startHeartbeat(interval) {
  stopHeartbeat();

  heartbeatInterval = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ op: 1, d: sequence }));
    }
  }, interval);
}

function identify() {
  log('Identifying with Discord...');
  ws.send(
    JSON.stringify({
      op: 2,
      d: {
        token: DISCORD_TOKEN,
        intents: INTENTS,
        properties: {
          os: 'linux',
          browser: 'receipt-bot',
          device: 'receipt-bot',
        },
      },
    })
  );
}

function resume() {
  log('Resuming Discord session...');
  ws.send(
    JSON.stringify({
      op: 6,
      d: {
        token: DISCORD_TOKEN,
        session_id: sessionId,
        seq: sequence,
      },
    })
  );
}

function handleDispatch(payload) {
  const eventType = payload.t;
  const eventData = payload.d;

  switch (eventType) {
    case 'READY':
      sessionId = eventData.session_id;
      resumeGatewayUrl = eventData.resume_gateway_url || null;
      log(`Bot ready! Logged in as: ${eventData.user.username}`);
      break;

    case 'RESUMED':
      log('Session resumed successfully');
      break;

    case 'MESSAGE_CREATE':
      handleMessage(eventData);
      break;
  }
}

function handleMessage(message) {
  if (message.author && message.author.bot) return;
  if (!message.content && (!message.attachments || message.attachments.length === 0)) return;

  log(`New message from ${message.author.username}: ${message.content || '[attachment]'}`);

  const imageAttachments = message.attachments
    ? message.attachments.filter((att) => {
        const filename = (att.filename || '').toLowerCase();
        return (
          filename.endsWith('.jpg') ||
          filename.endsWith('.jpeg') ||
          filename.endsWith('.png') ||
          filename.endsWith('.webp') ||
          filename.endsWith('.gif') ||
          (att.content_type && att.content_type.startsWith('image/'))
        );
      })
    : [];

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
    attachmentContentType: imageAttachments.length > 0 ? imageAttachments[0].content_type : null,
    allAttachments: imageAttachments.map((att) => ({
      url: att.url,
      filename: att.filename,
      size: att.size,
      content_type: att.content_type,
    })),
  };

  log(`Sending to Activepieces: hasAttachment=${dataToSend.hasAttachment}`);
  sendToActivepieces(dataToSend);
}

async function connect(useResume = false) {
  if (shuttingDown) return;

  clearReconnectTimer();

  try {
    let gatewayUrl;

    if (useResume && resumeGatewayUrl && sessionId) {
      gatewayUrl = resumeGatewayUrl;
    } else {
      log('Getting Discord gateway URL...');
      gatewayUrl = await getGatewayUrl();
    }

    const wsUrl = `${gatewayUrl}?v=10&encoding=json`;
    log(`Connecting to Discord gateway: ${wsUrl}`);

    if (ws) {
      try {
        ws.removeAllListeners();
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
          ws.close();
        }
      } catch (_) {}
    }

    ws = new WebSocket(wsUrl);

    ws.on('open', () => {
      log('WebSocket connection opened');
      reconnectAttempts = 0;
    });

    ws.on('message', (data) => {
      try {
        const payload = JSON.parse(data.toString());

        if (typeof payload.s === 'number') {
          sequence = payload.s;
        }

        switch (payload.op) {
          case 10:
            log('Received Hello from Discord');
            startHeartbeat(payload.d.heartbeat_interval);

            if (useResume && sessionId) {
              resume();
            } else {
              identify();
            }
            break;

          case 11:
            break;

          case 7:
            log('Discord requested reconnect');
            try {
              ws.close();
            } catch (_) {}
            break;

          case 9:
            log('Session invalidated, reconnecting fresh...');
            sessionId = null;
            sequence = null;
            resumeGatewayUrl = null;
            stopHeartbeat();
            scheduleReconnect(false);
            break;

          case 0:
            handleDispatch(payload);
            break;
        }
      } catch (error) {
        log(`Error parsing message: ${error.message}`);
      }
    });

    ws.on('close', (code, reason) => {
      const reasonText = reason ? reason.toString() : '';
      log(`WebSocket closed. Code: ${code}, Reason: ${reasonText}`);

      stopHeartbeat();

      if (shuttingDown) return;

      const fatalCodes = [4004, 4010, 4011, 4012, 4013, 4014];
      if (fatalCodes.includes(code)) {
        log(`Fatal Discord close code ${code}. Check token, intents, or permissions.`);
        process.exit(1);
        return;
      }

      scheduleReconnect(code === 4003 ? false : true);
    });

    ws.on('error', (error) => {
      log(`WebSocket error: ${error.message}`);
    });
  } catch (error) {
    log(`Connection error: ${error.message}`);
    scheduleReconnect(false);
  }
}

log('Starting Discord Receipt Bot...');
log(`Token starts with: ${DISCORD_TOKEN.substring(0, 10)}...`);
log(`Webhook URL: ${ACTIVEPIECES_WEBHOOK_URL.substring(0, 50)}...`);
connect(false);
