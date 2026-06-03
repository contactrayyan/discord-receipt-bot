require('dotenv').config();
const WebSocket = require('ws');
const https = require('https');
const http = require('http');

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const ACTIVEPIECES_WEBHOOK_URL = process.env.ACTIVEPIECES_WEBHOOK_URL;

if (!DISCORD_TOKEN) {
  console.error('ERROR: DISCORD_TOKEN is missing from environment');
  process.exit(1);
}
if (!ACTIVEPIECES_WEBHOOK_URL) {
  console.error('ERROR: ACTIVEPIECES_WEBHOOK_URL is missing from environment');
  process.exit(1);
}

let ws;
let heartbeatInterval;
let sequence = null;
let sessionId = null;
let resumeGatewayUrl = null;
let reconnectAttempts = 0;

function log(message) {
  console.log(`[${new Date().toISOString()}] ${message}`);
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
        'Content-Length': Buffer.byteLength(payload)
      }
    };
    const req = requester.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => log(`Activepieces responded: ${res.statusCode}`));
    });
    req.on('error', err => log(`Send error: ${err.message}`));
    req.write(payload);
    req.end();
  } catch (err) {
    log(`Failed to send: ${err.message}`);
  }
}

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
    }, res => {
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
    req.end();
  });
}

function sendHeartbeat() {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ op: 1, d: sequence }));
  }
}

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
  log('Resuming session...');
  ws.send(JSON.stringify({
    op: 6,
    d: {
      token: DISCORD_TOKEN,
      session_id: sessionId,
      seq: sequence
    }
  }));
}

function startHeartbeat(interval) {
  if (heartbeatInterval) clearInterval(heartbeatInterval);
  heartbeatInterval = setInterval(sendHeartbeat, interval);
}

async function connect(isResume = false) {
  try {
    let gatewayUrl;
    if (isResume && resumeGatewayUrl) {
      gatewayUrl = resumeGatewayUrl;
    } else {
      log('Getting Discord gateway URL...');
      gatewayUrl = await getGatewayUrl();
    }

    log(`Connecting to: ${gatewayUrl}`);
    ws = new WebSocket(`${gatewayUrl}?v=10&encoding=json`);

    ws.on('open', () => {
      log('WebSocket opened');
      reconnectAttempts = 0;
    });

    ws.on('message', data => {
      try {
        const payload = JSON.parse(data.toString());
        if (payload.s) sequence = payload.s;

        switch (payload.op) {
          case 10:
            log('Hello received from Discord');
            startHeartbeat(payload.d.heartbeat_interval);
            if (isResume && sessionId) resume();
            else identify();
            break;
          case 11:
            break;
          case 9:
            log('Session invalid - fresh connect in 5s');
            sessionId = null;
            sequence = null;
            setTimeout(() => connect(false), 5000);
            break;
          case 7:
            log('Reconnect requested');
            ws.close();
            setTimeout(() => connect(true), 1000);
            break;
          case 0:
            handleEvent(payload);
            break;
        }
      } catch (err) {
        log(`Message parse error: ${err.message}`);
      }
    });

    ws.on('close', code => {
      log(`WebSocket closed: ${code}`);
      if (heartbeatInterval) clearInterval(heartbeatInterval);
      const fatal = [4004, 4010, 4011, 4012, 4013, 4014];
      if (fatal.includes(code)) {
        log(`Fatal code ${code} - check bot token`);
        process.exit(1);
      }
      reconnectAttempts++;
      const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
      log(`Reconnecting in ${delay}ms`);
      setTimeout(() => connect(true), delay);
    });

    ws.on('error', err => log(`WebSocket error: ${err.message}`));

  } catch (err) {
    log(`Connection failed: ${err.message}`);
    reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
    setTimeout(() => connect(false), delay);
  }
}

function handleEvent(payload) {
  switch (payload.t) {
    case 'READY':
      sessionId = payload.d.session_id;
      resumeGatewayUrl = payload.d.resume_gateway_url;
      log(`Bot ready: ${payload.d.user.username}`);
      break;
    case 'RESUMED':
      log('Session resumed');
      break;
    case 'MESSAGE_CREATE':
      handleMessage(payload.d);
      break;
  }
}

function handleMessage(message) {
  if (message.author?.bot) return;
  if (!message.content && !message.attachments?.length) return;

  log(`Message from ${message.author.username}: ${message.content || '[file]'}`);

  const images = (message.attachments || []).filter(att => {
    const name = att.filename.toLowerCase();
    return name.endsWith('.jpg') ||
           name.endsWith('.jpeg') ||
           name.endsWith('.png') ||
           name.endsWith('.webp') ||
           name.endsWith('.gif') ||
           att.content_type?.startsWith('image/');
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
    attachmentUrl: images[0]?.url || null,
    attachmentFilename: images[0]?.filename || null,
    attachmentContentType: images[0]?.content_type || null,
    allAttachments: images.map(a => ({
      url: a.url,
      filename: a.filename,
      size: a.size,
      content_type: a.content_type
    }))
  };

  log(`Forwarding to Activepieces - hasAttachment: ${data.hasAttachment}`);
  sendToActivepieces(data);
}

log('Starting Discord Receipt Bot...');
log(`Token preview: ${DISCORD_TOKEN.substring(0, 10)}...`);
log(`Webhook preview: ${ACTIVEPIECES_WEBHOOK_URL.substring(0, 50)}...`);
connect(false);