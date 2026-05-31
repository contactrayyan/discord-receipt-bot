require('dotenv').config();
const WebSocket = require('ws');
const https = require('https');
const http = require('http');

// TOKENS LOADED FROM .env FILE AUTOMATICALLY
const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const ACTIVEPIECES_WEBHOOK_URL = process.env.ACTIVEPIECES_WEBHOOK_URL;

// Safety check - stop immediately if tokens are missing
if (!DISCORD_TOKEN) {
  console.error('ERROR: DISCORD_TOKEN is missing from .env file or environment variables');
  process.exit(1);
}

if (!ACTIVEPIECES_WEBHOOK_URL) {
  console.error('ERROR: ACTIVEPIECES_WEBHOOK_URL is missing from .env file or environment variables');
  process.exit(1);
}

// DO NOT CHANGE ANYTHING BELOW THIS LINE
const DISCORD_API = 'https://discord.com/api/v10';
const INTENTS = 33281;

let ws;
let heartbeatInterval;
let sequence = null;
let sessionId = null;
let resumeGatewayUrl = null;
let reconnectAttempts = 0;

function log(message) {
  const time = new Date().toISOString();
  console.log(`[${time}] ${message}`);
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
      let responseData = '';
      res.on('data', (chunk) => responseData += chunk);
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
        'Authorization': `Bot ${DISCORD_TOKEN}`,
        'Content-Type': 'application/json'
      }
    };
    
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.url) {
            resolve(parsed.url);
          } else {
            reject(new Error(`Gateway error: ${data}`));
          }
        } catch (e) {
          reject(new Error(`Parse error: ${data}`));
        }
      });
    });
    
    req.on('error', reject);
    req.end();
  });
}

function identify() {
  log('Identifying with Discord...');
  ws.send(JSON.stringify({
    op: 2,
    d: {
      token: DISCORD_TOKEN,
      intents: INTENTS,
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

function startHeartbeat(interval) {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
  }
  
  heartbeatInterval = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ op: 1, d: sequence }));
    }
  }, interval);
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
    
    const wsUrl = `${gatewayUrl}?v=10&encoding=json`;
    log(`Connecting to Discord gateway: ${wsUrl}`);
    
    ws = new WebSocket(wsUrl);
    
    ws.on('open', () => {
      log('WebSocket connection opened');
      reconnectAttempts = 0;
    });
    
    ws.on('message', (data) => {
      try {
        const payload = JSON.parse(data.toString());
        
        if (payload.s) {
          sequence = payload.s;
        }
        
        switch (payload.op) {
          case 10:
            log('Received Hello from Discord');
            startHeartbeat(payload.d.heartbeat_interval);
            
            if (isResume && sessionId) {
              resume();
            } else {
              identify();
            }
            break;
            
          case 11:
            break;
            
          case 9:
            log('Session invalidated, reconnecting fresh...');
            sessionId = null;
            sequence = null;
            setTimeout(() => connect(false), 5000);
            break;
            
          case 7:
            log('Discord requested reconnect');
            ws.close();
            setTimeout(() => connect(true), 1000);
            break;
            
          case 0:
            handleDispatch(payload);
            break;
            
          default:
            break;
        }
      } catch (error) {
        log(`Error parsing message: ${error.message}`);
      }
    });
    
    ws.on('close', (code, reason) => {
      log(`WebSocket closed. Code: ${code}, Reason: ${reason}`);
      
      if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
      }
      
      const nonResumableCodes = [4004, 4010, 4011, 4012, 4013, 4014];
      
      if (nonResumableCodes.includes(code)) {
        log(`Fatal error code ${code}. Check your bot token.`);
        process.exit(1);
      }
      
      reconnectAttempts++;
      const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
      log(`Reconnecting in ${delay}ms (attempt ${reconnectAttempts})`);
      setTimeout(() => connect(true), delay);
    });
    
    ws.on('error', (error) => {
      log(`WebSocket error: ${error.message}`);
    });
    
  } catch (error) {
    log(`Connection error: ${error.message}`);
    reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
    log(`Retrying in ${delay}ms`);
    setTimeout(() => connect(false), delay);
  }
}

function handleDispatch(payload) {
  const eventType = payload.t;
  const eventData = payload.d;
  
  switch (eventType) {
    case 'READY':
      sessionId = eventData.session_id;
      resumeGatewayUrl = eventData.resume_gateway_url;
      log(`Bot ready! Logged in as: ${eventData.user.username}`);
      break;
      
    case 'RESUMED':
      log('Session resumed successfully');
      break;
      
    case 'MESSAGE_CREATE':
      handleMessage(eventData);
      break;
      
    default:
      break;
  }
}

function handleMessage(message) {
  // Ignore messages from bots including itself
  if (message.author && message.author.bot) {
    return;
  }
  
  // Ignore empty messages with no content and no attachments
  if (!message.content && (!message.attachments || message.attachments.length === 0)) {
    return;
  }
  
  log(`New message from ${message.author.username}: ${message.content || '[attachment]'}`);
  
  // Check for image attachments
  const imageAttachments = message.attachments ? message.attachments.filter(att => {
    const filename = att.filename.toLowerCase();
    return filename.endsWith('.jpg') || 
           filename.endsWith('.jpeg') || 
           filename.endsWith('.png') || 
           filename.endsWith('.webp') ||
           filename.endsWith('.gif') ||
           att.content_type && att.content_type.startsWith('image/');
  }) : [];
  
  // Build the data object to send to Activepieces
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
    allAttachments: imageAttachments.map(att => ({
      url: att.url,
      filename: att.filename,
      size: att.size,
      content_type: att.content_type
    }))
  };
  
  log(`Sending to Activepieces: hasAttachment=${dataToSend.hasAttachment}`);
  sendToActivepieces(dataToSend);
}

// Start the bot
log('Starting Discord Receipt Bot...');
log(`Token starts with: ${DISCORD_TOKEN.substring(0, 10)}...`);
log(`Webhook URL: ${ACTIVEPIECES_WEBHOOK_URL.substring(0, 50)}...`);
connect(false);