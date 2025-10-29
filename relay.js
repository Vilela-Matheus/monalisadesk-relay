import { WebSocketServer } from "ws";

const PORT = process.env.PORT || 8080;

const agents = new Map();   // agentId -> { ws, token }
const viewers = new Map();  // agentId -> Set<ws>

const wss = new WebSocketServer({ port: PORT }, () =>
  console.log(`[relay] up on ${PORT}`)
);

function sendSafe(ws, obj) {
  try { ws.readyState === 1 && ws.send(JSON.stringify(obj)); } catch {}
}
function broadcastToViewers(agentId, obj) {
  const set = viewers.get(agentId);
  if (!set) return;
  for (const v of set) sendSafe(v, obj);
}
function cleanup(ws) {
  for (const [aid, entry] of agents.entries()) {
    if (entry.ws === ws) {
      agents.delete(aid);
      console.log(`[relay] agent offline: ${aid}`);
      broadcastToViewers(aid, { type: "agent_offline", agentId: aid });
    }
  }
  for (const [aid, set] of viewers.entries()) {
    if (set.delete(ws) && set.size === 0) {
      viewers.delete(aid);
      const a = agents.get(aid);
      if (a) sendSafe(a.ws, { type: "viewer_left", agentId: aid });
    }
  }
}

// keepalive
function heartbeat() { this.isAlive = true; }

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", heartbeat);

  let role = null, agentId = null, token = null;

  ws.on("message", (buf) => {
    let msg;
    try { msg = JSON.parse(buf.toString()); } catch { return; }

    // handshake
    if (msg.role && !role) {
      role = msg.role;
      agentId = msg.agentId;
      token = msg.token || "";
      if (!agentId || !token) {
        sendSafe(ws, { type: "error", reason: "agentId/token ausente" });
        return ws.close();
      }

      if (role === "agent") {
        agents.set(agentId, { ws, token });
        if (!viewers.has(agentId)) viewers.set(agentId, new Set());
        console.log(`[relay] agent on: ${agentId}`);
        sendSafe(ws, { type: "ok", role: "agent-registered" });
        // avisa viewers conectados (se já existirem)
        broadcastToViewers(agentId, { type: "agent_online", agentId });
      } else if (role === "viewer") {
        const a = agents.get(agentId);
        if (!a || a.token !== token) {
          sendSafe(ws, { type: "error", reason: "token inválido ou inexistente" });
          return ws.close();
        }
        if (!viewers.has(agentId)) viewers.set(agentId, new Set());
        viewers.get(agentId).add(ws);
        console.log(`[relay] viewer linked -> ${agentId}`);
        sendSafe(ws, { type: "ok", role: "viewer-connected" });
        sendSafe(a.ws, { type: "viewer_joined", agentId });
      }
      return;
    }

    // roteamento
    if (role === "agent") {
      broadcastToViewers(agentId, msg);
    } else if (role === "viewer") {
      const a = agents.get(agentId);
      if (a) sendSafe(a.ws, msg);
    }
  });

  ws.on("close", () => cleanup(ws));
  ws.on("error", () => cleanup(ws));
});

// encerra conexões mortas
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false; ws.ping();
  });
}, 30000);
