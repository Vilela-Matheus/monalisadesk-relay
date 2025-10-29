// relay.js — Monalisa Desk Relay v1.0
const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;

const agents = new Map();   // agentId -> { ws, token }
const viewers = new Map();  // agentId -> Set<ws>

function log(...args){ console.log(new Date().toISOString(), ...args); }
function safeSend(ws, obj){ try{ ws.readyState===WebSocket.OPEN && ws.send(JSON.stringify(obj)); }catch{} }
function cleanupWS(ws){
  for (const [aid, entry] of agents.entries()){
    if (entry.ws === ws){ agents.delete(aid); log(`Agent desconectado: ${aid}`); }
  }
  for (const [aid, set] of viewers.entries()){
    if (set.has(ws)){ set.delete(ws); if (!set.size) viewers.delete(aid); log(`Viewer desconectado de ${aid}`); }
  }
}

const wss = new WebSocket.Server({ port: PORT }, ()=> log(`Relay rodando na porta ${PORT}`));

// keepalive simples
function heartbeat(){ this.isAlive = true; }
wss.on("connection", (ws)=>{
  ws.isAlive = true;
  ws.on("pong", heartbeat);

  let role=null, agentId=null, token=null;

  ws.on("message", (data)=>{
    let msg; try{ msg = JSON.parse(data.toString()); } catch { return; }

    // handshake inicial
    if (msg.role && !role){
      role = msg.role; agentId = msg.agentId; token = msg.token || "";
      if (!agentId || !token){ safeSend(ws, {type:"error", reason:"agentId/token ausente"}); return ws.close(); }

      if (role === "agent"){
        agents.set(agentId, { ws, token });
        if (!viewers.has(agentId)) viewers.set(agentId, new Set());
        log(`Agent registrado: ${agentId}`);
        safeSend(ws, { type:"ok", role:"agent-registered" });
        for (const v of (viewers.get(agentId) || [])) safeSend(v, { type:"agent_online", agentId });
      } else if (role === "viewer"){
        const a = agents.get(agentId);
        if (!a || a.token !== token){ safeSend(ws, {type:"error", reason:"agent inexistente ou token inválido"}); return ws.close(); }
        if (!viewers.has(agentId)) viewers.set(agentId, new Set());
        viewers.get(agentId).add(ws);
        log(`Viewer conectado ao agent ${agentId}`);
        safeSend(ws, { type:"ok", role:"viewer-connected" });
        safeSend(a.ws, { type:"viewer_joined", agentId });
      }
      return;
    }

    // roteamento
    if (role === "agent"){
      const set = viewers.get(agentId);
      if (!set || set.size===0) return;
      for (const v of set) safeSend(v, msg);
    } else if (role === "viewer"){
      const a = agents.get(agentId);
      if (!a) return;
      safeSend(a.ws, msg);
    }
  });

  ws.on("close", ()=> cleanupWS(ws));
  ws.on("error", ()=> cleanupWS(ws));
});

// encerra conexões mortas
const interval = setInterval(()=>{
  wss.clients.forEach((ws)=>{
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false; ws.ping();
  });
}, 30000);
wss.on("close", ()=> clearInterval(interval));
