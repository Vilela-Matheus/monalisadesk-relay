import asyncio
import json
import websockets

# === CONFIG ===
HOST = "0.0.0.0"
PORT = 8080  # Railway automaticamente expõe esta porta
agents = {}  # { agentId: { "token": "abc", "ws": ws } }
viewers = {} # { agentId: ws }

async def handle(ws, path):
    try:
        raw = await ws.recv()
        hello = json.loads(raw)
        role = hello.get("role")

        # === Agent se conecta ===
        if role == "agent":
            agentId = hello.get("agentId")
            token = hello.get("token")
            agents[agentId] = {"token": token, "ws": ws}
            print(f"🟢 Agent {agentId} conectado com token {token}")
            await ws.send(json.dumps({"type": "ok", "role": "agent"}))

            async for msg in ws:
                # repassa frames e infos pro viewer correspondente
                payload = json.loads(msg)
                vws = viewers.get(agentId)
                if vws:
                    await vws.send(json.dumps(payload))

        # === Viewer se conecta ===
        elif role == "viewer":
            agentId = hello.get("agentId")
            token = hello.get("token")
            agent = agents.get(agentId)

            if not agent or agent["token"] != token:
                print(f"🚫 Tentativa inválida para {agentId}")
                await ws.send(json.dumps({"type": "error", "reason": "token inválido ou inexistente"}))
                return

            viewers[agentId] = ws
            print(f"👁️ Viewer conectado ao agent {agentId}")
            await ws.send(json.dumps({"type": "ok", "role": "viewer-connected"}))

            async for msg in ws:
                payload = json.loads(msg)
                aws = agent["ws"]
                await aws.send(json.dumps(payload))

    except Exception as e:
        print("⚠️ Conexão encerrada:", e)
    finally:
        # Limpa agentes/viewers mortos
        for a in list(agents.keys()):
            if agents[a]["ws"] == ws:
                del agents[a]
        for v in list(viewers.keys()):
            if viewers[v] == ws:
                del viewers[v]

print(f"🚀 Iniciando relay em {HOST}:{PORT}")
asyncio.get_event_loop().run_until_complete(
    websockets.serve(handle, HOST, PORT)
)
asyncio.get_event_loop().run_forever()
