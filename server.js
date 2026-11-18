const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });
const PORT = process.env.PORT || 3000;

// Each room has one host and zero or more viewers.
const rooms = new Map();
const peerInfo = new WeakMap();

app.get("/", (_req, res) => res.status(200).send("ScreenShare signaling server is running."));
app.get("/health", (_req, res) => res.json({ ok: true, service: "screenshare-signaling" }));

server.on("upgrade", (req, socket, head) => {
  if (new URL(req.url, "http://localhost").pathname !== "/ws") return socket.destroy();
  wss.handleUpgrade(req, socket, head, ws => wss.emit("connection", ws, req));
});

function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}

function removeClient(ws) {
  const info = peerInfo.get(ws);
  if (!info) return;
  const room = rooms.get(info.room);
  if (!room) return;

  if (info.role === "host" && room.host === ws) {
    for (const viewer of room.viewers.values()) {
      send(viewer, { type: "host-stopped" });
      send(viewer, { type: "error", message: "共享者已断开连接。" });
    }
    rooms.delete(info.room);
  } else if (info.role === "viewer") {
    room.viewers.delete(info.peerId);
    send(room.host, { type: "viewer-left", peerId: info.peerId });
    if (!room.host && room.viewers.size === 0) rooms.delete(info.room);
  }
  peerInfo.delete(ws);
}

wss.on("connection", ws => {
  ws.on("message", raw => {
    let m;
    try { m = JSON.parse(raw.toString()); }
    catch { return send(ws, { type: "error", message: "收到无效消息。" }); }

    const info = peerInfo.get(ws);

    if (m.type === "host") {
      const roomId = String(m.room || "").trim().toUpperCase();
      if (!/^[A-Z0-9]{4,16}$/.test(roomId))
        return send(ws, { type: "error", message: "房间号格式无效。" });
      if (rooms.has(roomId) && rooms.get(roomId).host)
        return send(ws, { type: "error", message: "房间号冲突，请重新创建房间。" });
      const room = rooms.get(roomId) || { host: null, viewers: new Map() };
      room.host = ws;
      rooms.set(roomId, room);
      peerInfo.set(ws, { role: "host", room: roomId });
      return send(ws, { type: "host-ready", room: roomId });
    }

    if (m.type === "join") {
      const roomId = String(m.room || "").trim().toUpperCase();
      const room = rooms.get(roomId);
      if (!room || !room.host || room.host.readyState !== WebSocket.OPEN)
        return send(ws, { type: "error", message: "找不到正在共享的房间，请确认链接和共享者状态。" });
      const peerId = Math.random().toString(36).slice(2, 10);
      room.viewers.set(peerId, ws);
      peerInfo.set(ws, { role: "viewer", room: roomId, peerId });
      send(ws, { type: "joined", room: roomId, peerId });
      return send(room.host, { type: "viewer-joined", peerId });
    }

    if (!info) return send(ws, { type: "error", message: "请先创建房间或加入房间。" });
    const room = rooms.get(info.room);
    if (!room) return send(ws, { type: "error", message: "房间已关闭。" });

    if (m.type === "host-stop" && info.role === "host") {
      for (const viewer of room.viewers.values()) send(viewer, { type: "host-stopped" });
    } else if (m.type === "offer" && info.role === "host") {
      send(room.viewers.get(String(m.target || "")), {
        type: "offer", from: "host", description: m.description
      });
    } else if (m.type === "answer" && info.role === "viewer") {
      send(room.host, { type: "answer", from: info.peerId, description: m.description });
    } else if (m.type === "ice") {
      if (info.role === "host") {
        send(room.viewers.get(String(m.target || "")), {
          type: "ice", from: "host", candidate: m.candidate
        });
      } else {
        send(room.host, { type: "ice", from: info.peerId, candidate: m.candidate });
      }
    }
  });
  ws.on("close", () => removeClient(ws));
  ws.on("error", () => removeClient(ws));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("ScreenShare signaling server listening on port " + PORT);
});