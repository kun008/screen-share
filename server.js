const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    return res.end(JSON.stringify({ ok: true, service: "screenshare-signaling" }));
  }
  res.writeHead(404);
  res.end("Not found");
});
const wss = new WebSocket.Server({ server, path: "/ws", maxPayload: 1024 * 1024 });
const rooms = new Map();
let nextPeerId = 1;

function send(ws, payload) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload));
}
function removePeer(ws) {
  const room = rooms.get(ws.room);
  if (!room) return;
  if (room.host === ws) {
    for (const viewer of room.viewers.values()) send(viewer, { type: "host-stopped" });
    rooms.delete(ws.room);
    return;
  }
  if (ws.peerId && room.viewers.get(ws.peerId) === ws) {
    room.viewers.delete(ws.peerId);
    send(room.host, { type: "viewer-left", peerId: ws.peerId });
  }
  if (!room.host && room.viewers.size === 0) rooms.delete(ws.room);
}
wss.on("connection", (ws) => {
  ws.peerId = String(nextPeerId++);
  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return send(ws, { type: "error", message: "无效的数据格式。" }); }
    if (msg.type === "host") {
      const roomId = String(msg.room || "").toUpperCase();
      if (!/^[A-Z0-9]{6,12}$/.test(roomId)) return send(ws, { type: "error", message: "房间号格式无效。" });
      if (rooms.has(roomId)) return send(ws, { type: "error", message: "房间号冲突，请重新创建。" });
      ws.room = roomId;
      const room = { host: ws, viewers: new Map() };
      rooms.set(roomId, room);
      return send(ws, { type: "host-ready", room: roomId });
    }
    if (msg.type === "join") {
      const roomId = String(msg.room || "").toUpperCase();
      const room = rooms.get(roomId);
      if (!room || !room.host || room.host.readyState !== WebSocket.OPEN) return send(ws, { type: "error", message: "找不到该房间，或共享者尚未开始共享。" });
      ws.room = roomId;
      room.viewers.set(ws.peerId, ws);
      send(ws, { type: "joined", room: roomId, peerId: ws.peerId });
      return send(room.host, { type: "viewer-joined", peerId: ws.peerId });
    }
    const room = rooms.get(ws.room);
    if (!room) return;
    if (msg.type === "host-stop" && room.host === ws) {
      for (const viewer of room.viewers.values()) send(viewer, { type: "host-stopped" });
      rooms.delete(ws.room);
      return;
    }
    const targetId = String(msg.target || "");
    const target = room.viewers.get(targetId) || (room.host && room.host.peerId === targetId ? room.host : null);
    if (["offer", "answer", "ice"].includes(msg.type)) {
      if (room.host === ws && target) return send(target, { ...msg, from: ws.peerId });
      if (room.viewers.get(ws.peerId) === ws && room.host) return send(room.host, { ...msg, from: ws.peerId });
    }
  });
  ws.on("close", () => removePeer(ws));
  ws.on("error", () => removePeer(ws));
});
server.listen(PORT, "0.0.0.0", () => console.log("ScreenShare signaling server listening on " + PORT));
