import http from "node:http";
import os from "node:os";
import { WebSocket, WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 8787);
const rooms = new Map();
let nextClientId = 1;

const CORS_HEADERS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET,POST,OPTIONS",
  "access-control-allow-headers": "content-type"
};

function createClientRecord(socket = null, transport = "ws") {
  return {
    id: `c${nextClientId++}`,
    socket,
    transport,
    roomId: "",
    role: "",
    playerId: "p2",
    queue: [],
    lastSeenAt: Date.now(),
    lastSnapshotSent: ""
  };
}

function getRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      id: roomId,
      host: null,
      clients: new Map(),
      lastSnapshot: ""
    });
  }
  return rooms.get(roomId);
}

function roomPeerCount(room) {
  return (room.host ? 1 : 0) + room.clients.size;
}

function safeSend(socket, payload) {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }
  socket.send(JSON.stringify(payload));
}

function deliverToClient(client, payload) {
  if (!client) {
    return;
  }
  client.lastSeenAt = Date.now();
  if (client.transport === "http") {
    client.queue.push(payload);
    if (client.queue.length > 64) {
      client.queue.splice(0, client.queue.length - 64);
    }
    return;
  }
  safeSend(client.socket, payload);
}

function deliverToPeer(peer, payload) {
  if (!peer) {
    return;
  }
  peer.lastSeenAt = Date.now();
  if (peer.transport === "http") {
    peer.queue.push(payload);
    if (peer.queue.length > 64) {
      peer.queue.splice(0, peer.queue.length - 64);
    }
    return;
  }
  safeSend(peer.socket, payload);
}

function broadcastRoomStatus(room, status) {
  const payload = {
    type: "peer",
    roomId: room.id,
    peers: roomPeerCount(room),
    status
  };
  deliverToPeer(room.host, payload);
  room.clients.forEach((client) => {
    deliverToClient(client, payload);
  });
}

function detachClient(client) {
  if (!client || !client.roomId) {
    return;
  }
  const room = rooms.get(client.roomId);
  if (!room) {
    return;
  }
  if (client.role === "host" && room.host && room.host.id === client.id) {
    room.host = null;
    room.clients.forEach((peer) => {
      deliverToClient(peer, {
        type: "error",
        roomId: room.id,
        message: "房主已断开，房间已失效。"
      });
      if (peer.socket) {
        try {
          peer.socket.close();
        } catch (error) {
          // Ignore close errors during shutdown.
        }
      }
    });
    room.clients.clear();
  } else {
    room.clients.delete(client.id);
  }
  if (!room.host && room.clients.size === 0) {
    rooms.delete(room.id);
    return;
  }
  broadcastRoomStatus(room, `${client.role} 离开房间`);
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, {
    ...CORS_HEADERS,
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store"
  });
  response.end(JSON.stringify(payload));
}

function getRequestUrl(request) {
  return new URL(request.url || "/", `http://${request.headers.host || "127.0.0.1"}`);
}

function readJsonBody(request) {
  return new Promise((resolve, reject) => {
    let body = "";
    request.on("data", (chunk) => {
      body += String(chunk);
      if (body.length > 512000) {
        reject(new Error("请求体过大。"));
      }
    });
    request.on("end", () => {
      if (!body) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(body));
      } catch (error) {
        reject(new Error("请求体必须为 JSON。"));
      }
    });
    request.on("error", reject);
  });
}

function getRoomClient(room, clientId) {
  if (!room || !clientId) {
    return null;
  }
  return room.clients.get(String(clientId)) || null;
}

async function handleHttpJoin(request, response) {
  const body = await readJsonBody(request);
  const roomId = String(body.roomId || "ripple-room");
  const role = body.role === "client" ? "client" : "host";
  const playerId = String(body.playerId || "p2");
  const room = getRoom(roomId);
  const client = createClientRecord(null, "http");
  client.roomId = roomId;
  client.role = role;
  client.playerId = playerId;
  if (role === "host") {
    if (room.host) {
      sendJson(response, 409, { ok: false, message: "该房间已有房主。" });
      return;
    }
    room.host = client;
  } else {
    room.clients.set(client.id, client);
  }
  const joinedMessage = {
    type: "joined",
    roomId,
    role,
    playerId,
    clientId: client.id,
    peers: roomPeerCount(room)
  };
  console.log(`[http-join] room=${roomId} player=${playerId} client=${client.id} peers=${roomPeerCount(room)}`);
  broadcastRoomStatus(room, `${role} 已加入房间`);
  client.lastSnapshotSent = room.lastSnapshot || "";
  sendJson(response, 200, {
    ok: true,
    message: joinedMessage,
    snapshot: room.lastSnapshot || "",
    peers: roomPeerCount(room)
  });
}

function handleHttpHostPoll(request, response) {
  const url = getRequestUrl(request);
  const roomId = String(url.searchParams.get("roomId") || "ripple-room");
  const clientId = String(url.searchParams.get("clientId") || "");
  const room = rooms.get(roomId);
  if (!room || !room.host || room.host.id !== clientId) {
    sendJson(response, 404, { ok: false, message: "房主不存在，请重新加入。" });
    return;
  }
  room.host.lastSeenAt = Date.now();
  const messages = room.host.queue.splice(0, room.host.queue.length);
  sendJson(response, 200, {
    ok: true,
    roomId,
    clientId,
    peers: roomPeerCount(room),
    messages
  });
}

function handleHttpPoll(request, response) {
  const url = getRequestUrl(request);
  const roomId = String(url.searchParams.get("roomId") || "ripple-room");
  const clientId = String(url.searchParams.get("clientId") || "");
  const room = rooms.get(roomId);
  if (!room) {
    sendJson(response, 404, { ok: false, message: "房间不存在。" });
    return;
  }
  const client = getRoomClient(room, clientId);
  if (!client) {
    sendJson(response, 404, { ok: false, message: "客户端不存在，请重新加入。" });
    return;
  }
  client.lastSeenAt = Date.now();
  const messages = client.queue.splice(0, client.queue.length);
  let snapshot = "";
  if (room.lastSnapshot && client.lastSnapshotSent !== room.lastSnapshot) {
    snapshot = room.lastSnapshot;
    client.lastSnapshotSent = room.lastSnapshot;
  }
  sendJson(response, 200, {
    ok: true,
    roomId,
    clientId,
    peers: roomPeerCount(room),
    messages,
    snapshot
  });
}

async function handleHttpInput(request, response) {
  const body = await readJsonBody(request);
  const roomId = String(body.roomId || "ripple-room");
  const clientId = String(body.clientId || "");
  const packets = Array.isArray(body.packets)
    ? body.packets.filter((item) => typeof item === "string" && item)
    : (typeof body.packet === "string" && body.packet ? [body.packet] : []);
  const room = rooms.get(roomId);
  if (!room) {
    sendJson(response, 404, { ok: false, message: "房间不存在。" });
    return;
  }
  const client = getRoomClient(room, clientId);
  if (!client) {
    sendJson(response, 404, { ok: false, message: "客户端不存在，请重新加入。" });
    return;
  }
  if (!room.host) {
    sendJson(response, 409, { ok: false, message: "房主未上线。" });
    return;
  }
  let forwarded = 0;
  packets.forEach((packet) => {
    deliverToPeer(room.host, {
      type: "input",
      roomId: room.id,
      playerId: client.playerId,
      packet
    });
    forwarded += 1;
  });
  client.lastSeenAt = Date.now();
  if (forwarded > 0) {
    console.log(`[http-input] room=${room.id} from=${client.playerId} packets=${forwarded}`);
  }
  sendJson(response, 200, {
    ok: true,
    roomId,
    clientId,
    forwarded
  });
}

async function handleHttpSnapshot(request, response) {
  const body = await readJsonBody(request);
  const roomId = String(body.roomId || "ripple-room");
  const clientId = String(body.clientId || "");
  const snapshot = String(body.snapshot || "");
  const room = rooms.get(roomId);
  if (!room || !room.host || room.host.id !== clientId) {
    sendJson(response, 404, { ok: false, message: "房主不存在，请重新加入。" });
    return;
  }
  room.host.lastSeenAt = Date.now();
  room.lastSnapshot = snapshot;
  sendJson(response, 200, {
    ok: true,
    roomId,
    peers: roomPeerCount(room)
  });
}

const server = http.createServer(async (request, response) => {
  try {
    const url = getRequestUrl(request);
    if (request.method === "OPTIONS") {
      response.writeHead(204, CORS_HEADERS);
      response.end();
      return;
    }
    if (request.method === "GET" && url.pathname === "/") {
      sendJson(response, 200, {
        ok: true,
        service: "ripple-catastrophe-lan-host",
        rooms: Array.from(rooms.values()).map((room) => ({
          id: room.id,
          peers: roomPeerCount(room),
          hasHost: !!room.host
        }))
      });
      return;
    }
    if (request.method === "POST" && url.pathname === "/join") {
      await handleHttpJoin(request, response);
      return;
    }
    if (request.method === "GET" && url.pathname === "/host/poll") {
      handleHttpHostPoll(request, response);
      return;
    }
    if (request.method === "GET" && url.pathname === "/poll") {
      handleHttpPoll(request, response);
      return;
    }
    if (request.method === "POST" && url.pathname === "/input") {
      await handleHttpInput(request, response);
      return;
    }
    if (request.method === "POST" && url.pathname === "/snapshot") {
      await handleHttpSnapshot(request, response);
      return;
    }
    sendJson(response, 404, { ok: false, message: "未找到接口。" });
  } catch (error) {
    sendJson(response, 500, {
      ok: false,
      message: error instanceof Error ? error.message : "服务异常。"
    });
  }
});

const wss = new WebSocketServer({ server });

wss.on("connection", (socket) => {
  const client = createClientRecord(socket, "ws");

  socket.on("message", (raw) => {
    let message = null;
    try {
      message = JSON.parse(String(raw));
    } catch (error) {
      safeSend(socket, { type: "error", message: "消息必须是 JSON。" });
      return;
    }

    if (!message || typeof message.type !== "string") {
      safeSend(socket, { type: "error", message: "缺少消息类型。" });
      return;
    }

    if (message.type === "join") {
      const roomId = String(message.roomId || "ripple-room");
      const role = message.role === "client" ? "client" : "host";
      const playerId = String(message.playerId || "p2");
      const room = getRoom(roomId);

      if (role === "host") {
        if (room.host) {
          safeSend(socket, { type: "error", roomId, message: "该房间已有房主。" });
          return;
        }
        room.host = client;
      } else {
        room.clients.set(client.id, client);
      }

      client.roomId = roomId;
      client.role = role;
      client.playerId = playerId;

      safeSend(socket, {
        type: "joined",
        roomId,
        role,
        playerId,
        clientId: client.id,
        peers: roomPeerCount(room)
      });
      if (role === "client" && room.lastSnapshot) {
        safeSend(socket, {
          type: "snapshot",
          roomId,
          snapshot: room.lastSnapshot
        });
      }
      console.log(`[join] room=${roomId} role=${role} player=${playerId} peers=${roomPeerCount(room)}`);
      broadcastRoomStatus(room, `${role} 已加入房间`);
      return;
    }

    if (!client.roomId) {
      safeSend(socket, { type: "error", message: "请先 join 房间。" });
      return;
    }

    const room = rooms.get(client.roomId);
    if (!room) {
      safeSend(socket, { type: "error", message: "房间不存在。" });
      return;
    }

    if (message.type === "input") {
      if (client.role !== "client") {
        safeSend(socket, { type: "error", roomId: room.id, message: "只有客户端可以发送输入包。" });
        return;
      }
      if (!room.host) {
        safeSend(socket, { type: "error", roomId: room.id, message: "房主未上线。" });
        return;
      }
      safeSend(room.host.socket, {
        type: "input",
        roomId: room.id,
        playerId: client.playerId,
        packet: message.packet
      });
      console.log(`[input] room=${room.id} from=${client.playerId} bytes=${String(message.packet || "").length}`);
      return;
    }

    if (message.type === "snapshot") {
      if (client.role !== "host") {
        safeSend(socket, { type: "error", roomId: room.id, message: "只有房主可以广播状态快照。" });
        return;
      }
      room.lastSnapshot = String(message.snapshot || "");
      room.clients.forEach((peer) => {
        deliverToClient(peer, {
          type: "snapshot",
          roomId: room.id,
          snapshot: message.snapshot
        });
      });
      return;
    }

    if (message.type === "ping") {
      safeSend(socket, {
        type: "pong",
        roomId: client.roomId,
        now: Date.now()
      });
      return;
    }

    safeSend(socket, { type: "error", roomId: client.roomId, message: `未知消息类型: ${message.type}` });
  });

  socket.on("close", () => {
    detachClient(client);
  });

  socket.on("error", () => {
    detachClient(client);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  const interfaces = os.networkInterfaces();
  const addresses = [];
  Object.values(interfaces).forEach((items) => {
    (items || []).forEach((item) => {
      if (item.family === "IPv4" && !item.internal) {
        addresses.push(item.address);
      }
    });
  });
  console.log(`Ripple LAN host listening on ws://0.0.0.0:${PORT}`);
  console.log(`HTTP relay available on http://0.0.0.0:${PORT}`);
  addresses.forEach((address) => {
    console.log(`  ws://${address}:${PORT}`);
    console.log(`  http://${address}:${PORT}`);
  });
});
