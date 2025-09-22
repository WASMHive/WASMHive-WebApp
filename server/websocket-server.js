const WebSocket = require("ws");

const wss = new WebSocket.Server({ port: 3000 });
let clients = {};
let masters = {}; // Track master nodes separately
let workers = {}; // Track worker nodes separately

wss.on("connection", (ws) => {
  // Generate a unique ID for the client.
  const id = Math.random().toString(36).substr(2, 9);
  clients[id] = { ws, type: "unknown" }; // Default to unknown until identified
  console.log(`Client connected: ${id}`);

  // Send the welcome message with this client's ID.
  ws.send(JSON.stringify({ type: "welcome", id }));

  // Initially treat as worker until master identifies itself
  workers[id] = clients[id];

  // Broadcast the updated peer list to everyone.
  broadcastPeerList();

  ws.on("message", (message) => {
    const data = JSON.parse(message);

    // Handle master registration
    if (data.type === "registerMaster") {
      console.log(`Client ${id} registered as MASTER`);
      clients[id].type = "master";
      masters[id] = clients[id];
      delete workers[id]; // Remove from workers list
      broadcastPeerList(); // Update peer lists
      return;
    }

    // Handle worker registration (explicit)
    if (data.type === "registerWorker") {
      console.log(`Client ${id} registered as WORKER`);
      clients[id].type = "worker";
      workers[id] = clients[id];
      delete masters[id]; // Remove from masters list if it was there
      broadcastPeerList(); // Update peer lists
      return;
    }

    // Relay messages between clients.
    if (data.to && clients[data.to]) {
      clients[data.to].ws.send(JSON.stringify({ ...data, from: id }));
    }
  });

  ws.on("close", () => {
    console.log(`Client disconnected: ${id} (type: ${clients[id]?.type || "unknown"})`);
    delete clients[id];
    delete masters[id];
    delete workers[id];
    broadcastPeerList();
  });
});

// Broadcast the list of worker IDs only (exclude masters from peer discovery).
function broadcastPeerList() {
  const workerList = Object.keys(workers);
  const masterList = Object.keys(masters);
  console.log(`Broadcasting peer list - Workers: [${workerList.join(', ')}], Masters: [${masterList.join(', ')}]`);

  const message = JSON.stringify({ type: "peerList", peers: workerList });
  for (const clientId in clients) {
    clients[clientId].ws.send(message);
  }
}

console.log("WebSocket server running on ws://localhost:3000");