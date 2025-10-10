const WebSocket = require("ws");

const wss = new WebSocket.Server({ port: 3000 });
let nodes = {};
let clients = {}; // Track client nodes (task distributors) separately
let workers = {}; // Track worker nodes separately

wss.on("connection", (ws) => {
  // Generate a unique ID for the node.
  const id = Math.random().toString(36).substr(2, 9);
  nodes[id] = { ws, type: "unknown" }; // Default to unknown until identified
  console.log(`Node connected: ${id}`);

  // Send the welcome message with this node's ID.
  ws.send(JSON.stringify({ type: "welcome", id }));

  // Initially treat as worker until client identifies itself
  workers[id] = nodes[id];

  // Broadcast the updated peer list to everyone.
  broadcastPeerList();

  ws.on("message", (message) => {
    const data = JSON.parse(message);

    // Handle client registration (task distributor)
    if (data.type === "registerMaster") {
      console.log(`Node ${id} registered as CLIENT`);
      nodes[id].type = "client";
      clients[id] = nodes[id];
      delete workers[id]; // Remove from workers list
      broadcastPeerList(); // Update peer lists
      return;
    }

    // Handle worker registration (explicit)
    if (data.type === "registerWorker") {
      console.log(`Node ${id} registered as WORKER`);
      nodes[id].type = "worker";
      workers[id] = nodes[id];
      delete clients[id]; // Remove from clients list if it was there
      broadcastPeerList(); // Update peer lists
      return;
    }

    // Relay messages between nodes.
    if (data.to && nodes[data.to]) {
      nodes[data.to].ws.send(JSON.stringify({ ...data, from: id }));
    }
  });

  ws.on("close", () => {
    console.log(`Node disconnected: ${id} (type: ${nodes[id]?.type || "unknown"})`);
    delete nodes[id];
    delete clients[id];
    delete workers[id];
    broadcastPeerList();
  });
});

// Broadcast the list of worker IDs only (exclude clients from peer discovery).
function broadcastPeerList() {
  const workerList = Object.keys(workers);
  const clientList = Object.keys(clients);
  console.log(`Broadcasting peer list - Workers: [${workerList.join(', ')}], Clients: [${clientList.join(', ')}]`);

  const message = JSON.stringify({ type: "peerList", peers: workerList });
  for (const nodeId in nodes) {
    nodes[nodeId].ws.send(message);
  }
}

console.log("WebSocket server running on ws://localhost:3000");