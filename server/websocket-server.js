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

  for (const nodeId in nodes) {
    const node = nodes[nodeId];
    // Clients (masters) should only see workers; Workers should see workers + clients (so they can dial masters)
    const peers = node.type === "client"
      ? workerList
      : Array.from(new Set([...workerList, ...clientList]));

    node.ws.send(JSON.stringify({ type: "peerList", peers }));
  }

  // Also broadcast fair-share allocation of workers to clients
  const allocationPayload = computeFairShareAllocation();
  for (const nodeId in nodes) {
    const node = nodes[nodeId];
    node.ws.send(JSON.stringify({ type: "allocation", ...allocationPayload }));
  }
}

// Fair-share scheduling: distribute workers evenly across clients in round-robin order
function computeFairShareAllocation() {
  const workerList = Object.keys(workers);
  const clientList = Object.keys(clients);

  // Deterministic order for stability
  clientList.sort();
  workerList.sort();

  /** @type {Record<string, string[]>} */
  const allocation = {};
  clientList.forEach((c) => (allocation[c] = []));

  if (clientList.length === 0) {
    return {
      allocation,
      counts: {},
      totalWorkers: workerList.length,
      totalClients: 0,
    };
  }

  for (let i = 0; i < workerList.length; i++) {
    const clientId = clientList[i % clientList.length];
    allocation[clientId].push(workerList[i]);
  }

  const counts = {};
  for (const c of clientList) counts[c] = allocation[c].length;

  return {
    allocation,
    counts,
    totalWorkers: workerList.length,
    totalClients: clientList.length,
  };
}

console.log("WebSocket server running on ws://localhost:3000");