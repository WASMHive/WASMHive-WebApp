const WebSocket = require("ws");

const wss = new WebSocket.Server({ port: 3000, host: "0.0.0.0" });
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

// Broadcast the list of all peer IDs (masters + workers) so workers can discover masters
function broadcastPeerList() {
  const workerList = Object.keys(workers);
  const masterList = Object.keys(masters);
  const allPeers = Object.keys(clients);
  console.log(`Broadcasting peer list - Workers: [${workerList.join(', ')}], Masters: [${masterList.join(', ')}]`);

  const message = JSON.stringify({ type: "peerList", peers: allPeers });
  for (const clientId in clients) {
    clients[clientId].ws.send(message);
  }

  // Also broadcast fair-share allocation of workers to masters
  const allocationPayload = computeFairShareAllocation();
  const allocationMessage = JSON.stringify({ type: "allocation", ...allocationPayload });
  for (const clientId in clients) {
    clients[clientId].ws.send(allocationMessage);
  }
}

console.log("WebSocket server running on ws://localhost:3000");

// Fair-share scheduling: distribute workers evenly across masters in round-robin order
function computeFairShareAllocation() {
  const workerList = Object.keys(workers);
  const masterList = Object.keys(masters);

  // Deterministic order for stability
  masterList.sort();
  workerList.sort();

  /** @type {Record<string, string[]>} */
  const allocation = {};
  masterList.forEach((m) => (allocation[m] = []));

  if (masterList.length === 0) {
    return {
      allocation,
      counts: {},
      totalWorkers: workerList.length,
      totalMasters: 0,
    };
  }

  for (let i = 0; i < workerList.length; i++) {
    const masterId = masterList[i % masterList.length];
    allocation[masterId].push(workerList[i]);
  }

  const counts = {};
  for (const m of masterList) counts[m] = allocation[m].length;

  return {
    allocation,
    counts,
    totalWorkers: workerList.length,
    totalMasters: masterList.length,
  };
}