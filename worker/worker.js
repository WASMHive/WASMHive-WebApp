// Worker Node - receives compute tasks and WASM modules from master via WebRTC

// Signaling endpoint. A hosted page joins a remote hive via ?server=, e.g.
//   ?server=wss://hive.example/ws   or   ?server=192.168.1.10:3000
// A bare host:port gets a scheme matching the page (wss when served over https).
const SIGNALING_URL = (() => {
    const raw = new URLSearchParams(location.search).get("server");
    if (!raw) return "ws://localhost:3000";
    if (/^wss?:\/\//i.test(raw)) return raw;
    return (location.protocol === "https:" ? "wss://" : "ws://") + raw;
})();
let ws = null; // (re)created by connectSignaling()
let myId;
let peerConnections = {}; // { peerId: RTCPeerConnection }
let dataChannels = {}; // { peerId: RTCDataChannel }
let connectedPeers = {}; // { peerId: true } when data channel is open
let computeHistory = []; // Store compute task history
let rawModuleCache = {}; // { module_hash: { wasm: Uint8Array, glue: string } } persists across jobs
let pendingModuleWaits = {}; // { module_hash: { resolve, reject, timer } }
let latestPeers = []; // Cache latest peer list for redraws
let latestPeerTypes = null; // { peerId: "master"|"worker"|"unknown" }, absent on old servers
let latestAllocation = null; // Cache latest allocation payload

// Connection lifecycle
let connState = "connecting"; // "connecting" | "online" | "reconnecting" | "down"
let reconnectAttempt = 0;
let reconnectTimer = null;
let keepaliveTimer = null;
let intentionalClose = false; // set by the failure demo to suppress auto-reconnect
let announceRecoveryOnWelcome = false;

// Fault tolerance tracking
let workerHealth = "healthy"; // derived from connState in updateHealthStatus
let tasksCompleted = 0;
let tasksFailed = 0;
let tasksRunning = 0; // tasks currently executing in the exec-worker pool
let faultToleranceEvents = []; // Store fault tolerance events
let failedWorkers = new Set(); // Peers whose data channel errored while still listed

// Exec-worker pool: WASM runs here, off the main thread, so the tab and its
// dashboard stay responsive during compute.
const EXEC_POOL_SIZE = Math.min(navigator.hardwareConcurrency || 2, 4);
let execPool = []; // [{ worker, objectUrl, inFlight, loadedHashes: Set }]
let pendingExecTasks = {}; // { taskId: { header, channel, entry, inputLen, dispatchedAt } }
let lastTaskInfo = null; // { fn, ms } of the most recent completed task
let completionTimes = []; // performance.now() of recent completions (throughput window)

// SVG topology elements
const networkSvg = document.getElementById("networkSvg");

// ===== Binary wire protocol constants (mirrors distribute_runtime/src/protocol.rs) =====
const FRAME_MAGIC = 0xa5;
const PROTO_VERSION = 1;
const FRAME_TASK = 1;
const FRAME_RESULT = 2;
const FRAME_MODULE = 3;
const FRAME_CONTROL = 4;
const MAX_FRAME_PAYLOAD = 60000;
const BUFFERED_HIGH_WATER = 4 * 1024 * 1024;

// In-flight transfer reassembly: { transfer_id: { peerId, total, parts, received } }
let transfers = {};

// Send JSON over the signaling socket only when it is actually open; sends
// during reconnect windows are dropped (peer state is rebuilt on reconnect).
function safeSend(obj) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(obj));
        return true;
    }
    console.log("safeSend: signaling not open, dropped", obj && obj.type);
    return false;
}

// ===== Signaling connection lifecycle =====

function connectSignaling() {
    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }
    if (ws && (ws.readyState === WebSocket.CONNECTING || ws.readyState === WebSocket.OPEN)) return;
    ws = new WebSocket(SIGNALING_URL);
    ws.onopen = handleWsOpen;
    ws.onmessage = handleWsMessage;
    ws.onclose = handleWsClose;
    ws.onerror = handleWsError;
    updateHealthStatus();
}

function scheduleReconnect() {
    if (intentionalClose || reconnectTimer) return;
    reconnectAttempt++;
    const base = Math.min(1000 * 2 ** (reconnectAttempt - 1), 30000);
    const delay = Math.round(base * (0.75 + Math.random() * 0.5));
    // Attempt 1 and every 5th afterwards go to the event feed; the status
    // line updates every attempt without scrolling anything.
    if (reconnectAttempt === 1 || reconnectAttempt % 5 === 0) {
        addFaultToleranceEvent("info", `Reconnecting to signaling server (attempt ${reconnectAttempt})`);
    }
    const wsEl = document.getElementById("wsStatus");
    if (wsEl) {
        wsEl.innerText = `Reconnecting - attempt ${reconnectAttempt} (next in ~${Math.round(delay / 1000)}s)`;
        wsEl.className = "status disconnected";
    }
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectSignaling();
    }, delay);
}

function forceReconnect() {
    intentionalClose = false;
    announceRecoveryOnWelcome = true;
    reconnectAttempt = 0;
    connState = "reconnecting";
    connectSignaling();
}

function startKeepalive() {
    stopKeepalive();
    // A send on a dead link eventually errors the socket, which fires onclose
    // and the backoff loop; without traffic a half-open socket lingers forever.
    keepaliveTimer = setInterval(() => safeSend({ type: "ping" }), 25000);
}

function stopKeepalive() {
    if (keepaliveTimer) {
        clearInterval(keepaliveTimer);
        keepaliveTimer = null;
    }
}

// Drop every per-peer object. Module cache survives on purpose: modules are
// content-addressed and stay valid across signaling sessions.
// pendingModuleWaits entries self-heal via their own 30s timeout.
function resetPeerState() {
    Object.keys(peerConnections).forEach((peerId) => teardownPeer(peerId, true));
    peerConnections = {};
    dataChannels = {};
    connectedPeers = {};
    transfers = {};
    failedWorkers.clear();
    latestPeers = [];
    latestPeerTypes = null;
    const peerListEl = document.getElementById("peerList");
    if (peerListEl) peerListEl.innerHTML = "None";
    const totalEl = document.getElementById("totalPeers");
    if (totalEl) totalEl.innerText = "0";
    drawNetwork(latestPeers);
}

function handleWsOpen() {
    console.log("Worker connected to WebSocket server");
    document.getElementById("wsStatus").innerText = "Connected to signaling server";
    document.getElementById("wsStatus").className = "status connected";
    connState = "online";
    reconnectAttempt = 0;
    safeSend({ type: "registerWorker" });
    startKeepalive();
    updateHealthStatus();
}

function handleWsMessage(message) {
    const data = JSON.parse(message.data);
    console.log("Received WS message:", data);

    if (data.type === "welcome") {
        myId = data.id;
        document.getElementById("workerId").innerText = myId;
        if (announceRecoveryOnWelcome) {
            announceRecoveryOnWelcome = false;
            addFaultToleranceEvent("recovery", `Re-registered with signaling server (new id: ${myId})`);
        }
    }
    if (data.type === "peerList") {
        if (myId) updatePeerList(data.peers, data.peerTypes);
    }
    if (data.type === "allocation") {
        renderAllocation(data);
    }
    if (data.type === "offer") {
        handleOffer(data);
    }
    if (data.type === "answer") {
        const pc = peerConnections[data.from];
        if (pc) {
            console.log("Received answer from", data.from);
            pc.setRemoteDescription(new RTCSessionDescription(data.answer))
                .then(() => {
                    // Flush candidates that arrived while the answer was
                    // still being applied (sub-millisecond on localhost).
                    const pending = pc.pendingCandidates || [];
                    pc.pendingCandidates = [];
                    pending.forEach((candidate) =>
                        pc.addIceCandidate(candidate).catch((e) =>
                            console.log(`addIceCandidate failed for ${data.from}: ${e.name}: ${e.message}`)));
                })
                .catch((e) =>
                    console.log(`setRemoteDescription(answer) failed for ${data.from}: ${e.name}: ${e.message}`));
        }
    }
    if (data.type === "candidate") {
        const pc = peerConnections[data.from];
        if (pc) {
            const candidate = new RTCIceCandidate(data.candidate);
            if (!pc.remoteDescription) {
                // Answer not applied yet: buffer, the answer handler flushes.
                (pc.pendingCandidates = pc.pendingCandidates || []).push(candidate);
            } else {
                pc.addIceCandidate(candidate).catch((e) =>
                    console.log(`addIceCandidate failed for ${data.from}: ${e.name}: ${e.message}`));
            }
        }
    }
}

function handleWsClose(event) {
    if (event && event.target !== ws) return; // event from a superseded socket
    console.log("Disconnected from WebSocket signaling server");
    stopKeepalive();
    resetPeerState();
    myId = null;
    const idEl = document.getElementById("workerId");
    const wsEl = document.getElementById("wsStatus");
    if (intentionalClose) {
        connState = "down";
        if (idEl) idEl.innerText = "Offline (simulated)";
        if (wsEl) {
            wsEl.innerText = "Disconnected - simulated failure";
            wsEl.className = "status disconnected";
        }
    } else {
        if (connState === "online") {
            addFaultToleranceEvent("failure", "Signaling connection lost - attempting reconnect");
            announceRecoveryOnWelcome = true;
        }
        connState = "reconnecting";
        if (idEl) idEl.innerText = "Reconnecting...";
        scheduleReconnect();
    }
    updateHealthStatus();
}

function handleWsError(error) {
    // One of these fires per failed reconnect attempt; onclose drives the
    // lifecycle, so this stays log-only to keep the event feed honest.
    console.error("WebSocket error:", error);
}

// Fully release a peer: close its connection and drop all state keyed by it.
// Idempotent. Nulling the channel handlers first keeps our own close() from
// echoing back as a lifecycle event.
function teardownPeer(peerId, silent) {
    const channel = dataChannels[peerId];
    if (channel) {
        channel.onclose = null;
        channel.onerror = null;
        try { channel.close(); } catch (e) { /* already closed */ }
    }
    const pc = peerConnections[peerId];
    if (pc) {
        try { pc.close(); } catch (e) { /* already closed */ }
    }
    delete peerConnections[peerId];
    delete dataChannels[peerId];
    delete connectedPeers[peerId];
    dropTransfersFor(peerId);
    if (!silent) {
        updateStatus();
        updateHealthStatus();
    }
}

function dropTransfersFor(peerId) {
    Object.keys(transfers).forEach((transferId) => {
        if (transfers[transferId].peerId === peerId) delete transfers[transferId];
    });
}

// With a role-aware server only masters get dialed; a server that predates
// peerTypes gets the old dial-everyone behavior.
function isMasterPeer(peerId) {
    return !latestPeerTypes || latestPeerTypes[peerId] === "master";
}

function updatePeerList(peers, peerTypes) {
    const others = peers.filter((id) => id !== myId);
    latestPeerTypes = peerTypes || null;

    // Peers gone from the list have left signaling (normal at job end for
    // masters): release their state so a later rejoin dials fresh.
    const currentPeerSet = new Set(peers);
    const previousPeerSet = new Set(latestPeers || []);
    previousPeerSet.forEach(peerId => {
        if (!currentPeerSet.has(peerId) && peerId !== myId) {
            addFaultToleranceEvent("info", `Peer ${peerId} left the network`);
            teardownPeer(peerId, true);
            failedWorkers.delete(peerId);
        }
    });

    latestPeers = peers.slice();

    // Show all peers with current peer in bold
    if (peers.length > 0) {
        const peerDisplay = peers.map(id => {
            let display = id;
            if (id === myId) {
                display = `<strong>${id}</strong>`;
            } else if (failedWorkers.has(id)) {
                display = `<span style="color: #FF6B6B; text-decoration: line-through;">${id}</span>`;
            }
            return display;
        }).join(", ");
        document.getElementById("peerList").innerHTML = peerDisplay;
    } else {
        document.getElementById("peerList").innerHTML = "None";
    }

    // Update total peer count (including self)
    document.getElementById("totalPeers").innerText = peers.length;

    console.log("Worker updating peer list, others:", others);

    // Dial masters only; worker-to-worker channels serve nothing and the
    // simultaneous double-offer (glare) systematically failed anyway.
    others.forEach((peerId) => {
        if (!peerConnections[peerId] && isMasterPeer(peerId)) {
            console.log("Worker initiating connection to", peerId);
            createConnection(peerId);
        }
    });

    // Redraw topology and refresh health/channel counts (departures above
    // tear down channels silently, so the counters need a refresh here).
    drawNetwork(latestPeers);
    updateHealthStatus();
}

// Render resource allocation summary
function renderAllocation(payload) {
    latestAllocation = payload;
    const container = document.getElementById("allocationSummary");
    if (!container) return;

    const { allocation = {}, counts = {}, totalWorkers = 0 } = payload || {};
    const totalClients = (payload && (payload.totalClients ?? payload.totalMasters)) || 0;

    if (!totalClients) {
        container.innerHTML = '<div class="history-message">No masters/clients registered. All workers idle or awaiting tasks.</div>';
        return;
    }

    const rows = Object.keys(allocation).sort().map((clientId) => {
        const assigned = allocation[clientId] || [];
        const count = counts[clientId] || 0;
        const percentage = totalWorkers ? Math.round((count / totalWorkers) * 100) : 0;
        return `
            <div class="history-item">
                <div class="history-timestamp">Client: ${clientId}</div>
                <strong>Workers Assigned:</strong> ${count} / ${totalWorkers} (${percentage}%)<br>
                <strong>Worker IDs:</strong> ${assigned.length ? assigned.join(', ') : '—'}
            </div>
        `;
    }).join("");

    const header = `
        <div class="history-item">
            <div class="history-timestamp">Allocation Policy: Fair Share (Round-Robin)</div>
            <strong>Total Clients:</strong> ${totalClients} &nbsp; | &nbsp; <strong>Total Workers:</strong> ${totalWorkers}
        </div>
    `;

    container.innerHTML = header + rows;
}

function createConnection(peerId) {
    const pc = new RTCPeerConnection({
        iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
    });
    peerConnections[peerId] = pc;

    // Reliable, ordered channel for large transfers
    const dc = pc.createDataChannel("computation", {
        ordered: true
    });
    setupDataChannel(dc, peerId);

    pc.onicecandidate = (event) => {
        if (event.candidate) {
            safeSend({
                to: peerId,
                type: "candidate",
                candidate: event.candidate,
            });
        }
    };

    pc.ondatachannel = (event) => {
        console.log("Received remote data channel from", peerId);
        setupDataChannel(event.channel, peerId);
    };

    pc.createOffer()
        .then((offer) => {
            console.log("Created offer for", peerId);
            return pc.setLocalDescription(offer);
        })
        .then(() => {
            safeSend({
                to: peerId,
                type: "offer",
                offer: pc.localDescription,
            });
        })
        .catch(console.error);
}

function handleOffer(data) {
    const peerId = data.from;
    console.log("Received offer from", peerId);
    const pc = new RTCPeerConnection({
        iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
    });
    peerConnections[peerId] = pc;

    pc.onicecandidate = (event) => {
        if (event.candidate) {
            safeSend({
                to: peerId,
                type: "candidate",
                candidate: event.candidate,
            });
        }
    };

    pc.ondatachannel = (event) => {
        console.log("Setting up received data channel from", peerId);
        setupDataChannel(event.channel, peerId);
    };

    pc.setRemoteDescription(new RTCSessionDescription(data.offer))
        .then(() => pc.createAnswer())
        .then((answer) => {
            console.log("Created answer for", peerId);
            return pc.setLocalDescription(answer);
        })
        .then(() => {
            safeSend({
                to: peerId,
                type: "answer",
                answer: pc.localDescription,
            });
        })
        .catch(console.error);
}

function setupDataChannel(channel, peerId) {
    channel.binaryType = "arraybuffer";

    channel.onopen = () => {
        console.log("Data channel open with", peerId);
        connectedPeers[peerId] = true;
        if (failedWorkers.has(peerId)) {
            failedWorkers.delete(peerId);
            addFaultToleranceEvent("recovery", `Channel re-established with ${peerId}`);
        }
        updateStatus();
        updateHealthStatus();
    };

    channel.onclose = () => {
        console.log("Data channel closed with", peerId);
        addFaultToleranceEvent("info", `Data channel closed with ${peerId}`);
        teardownPeer(peerId);
        // A master that is still on signaling gets re-dialed once; if it left,
        // the peerList diff already released everything.
        setTimeout(() => {
            if (latestPeers.includes(peerId) && !peerConnections[peerId] && isMasterPeer(peerId)) {
                console.log("Re-dialing", peerId, "after channel loss");
                createConnection(peerId);
            }
        }, 2000);
    };

    channel.onerror = (event) => {
        console.error("Data channel error with", peerId, event.error || event);
        addFaultToleranceEvent("failure", `Data channel error with ${peerId}`);
        failedWorkers.add(peerId);
        updateStatus();
        updateHealthStatus();
    };

    channel.onmessage = async (event) => {
        if (typeof event.data === "string") {
            console.log("ℹ️ Ignoring non-protocol text message from", peerId);
            return;
        }
        let buf = event.data;
        if (typeof Blob !== "undefined" && buf instanceof Blob) {
            buf = await buf.arrayBuffer(); // Safari may deliver Blobs
        }
        const frame = decodeFrame(buf);
        if (!frame) {
            console.warn("⚠️ Dropped malformed frame from", peerId);
            return;
        }
        const payload = acceptFrame(frame, peerId);
        if (payload === null) return;
        if (frame.ftype === FRAME_TASK) {
            await handleTask(payload, channel);
        } else if (frame.ftype === FRAME_MODULE) {
            handleModulePayload(payload);
        } else {
            console.warn("⚠️ Unexpected frame type", frame.ftype, "from", peerId);
        }
    };

    dataChannels[peerId] = channel;
}

function updateStatus() {
    // This function is called when peer connections change
    // The total peer count is handled in updatePeerList
    if (latestPeers && latestPeers.length > 0) {
        drawNetwork(latestPeers);
    }
}

// Add task to compute history and update display
function addToComputeHistory(taskInfo) {
    const timestamp = new Date().toLocaleString();
    const historyItem = {
        timestamp: timestamp,
        ...taskInfo
    };

    computeHistory.unshift(historyItem); // Add to beginning of array

    // Keep only last 50 tasks to prevent memory issues
    if (computeHistory.length > 50) {
        computeHistory = computeHistory.slice(0, 50);
    }

    updateComputeHistoryDisplay();
}

// Update the compute history display
function updateComputeHistoryDisplay() {
    const historyContainer = document.getElementById("computeHistory");

    if (computeHistory.length === 0) {
        historyContainer.innerHTML = '<div class="history-message">No compute tasks received yet...</div>';
        return;
    }

    let historyHTML = '';
    computeHistory.forEach((item, index) => {
        const isLatest = index === 0 ? 'latest' : '';
        historyHTML += `
            <div class="history-item ${isLatest}">
                <div class="history-timestamp">${item.timestamp}</div>
                <strong>Task ID:</strong> ${item.taskId}<br>
                <strong>Data Chunk:</strong> [${item.dataChunk ? item.dataChunk.join(", ") : 'N/A'}]<br>
                <strong>Result:</strong> [${item.result ? item.result.join(", ") : 'N/A'}]<br>
                <strong>Mode:</strong> ${item.mode || 'N/A'}<br>
                <strong>Communication:</strong> ${item.communication || 'N/A'}
            </div>
        `;
    });

    historyContainer.innerHTML = historyHTML;

    // Auto-scroll to top to show latest task
    historyContainer.scrollTop = 0;
}

// ===== Network Topology Rendering =====

// Pointy-top hexagon vertex list for an SVG <polygon> (honeycomb cells).
function hexPoints(cx, cy, r) {
    const pts = [];
    for (let k = 0; k < 6; k++) {
        const a = (Math.PI / 180) * (-90 + 60 * k);
        pts.push((cx + r * Math.cos(a)).toFixed(1) + "," + (cy + r * Math.sin(a)).toFixed(1));
    }
    return pts.join(" ");
}

function drawNetwork(peers) {
    if (!networkSvg) return;

    // Clear SVG
    while (networkSvg.firstChild) networkSvg.removeChild(networkSvg.firstChild);

    const rect = networkSvg.getBoundingClientRect();
    const width = rect.width || 760;
    const height = rect.height || 320;
    const centerX = width / 2;
    const centerY = height / 2;
    const radius = Math.max(80, Math.min(width, height) / 2 - 40);

    // Calculate positions in a circle layout
    const positions = {}; // { id: {x,y} }
    const n = peers.length;
    if (n === 0) return;

    peers.forEach((id, index) => {
        const angle = (index / n) * Math.PI * 2 - Math.PI / 2;
        positions[id] = {
            x: centerX + radius * Math.cos(angle),
            y: centerY + radius * Math.sin(angle)
        };
    });

    // Draw only real edges: an amber line from this worker to each peer whose
    // data channel is actually open. No fabricated peer-to-peer mesh.
    const self = positions[myId];
    if (self) {
        peers.forEach((id) => {
            if (id === myId || !connectedPeers[id]) return;
            const b = positions[id];
            if (!b) return;
            const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
            line.setAttribute("x1", self.x);
            line.setAttribute("y1", self.y);
            line.setAttribute("x2", b.x);
            line.setAttribute("y2", b.y);
            line.setAttribute("stroke", "#f6b73c");
            line.setAttribute("stroke-width", "2");
            line.setAttribute("stroke-opacity", "0.7");
            networkSvg.appendChild(line);
        });
    }

    // Draw nodes as honeycomb cells (pointy-top hexagons).
    peers.forEach((id) => {
        const pos = positions[id];
        const isSelf = id === myId;
        const group = document.createElementNS("http://www.w3.org/2000/svg", "g");

        const r = isSelf ? 17 : 13;
        const hex = document.createElementNS("http://www.w3.org/2000/svg", "polygon");
        hex.setAttribute("points", hexPoints(pos.x, pos.y, r));

        // Default: an idle cell (charcoal fill, faint outline).
        let fill = "#ffffff10";
        let stroke = "#ffffff40";
        let strokeWidth = isSelf ? 3 : 2;

        if (failedWorkers.has(id)) {
            fill = "#ff6b6b33"; stroke = "#ff6b6b"; strokeWidth = 3;
        } else if (isSelf) {
            if (workerHealth === "failed") { fill = "#ff6b6b44"; stroke = "#ff6b6b"; }
            else if (workerHealth === "recovering" || workerHealth === "connecting") { fill = "#ffd93d33"; stroke = "#ffd93d"; }
            else { fill = "#f6b73c"; stroke = "#ffd27a"; } // healthy self: solid honey
        } else if (connectedPeers[id]) {
            fill = "#86e0a01f"; stroke = "#86e0a0"; // open data channel: green cell
        }

        hex.setAttribute("fill", fill);
        hex.setAttribute("stroke", stroke);
        hex.setAttribute("stroke-width", strokeWidth);
        // Peers known only via signaling (no open channel) render dashed.
        if (!isSelf && !connectedPeers[id] && !failedWorkers.has(id)) {
            hex.setAttribute("stroke-dasharray", "4 3");
        }

        const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
        label.setAttribute("x", pos.x);
        label.setAttribute("y", pos.y + (isSelf ? 32 : 28));
        label.setAttribute("fill", isSelf ? "#ffd27a" : "#9c927c");
        label.setAttribute("font-size", "11");
        label.setAttribute("text-anchor", "middle");
        label.setAttribute("font-family", '"SF Mono", "JetBrains Mono", Menlo, Consolas, monospace');
        label.textContent = id;

        group.appendChild(hex);
        group.appendChild(label);
        networkSvg.appendChild(group);
    });
}

// ===== Binary frame codec (mirrors distribute_runtime/src/protocol.rs) =====

// Frame: [magic u8][version u8][ftype u8][reserved u8]
//        [id_len u16 LE][transfer id][frame_seq u32 LE][total_frames u32 LE]
//        [payload_len u32 LE][payload]
function encodeFrame(ftype, transferId, seq, total, payload) {
    const idBytes = new TextEncoder().encode(transferId);
    const buf = new ArrayBuffer(4 + 2 + idBytes.length + 12 + payload.length);
    const view = new DataView(buf);
    const u8 = new Uint8Array(buf);
    u8[0] = FRAME_MAGIC;
    u8[1] = PROTO_VERSION;
    u8[2] = ftype;
    u8[3] = 0;
    view.setUint16(4, idBytes.length, true);
    u8.set(idBytes, 6);
    const pos = 6 + idBytes.length;
    view.setUint32(pos, seq, true);
    view.setUint32(pos + 4, total, true);
    view.setUint32(pos + 8, payload.length, true);
    u8.set(payload, pos + 12);
    return buf;
}

function decodeFrame(buf) {
    if (!(buf instanceof ArrayBuffer) || buf.byteLength < 4) return null;
    const u8 = new Uint8Array(buf);
    const view = new DataView(buf);
    if (u8[0] !== FRAME_MAGIC || u8[1] !== PROTO_VERSION) return null;
    const ftype = u8[2];
    const idLen = view.getUint16(4, true);
    let pos = 6 + idLen;
    if (buf.byteLength < pos + 12) return null;
    const transferId = new TextDecoder().decode(u8.subarray(6, 6 + idLen));
    const seq = view.getUint32(pos, true);
    const total = view.getUint32(pos + 4, true);
    const payloadLen = view.getUint32(pos + 8, true);
    if (buf.byteLength < pos + 12 + payloadLen) return null;
    const payload = u8.subarray(pos + 12, pos + 12 + payloadLen);
    return { ftype, transferId, seq, total, payload };
}

// Collect frames until a transfer completes; returns the payload or null.
// peerId lets teardownPeer drop a dead peer's half-received transfers.
function acceptFrame(frame, peerId) {
    if (frame.total === 0 || frame.seq >= frame.total) return null;
    let t = transfers[frame.transferId];
    if (!t) {
        t = transfers[frame.transferId] = {
            peerId,
            total: frame.total,
            parts: new Array(frame.total).fill(null),
            received: 0,
        };
    }
    if (t.total !== frame.total) {
        delete transfers[frame.transferId];
        return null;
    }
    if (t.parts[frame.seq] === null) {
        t.parts[frame.seq] = frame.payload.slice(); // copy out of the event buffer
        t.received++;
    }
    if (t.received < t.total) return null;
    delete transfers[frame.transferId];
    let size = 0;
    for (const p of t.parts) size += p.length;
    const out = new Uint8Array(size);
    let pos = 0;
    for (const p of t.parts) {
        out.set(p, pos);
        pos += p.length;
    }
    return out;
}

// Task payload: [u32 header_len][header json][u32 wasm_len][wasm]
//               [u32 glue_len][glue][u32 input_len][input]
function parseTaskPayload(bytes) {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    let pos = 0;
    const headerLen = view.getUint32(pos, true);
    pos += 4;
    const header = JSON.parse(new TextDecoder().decode(bytes.subarray(pos, pos + headerLen)));
    pos += headerLen;
    const wasmLen = view.getUint32(pos, true);
    pos += 4;
    const wasm = bytes.slice(pos, pos + wasmLen);
    pos += wasmLen;
    const glueLen = view.getUint32(pos, true);
    pos += 4;
    const glue = new TextDecoder().decode(bytes.subarray(pos, pos + glueLen));
    pos += glueLen;
    const inputLen = view.getUint32(pos, true);
    pos += 4;
    const input = bytes.slice(pos, pos + inputLen);
    return { header, wasm, glue, input };
}

// ===== Exec-worker pool =====
//
// WASM runs inside dedicated Web Workers so the main thread stays free to drive
// WebRTC, the dashboard, and reconnects during compute. This source is a string
// (the page is served from file://, where a separate .js worker file can't be
// loaded) turned into a classic Blob worker. It loads the wasm-bindgen glue via
// a data: URL because blob:null URLs are blocked inside workers on file://.
//
// NOTE: keep this code free of backticks and ${...} — it lives in a template
// literal on the page.
const EXEC_WORKER_SOURCE = `
'use strict';
const modules = {}; // module_hash -> Promise<glue namespace, initialized>

function glueDataUrl(glue) {
    // UTF-8 safe base64 of the glue text -> data: URL (importable in a worker).
    const bytes = new TextEncoder().encode(glue);
    let bin = '';
    for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
    return 'data:application/javascript;base64,' + btoa(bin);
}

function loadModule(hash, wasm, glue) {
    if (modules[hash]) return modules[hash];
    const p = (async () => {
        const ns = await import(glueDataUrl(glue));
        if (typeof ns.default === 'function') await ns.default(wasm);
        else if (typeof ns.init === 'function') await ns.init(wasm);
        else throw new Error('module ' + hash.slice(0, 12) + ' has no init export');
        return ns;
    })();
    // Rejection eviction: a failed load must not poison the cache forever, and
    // the page must be told so it re-sends the bytes next time.
    p.catch(() => {
        delete modules[hash];
        self.postMessage({ kind: 'moduleFailed', hash: hash });
    });
    modules[hash] = p;
    return p;
}

function toU8(result) {
    if (result instanceof Uint8Array) return result;
    if (ArrayBuffer.isView(result)) return new Uint8Array(result.buffer, result.byteOffset, result.byteLength);
    if (result instanceof ArrayBuffer) return new Uint8Array(result);
    throw new Error('map function returned ' + typeof result + ', expected bytes');
}

self.onmessage = async (e) => {
    const msg = e.data;
    if (msg.kind !== 'task') return;
    try {
        if (msg.module) loadModule(msg.module.hash, msg.module.wasm, msg.module.glue);
        if (!modules[msg.moduleHash]) {
            throw new Error('module ' + String(msg.moduleHash).slice(0, 12) + ' not loaded in exec worker');
        }
        const ns = await modules[msg.moduleHash];
        const fn = ns[msg.mapFunction];
        if (typeof fn !== 'function') throw new Error('map function ' + msg.mapFunction + ' not found in module');
        let out = fn(new Uint8Array(msg.input), msg.meta == null ? null : msg.meta);
        if (out instanceof Promise) out = await out;
        // .slice() detaches from WASM linear memory before we transfer the buffer.
        const copy = toU8(out).slice();
        self.postMessage({ kind: 'result', taskId: msg.taskId, ok: true, bytes: copy.buffer }, [copy.buffer]);
    } catch (err) {
        const m = String((err && err.message) || err);
        const fatal = (typeof WebAssembly !== 'undefined' && err instanceof WebAssembly.RuntimeError)
            || /unreachable|RuntimeError/i.test(m);
        self.postMessage({ kind: 'result', taskId: msg.taskId, ok: false, error: m, fatal: fatal });
    }
};
`;

function spawnExecWorker() {
    const blob = new Blob([EXEC_WORKER_SOURCE], { type: "application/javascript" });
    const objectUrl = URL.createObjectURL(blob);
    const worker = new Worker(objectUrl); // classic worker; data: URL glue import works inside
    const entry = { worker, objectUrl, inFlight: 0, loadedHashes: new Set() };
    worker.onmessage = (e) => handleExecMessage(entry, e.data);
    worker.onerror = (e) => {
        console.error("❌ Exec worker crashed:", e.message);
        respawnExecWorker(entry, "exec worker crashed: " + (e.message || "unknown"));
    };
    return entry;
}

// Least-loaded dispatch; grow the pool lazily up to EXEC_POOL_SIZE.
function pickExecEntry() {
    if (execPool.length < EXEC_POOL_SIZE) {
        const entry = spawnExecWorker();
        execPool.push(entry);
        return entry;
    }
    return execPool.reduce((a, b) => (b.inFlight < a.inFlight ? b : a));
}

function dispatchToExecWorker(header, channel, input) {
    const entry = pickExecEntry();
    const msg = {
        kind: "task",
        taskId: header.task_id,
        moduleHash: header.module_hash,
        mapFunction: header.map_function,
        meta: header.meta ?? null,
        input: input.buffer,
    };
    const transfer = [input.buffer]; // detaches our copy; capture length first
    const inputLen = input.length;
    if (!entry.loadedHashes.has(header.module_hash)) {
        const raw = rawModuleCache[header.module_hash];
        // Structured-clone copy (not transferred): other pool members may need it.
        msg.module = { hash: header.module_hash, wasm: raw.wasm, glue: raw.glue };
        entry.loadedHashes.add(header.module_hash); // optimistic; moduleFailed reverts
    }
    pendingExecTasks[header.task_id] = { header, channel, entry, inputLen, dispatchedAt: performance.now() };
    entry.inFlight++;
    tasksRunning++;
    updateHealthStatus();
    renderExecStats();
    entry.worker.postMessage(msg, transfer);
}

function handleExecMessage(entry, msg) {
    if (msg.kind === "moduleFailed") {
        entry.loadedHashes.delete(msg.hash); // force a re-send on the next task
        return;
    }
    if (msg.kind !== "result") return;
    const pending = pendingExecTasks[msg.taskId];
    if (!pending) return; // already resolved (e.g. by a respawn)
    delete pendingExecTasks[msg.taskId];
    entry.inFlight = Math.max(0, entry.inFlight - 1);
    tasksRunning = Math.max(0, tasksRunning - 1);

    const { header, channel, inputLen } = pending;
    if (msg.ok) {
        const result = new Uint8Array(msg.bytes);
        tasksCompleted++;
        lastTaskInfo = { fn: header.map_function, ms: Math.round(performance.now() - pending.dispatchedAt) };
        completionTimes.push(performance.now());
        updateHealthStatus();
        renderExecStats();
        addToComputeHistory({
            taskId: header.task_id,
            dataChunk: [inputLen],
            result: [result.length],
            mode: header.map_function,
            communication: "binary WebRTC",
        });
        sendResultFrame(channel, header, result);
    } else {
        tasksFailed++;
        addFaultToleranceEvent("failure", `Task ${header.task_id} failed: ${msg.error}`);
        updateHealthStatus();
        renderExecStats();
        sendErrorResultFrame(channel, header, msg.error);
        // A fatal wasm trap leaves the worker's instances unreliable; respawn it.
        if (msg.fatal) respawnExecWorker(entry, "exec worker hit a wasm trap");
    }
}

// Replace a dead/poisoned exec worker; fail its in-flight tasks back to the
// master (which reassigns them) so nothing hangs.
function respawnExecWorker(entry, reason) {
    const idx = execPool.indexOf(entry);
    if (idx === -1) return; // already handled
    execPool.splice(idx, 1);
    for (const taskId of Object.keys(pendingExecTasks)) {
        const p = pendingExecTasks[taskId];
        if (p.entry !== entry) continue;
        delete pendingExecTasks[taskId];
        tasksRunning = Math.max(0, tasksRunning - 1);
        tasksFailed++;
        addFaultToleranceEvent("failure", `Task ${p.header.task_id} lost (exec worker crashed), retrying`);
        sendErrorResultFrame(p.channel, p.header, reason);
    }
    try { entry.worker.terminate(); } catch (e) { /* already gone */ }
    try { URL.revokeObjectURL(entry.objectUrl); } catch (e) { /* already revoked */ }
    updateHealthStatus();
    renderExecStats();
    // A fresh worker spawns lazily on the next pickExecEntry (pool now under cap).
}

// Repaint the live compute-telemetry tiles (throughput, pool, last task).
function renderExecStats() {
    const now = (typeof performance !== "undefined" ? performance.now() : Date.now());
    completionTimes = completionTimes.filter((t) => now - t < 5000);
    const rate = completionTimes.length / 5;

    const tp = document.getElementById("throughput");
    if (tp) tp.textContent = rate.toFixed(1);

    const ps = document.getElementById("poolStatus");
    if (ps) {
        ps.textContent = execPool.length === 0
            ? "idle"
            : `${execPool.length}/${EXEC_POOL_SIZE} · ${Object.keys(rawModuleCache).length} mod`;
    }

    const lt = document.getElementById("lastTask");
    if (lt) lt.textContent = lastTaskInfo ? `${lastTaskInfo.fn} · ${lastTaskInfo.ms}ms` : "—";

    const rt = document.getElementById("runningTile");
    if (rt) rt.classList.toggle("live", tasksRunning > 0);
}

// ===== Shareable join link =====
function populateJoinInfo() {
    const link = document.getElementById("joinLink");
    if (link) link.value = location.href;
    const label = document.getElementById("serverLabel");
    if (label) label.textContent = SIGNALING_URL;
}

function copyJoinLink() {
    const el = document.getElementById("joinLink");
    if (!el) return;
    el.select();
    const flash = () => {
        const btn = document.getElementById("copyLinkBtn");
        if (!btn) return;
        const prev = btn.textContent;
        btn.textContent = "Copied";
        setTimeout(() => { btn.textContent = prev; }, 1200);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(el.value).then(flash).catch(() => { try { document.execCommand("copy"); flash(); } catch (e) {} });
    } else {
        try { document.execCommand("copy"); flash(); } catch (e) {}
    }
}

// ===== Module cache (raw bytes, content-hash keyed, pulled from the master) =====

// Module payload: [u32 hash_len][hash][u32 wasm_len][wasm][u32 glue_len][glue]
function handleModulePayload(bytes) {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    let pos = 0;
    const hashLen = view.getUint32(pos, true);
    pos += 4;
    const hash = new TextDecoder().decode(bytes.subarray(pos, pos + hashLen));
    pos += hashLen;
    const wasmLen = view.getUint32(pos, true);
    pos += 4;
    const wasm = bytes.slice(pos, pos + wasmLen);
    pos += wasmLen;
    const glueLen = view.getUint32(pos, true);
    pos += 4;
    const glue = new TextDecoder().decode(bytes.subarray(pos, pos + glueLen));

    console.log(`📦 Module ${hash.slice(0, 12)} received (${wasm.length} bytes)`);
    rawModuleCache[hash] = { wasm, glue };
    const wait = pendingModuleWaits[hash];
    if (wait) {
        clearTimeout(wait.timer);
        delete pendingModuleWaits[hash];
        wait.resolve(rawModuleCache[hash]);
    }
}

// Resolve a module's raw bytes by content hash, requesting it from the master if
// unseen. The cache survives across jobs and reconnects (content-addressed), so
// repeat jobs with an unchanged module transfer nothing.
function ensureRawModule(channel, moduleHash) {
    if (rawModuleCache[moduleHash]) return Promise.resolve(rawModuleCache[moduleHash]);
    const existing = pendingModuleWaits[moduleHash];
    if (existing) return existing.promise; // dedup concurrent tasks needing the same module

    let resolve, reject;
    const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
    const timer = setTimeout(() => {
        if (pendingModuleWaits[moduleHash]) {
            delete pendingModuleWaits[moduleHash];
            reject(new Error(`module ${moduleHash.slice(0, 12)} did not arrive within 30s`));
        }
    }, 30000);
    pendingModuleWaits[moduleHash] = { resolve, reject, timer, promise };

    console.log(`📡 Requesting module ${moduleHash.slice(0, 12)} from master`);
    const request = new TextEncoder().encode(
        JSON.stringify({ type: "need_module", module_hash: moduleHash })
    );
    sendPayloadFrames(channel, FRAME_CONTROL, "ctl_" + moduleHash.slice(0, 16), request).catch(
        (e) => console.error("❌ Failed to request module:", e)
    );
    return promise;
}

// ===== Task execution =====

// Result payload: [u32 header_len][header json][body bytes]
function buildResultPayload(header, body) {
    const headerBytes = new TextEncoder().encode(JSON.stringify(header));
    const out = new Uint8Array(4 + headerBytes.length + body.length);
    new DataView(out.buffer).setUint32(0, headerBytes.length, true);
    out.set(headerBytes, 4);
    out.set(body, 4 + headerBytes.length);
    return out;
}

// Send a payload as protocol frames with bufferedAmount backpressure.
async function sendPayloadFrames(channel, ftype, transferId, payload) {
    const total = Math.max(1, Math.ceil(payload.length / MAX_FRAME_PAYLOAD));
    for (let seq = 0; seq < total; seq++) {
        while (channel.bufferedAmount > BUFFERED_HIGH_WATER) {
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
        const start = seq * MAX_FRAME_PAYLOAD;
        const chunk = payload.subarray(start, Math.min(start + MAX_FRAME_PAYLOAD, payload.length));
        channel.send(encodeFrame(ftype, transferId, seq, total, chunk));
    }
}

// worker_id must be a string (the master's ResultHeader isn't Option); myId can
// be null if a reconnect reset the id while a task was still computing.
function sendResultFrame(channel, header, bytes) {
    const payload = buildResultPayload(
        { task_id: header.task_id, chunk_index: header.chunk_index, worker_id: myId || "unknown", meta: header.meta ?? null },
        bytes
    );
    sendPayloadFrames(channel, FRAME_RESULT, header.task_id, payload)
        .then(() => console.log(`✅ Result for ${header.task_id} sent (${bytes.length} bytes)`))
        .catch((e) => console.error("❌ Could not send result (channel gone?):", e));
}

function sendErrorResultFrame(channel, header, errMsg) {
    const payload = buildResultPayload(
        {
            task_id: header.task_id,
            chunk_index: header.chunk_index,
            worker_id: myId || "unknown",
            error: String(errMsg),
            meta: header.meta ?? null,
        },
        new Uint8Array(0)
    );
    sendPayloadFrames(channel, FRAME_RESULT, header.task_id, payload).catch((e) =>
        console.error("❌ Could not report task failure:", e)
    );
}

// Parse a task, make sure its module bytes are on hand, and hand it to the exec
// pool. Returns as soon as it is dispatched, so the channel keeps draining
// frames while compute runs off-thread. Results/errors are finished in
// handleExecMessage. Setup failures are reported so the master reassigns.
async function handleTask(payloadBytes, channel) {
    let header = null;
    try {
        const task = parseTaskPayload(payloadBytes);
        header = task.header;
        console.log(
            `🦀 Task ${header.task_id} (chunk ${header.chunk_index}, fn ${header.map_function}, ` +
            `input ${task.input.length} bytes, module ${(header.module_hash || "?").slice(0, 12)})`
        );

        // Tasks reference their module by content hash; inline module bytes
        // are accepted too (future small-module optimization).
        if (task.wasm.length > 0 && !rawModuleCache[header.module_hash]) {
            rawModuleCache[header.module_hash] = { wasm: task.wasm, glue: task.glue };
        }
        await ensureRawModule(channel, header.module_hash);
        dispatchToExecWorker(header, channel, task.input);
    } catch (error) {
        console.error("❌ Task setup failed:", error);
        tasksFailed++;
        if (header) {
            addFaultToleranceEvent("failure", `Task ${header.task_id} failed: ${error.message}`);
            sendErrorResultFrame(channel, header, String(error.message || error));
        }
        updateHealthStatus();
    }
}

// ===== Fault Tolerance Visualization Functions =====

function addFaultToleranceEvent(type, message) {
    const timestamp = new Date().toLocaleTimeString();
    const event = {
        type: type, // "failure", "reassignment", "recovery"
        message: message,
        timestamp: timestamp
    };

    faultToleranceEvents.unshift(event);

    // Keep only last 20 events
    if (faultToleranceEvents.length > 20) {
        faultToleranceEvents = faultToleranceEvents.slice(0, 20);
    }

    updateFaultToleranceEventsDisplay();
}

function updateFaultToleranceEventsDisplay() {
    const container = document.getElementById("faultToleranceEvents");
    if (!container) return;

    if (faultToleranceEvents.length === 0) {
        container.innerHTML = '<div class="history-message">No fault tolerance events yet...</div>';
        return;
    }

    const eventsHTML = faultToleranceEvents.map(event => {
        const icon = event.type === "failure" ? "❌" : event.type === "reassignment" ? "🔄" : event.type === "info" ? "ℹ️" : "✅";
        return `
            <div class="fault-event ${event.type}">
                <strong>${icon} ${event.timestamp}</strong><br>
                ${event.message}
            </div>
        `;
    }).join("");

    container.innerHTML = eventsHTML;
    container.scrollTop = 0;
}

function updateHealthStatus() {
    const healthIndicator = document.getElementById("healthIndicator");
    const healthStatus = document.getElementById("healthStatus");
    const connectionState = document.getElementById("connectionState");
    const tasksCompletedEl = document.getElementById("tasksCompleted");
    const tasksFailedEl = document.getElementById("tasksFailed");
    const tasksRunningEl = document.getElementById("tasksRunning");

    if (!healthIndicator || !healthStatus) return;

    // Health derives from the connection state machine plus real channel count
    const channelCount = Object.keys(connectedPeers).length;
    const openChannelsEl = document.getElementById("openChannels");

    switch (connState) {
        case "connecting":
            workerHealth = "connecting";
            healthIndicator.className = "health-indicator connecting";
            healthStatus.textContent = "Connecting";
            connectionState.textContent = "Signaling: connecting...";
            connectionState.style.color = "#87CEEB";
            break;
        case "online":
            workerHealth = "healthy";
            healthIndicator.className = "health-indicator healthy";
            healthStatus.textContent = channelCount > 0 ? "Healthy" : "Idle - waiting for master";
            connectionState.textContent = `Signaling: connected · Channels: ${channelCount}`;
            connectionState.style.color = "#90EE90";
            break;
        case "reconnecting":
            workerHealth = "recovering";
            healthIndicator.className = "health-indicator recovering";
            healthStatus.textContent = `Reconnecting (attempt ${reconnectAttempt})`;
            connectionState.textContent = "Signaling: down · Channels: 0";
            connectionState.style.color = "#FFD93D";
            break;
        case "down":
            workerHealth = "failed";
            healthIndicator.className = "health-indicator failed";
            healthStatus.textContent = "Failed (simulated)";
            connectionState.textContent = "Signaling: closed (simulated failure)";
            connectionState.style.color = "#FF6B6B";
            break;
    }

    if (openChannelsEl) openChannelsEl.textContent = channelCount;
    if (tasksCompletedEl) tasksCompletedEl.textContent = tasksCompleted;
    if (tasksFailedEl) tasksFailedEl.textContent = tasksFailed;
    if (tasksRunningEl) tasksRunningEl.textContent = tasksRunning;

    // Redraw network to show health status
    if (latestPeers && latestPeers.length > 0) {
        drawNetwork(latestPeers);
    }
}

// Kill this worker for real (channels + signaling), stay down for 3s, then
// rejoin through the normal reconnect path. Every message shown is true: the
// master genuinely loses this worker and the recovery event only fires once
// re-registration actually happened.
function simulateWorkerFailure() {
    if (connState !== "online") {
        console.log("Simulate ignored: worker is not online");
        return;
    }
    addFaultToleranceEvent("failure", "Simulated failure: closing data channels and signaling connection");
    intentionalClose = true;
    ws.close(); // handleWsClose tears down peers and parks us in "down"
    setTimeout(() => {
        addFaultToleranceEvent("info", "Simulated outage window over - reconnecting");
        forceReconnect();
    }, 3000);
}

// Initialize on load
function initWorker() {
    populateJoinInfo();
    updateHealthStatus();
    renderExecStats();
    // Repaint telemetry on a light cadence so throughput decays after a job
    // and the "running" tile reflects live state.
    setInterval(renderExecStats, 1000);
    connectSignaling();
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initWorker);
} else {
    initWorker();
}