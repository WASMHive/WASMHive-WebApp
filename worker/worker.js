// Worker Node - receives compute tasks and WASM modules from master via WebRTC
let ws = new WebSocket("ws://localhost:3000");
let myId;
let peerConnections = {}; // { peerId: RTCPeerConnection }
let dataChannels = {}; // { peerId: RTCDataChannel }
let connectedPeers = {}; // { peerId: true } when data channel is open
let computeHistory = []; // Store compute task history
let moduleCache = {}; // { module_hash: Promise<wasm module> } persists across jobs
let pendingModuleWaits = {}; // { module_hash: { resolve, reject, timer } }
let latestPeers = []; // Cache latest peer list for redraws
let latestAllocation = null; // Cache latest allocation payload

// Fault tolerance tracking
let workerHealth = "healthy"; // "healthy", "failed", "recovering", "connecting"
let tasksCompleted = 0;
let tasksFailed = 0;
let faultToleranceEvents = []; // Store fault tolerance events
let failedWorkers = new Set(); // Track failed workers in network
let hasEverConnected = false; // Track if we've ever had a successful connection

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

// In-flight transfer reassembly: { transfer_id: { total, parts, received } }
let transfers = {};

// WebSocket connection setup
ws.onopen = () => {
    console.log("Worker connected to WebSocket server");
    document.getElementById("wsStatus").innerText = "Connected to signaling server";
    document.getElementById("wsStatus").className = "status connected";
    hasEverConnected = true;
    // Explicitly register as a worker for clarity on the signaling server
    try {
        ws.send(JSON.stringify({ type: "registerWorker" }));
    } catch (e) {
        console.error("Failed to register as worker:", e);
    }
    updateHealthStatus();
};

ws.onmessage = (message) => {
    const data = JSON.parse(message.data);
    console.log("Received WS message:", data);

    if (data.type === "welcome") {
        myId = data.id;
        document.getElementById("workerId").innerText = myId;
        if (data.peers) updatePeerList(data.peers);
    }
    if (data.type === "peerList") {
        if (myId) updatePeerList(data.peers);
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
};

ws.onclose = () => {
    console.log("Disconnected from WebSocket signaling server");
    const el = document.getElementById("wsStatus");
    if (el) {
        el.innerText = "Disconnected";
        el.className = "status disconnected";
    }
    addFaultToleranceEvent("failure", "WebSocket connection lost");
    updateHealthStatus();
};

ws.onerror = (error) => {
    console.error("WebSocket error:", error);
    const el = document.getElementById("wsStatus");
    if (el) {
        el.innerText = "Connection Error";
        el.className = "status disconnected";
    }
    addFaultToleranceEvent("failure", "WebSocket connection error");
    updateHealthStatus();
};

function updatePeerList(peers) {
    const others = peers.filter((id) => id !== myId);

    // Track which peers are no longer in the list (they may have failed)
    const currentPeerSet = new Set(peers);
    const previousPeerSet = new Set(latestPeers || []);

    // Mark peers that disappeared as failed
    previousPeerSet.forEach(peerId => {
        if (!currentPeerSet.has(peerId) && peerId !== myId) {
            failedWorkers.add(peerId);
            addFaultToleranceEvent("failure", `Peer ${peerId} disconnected from network`);
        }
    });

    // Remove peers that came back from failed list
    currentPeerSet.forEach(peerId => {
        if (failedWorkers.has(peerId) && peerId !== myId) {
            failedWorkers.delete(peerId);
            addFaultToleranceEvent("recovery", `Peer ${peerId} reconnected`);
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

    // Workers connect to all other peers (masters and workers)
    others.forEach((peerId) => {
        if (!peerConnections[peerId]) {
            console.log("Worker initiating connection to", peerId);
            createConnection(peerId);
        }
    });

    // Redraw topology
    drawNetwork(latestPeers);
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
            ws.send(
                JSON.stringify({
                    to: peerId,
                    type: "candidate",
                    candidate: event.candidate,
                }),
            );
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
            ws.send(
                JSON.stringify({
                    to: peerId,
                    type: "offer",
                    offer: pc.localDescription,
                }),
            );
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
            ws.send(
                JSON.stringify({
                    to: peerId,
                    type: "candidate",
                    candidate: event.candidate,
                }),
            );
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
            ws.send(
                JSON.stringify({
                    to: peerId,
                    type: "answer",
                    answer: pc.localDescription,
                }),
            );
        })
        .catch(console.error);
}

function setupDataChannel(channel, peerId) {
    channel.binaryType = "arraybuffer";

    channel.onopen = () => {
        console.log("Data channel open with", peerId);
        connectedPeers[peerId] = true;
        updateStatus();
        updateHealthStatus();
    };

    channel.onclose = () => {
        console.log("Data channel closed with", peerId);
        delete connectedPeers[peerId];
        addFaultToleranceEvent("failure", `Data channel closed with ${peerId}`);
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
        const payload = acceptFrame(frame);
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

    // Draw connections: interconnect all nodes with lines (undirected)
    for (let i = 0; i < peers.length; i++) {
        for (let j = i + 1; j < peers.length; j++) {
            const a = positions[peers[i]];
            const b = positions[peers[j]];
            if (!a || !b) continue;
            const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
            line.setAttribute("x1", a.x);
            line.setAttribute("y1", a.y);
            line.setAttribute("x2", b.x);
            line.setAttribute("y2", b.y);
            line.setAttribute("stroke", "#87CEEB");
            line.setAttribute("stroke-width", "1.5");
            line.setAttribute("stroke-opacity", "0.35");
            networkSvg.appendChild(line);
        }
    }

    // Draw nodes
    peers.forEach((id) => {
        const pos = positions[id];
        const isSelf = id === myId;
        const group = document.createElementNS("http://www.w3.org/2000/svg", "g");

        // Node circle
        const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
        circle.setAttribute("cx", pos.x);
        circle.setAttribute("cy", pos.y);
        circle.setAttribute("r", isSelf ? "16" : "12");
        circle.setAttribute("fill", isSelf ? "#90EE90" : "#ffffff22");
        circle.setAttribute("stroke", isSelf ? "#32CD32" : "#FFFFFF55");
        circle.setAttribute("stroke-width", isSelf ? "3" : "2");

        // Label
        const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
        label.setAttribute("x", pos.x);
        label.setAttribute("y", pos.y + (isSelf ? 30 : 26));
        label.setAttribute("fill", "#ffffffcc");
        label.setAttribute("font-size", "12");
        label.setAttribute("text-anchor", "middle");
        label.setAttribute("font-family", "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif");
        label.textContent = id;

        // Color code based on health status
        if (failedWorkers.has(id)) {
            circle.setAttribute("fill", "#FF6B6B");
            circle.setAttribute("stroke", "#FF4444");
            circle.setAttribute("stroke-width", "3");
        } else if (isSelf && workerHealth === "failed") {
            circle.setAttribute("fill", "#FF6B6B");
            circle.setAttribute("stroke", "#FF4444");
        } else if (isSelf && (workerHealth === "recovering" || workerHealth === "connecting")) {
            circle.setAttribute("fill", "#FFD93D");
            circle.setAttribute("stroke", "#FFB800");
        } else if (isSelf) {
            // Healthy - keep default green
            circle.setAttribute("fill", "#90EE90");
            circle.setAttribute("stroke", "#32CD32");
        }

        group.appendChild(circle);
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
function acceptFrame(frame) {
    if (frame.total === 0 || frame.seq >= frame.total) return null;
    let t = transfers[frame.transferId];
    if (!t) {
        t = transfers[frame.transferId] = {
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

// Load separate WASM module and JS glue
async function loadSeparateWasmModule(wasmBytes, jsGlue) {
    try {
        console.log("🔧 Loading separate WASM module and JS glue...");

        // Create a blob URL for the JS glue code
        const jsBlob = new Blob([jsGlue], { type: 'application/javascript' });
        const jsUrl = URL.createObjectURL(jsBlob);

        // Import the JS module
        const wasmModule = await import(jsUrl);

        // Initialize the WASM module with the raw bytes
        // Try different initialization methods based on wasm-bindgen version
        if (typeof wasmModule.default === 'function') {
            // Modern wasm-bindgen: default export is the init function
            await wasmModule.default(wasmBytes);
        } else if (typeof wasmModule.init === 'function') {
            // Alternative: init function exists
            await wasmModule.init(wasmBytes);
        } else {
            // Try manual WebAssembly instantiation
            const wasmWebAssembly = await WebAssembly.instantiate(wasmBytes);
            // This won't have the JS wrapper functions, so this approach won't work
            throw new Error("Unable to initialize WASM module - no suitable init method found");
        }

        console.log("✅ Separate WASM module loaded successfully");
        return wasmModule;
    } catch (error) {
        console.error("❌ Failed to load separate WASM module:", error);
        throw error;
    }
}

// ===== Module cache (content-hash keyed, pulled from the master on demand) =====

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
    const loading = loadSeparateWasmModule(wasm, glue);
    const wait = pendingModuleWaits[hash];
    if (wait) {
        clearTimeout(wait.timer);
        delete pendingModuleWaits[hash];
        loading.then(wait.resolve, wait.reject);
    } else {
        moduleCache[hash] = loading; // unsolicited but welcome
    }
}

// Resolve a module by content hash, requesting it from the master if this
// worker has never seen it. The cache survives across jobs, so repeat jobs
// with an unchanged module transfer nothing.
function ensureModule(channel, moduleHash) {
    let promise = moduleCache[moduleHash];
    if (promise) return promise;

    promise = new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            if (pendingModuleWaits[moduleHash]) {
                delete pendingModuleWaits[moduleHash];
                delete moduleCache[moduleHash];
                reject(new Error(`module ${moduleHash.slice(0, 12)} did not arrive within 30s`));
            }
        }, 30000);
        pendingModuleWaits[moduleHash] = { resolve, reject, timer };
    });
    moduleCache[moduleHash] = promise;

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

// Coerce whatever a map function returned into a Uint8Array.
function coerceToU8(result) {
    if (result instanceof Uint8Array) return result;
    if (ArrayBuffer.isView(result)) {
        return new Uint8Array(result.buffer, result.byteOffset, result.byteLength);
    }
    if (result instanceof ArrayBuffer) return new Uint8Array(result);
    throw new Error("map function returned " + typeof result + ", expected bytes");
}

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

// Execute one task: load/reuse the job's WASM module, call the named map
// function with (bytes, meta), send the returned bytes back. Any failure is
// reported to the master as an error result so the task can be reassigned.
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
        if (task.wasm.length > 0 && !moduleCache[header.module_hash]) {
            moduleCache[header.module_hash] = loadSeparateWasmModule(task.wasm, task.glue);
        }
        const wasmModule = await ensureModule(channel, header.module_hash);

        const mapFn = wasmModule[header.map_function];
        if (typeof mapFn !== "function") {
            throw new Error(`map function ${header.map_function} not found in module`);
        }

        let result = mapFn(task.input, header.meta ?? null);
        if (result instanceof Promise) result = await result;
        result = coerceToU8(result);

        tasksCompleted++;
        updateHealthStatus();
        addToComputeHistory({
            taskId: header.task_id,
            dataChunk: [task.input.length],
            result: [result.length],
            mode: header.map_function,
            communication: "binary WebRTC",
        });

        await sendPayloadFrames(
            channel,
            FRAME_RESULT,
            header.task_id,
            buildResultPayload(
                {
                    task_id: header.task_id,
                    chunk_index: header.chunk_index,
                    worker_id: myId,
                    meta: header.meta ?? null,
                },
                result
            )
        );
        console.log(`✅ Result for ${header.task_id} sent (${result.length} bytes)`);
    } catch (error) {
        console.error("❌ Task failed:", error);
        tasksFailed++;
        addFaultToleranceEvent("failure", `Task ${header ? header.task_id : "?"} failed: ${error.message}`);
        updateHealthStatus();
        if (header) {
            try {
                await sendPayloadFrames(
                    channel,
                    FRAME_RESULT,
                    header.task_id,
                    buildResultPayload(
                        {
                            task_id: header.task_id,
                            chunk_index: header.chunk_index,
                            worker_id: myId,
                            error: String(error.message || error),
                            meta: header.meta ?? null,
                        },
                        new Uint8Array(0)
                    )
                );
            } catch (sendError) {
                console.error("❌ Could not report task failure:", sendError);
            }
        }
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
        const icon = event.type === "failure" ? "❌" : event.type === "reassignment" ? "🔄" : "✅";
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

    if (!healthIndicator || !healthStatus) return;

    // Determine health based on connections
    const hasActiveConnections = Object.keys(connectedPeers).length > 0;
    const wsConnected = ws.readyState === WebSocket.OPEN;
    const wsConnecting = ws.readyState === WebSocket.CONNECTING;

    // If we've never connected, we're still in initial state (connecting/healthy)
    if (!hasEverConnected) {
        if (wsConnecting) {
            workerHealth = "connecting";
            healthIndicator.className = "health-indicator connecting";
            healthStatus.textContent = "Connecting";
            connectionState.textContent = "Connecting...";
            connectionState.style.color = "#87CEEB";
        } else if (wsConnected) {
            // Just connected for the first time
            hasEverConnected = true;
            workerHealth = "healthy";
            healthIndicator.className = "health-indicator healthy";
            healthStatus.textContent = "Healthy";
            connectionState.textContent = "Connected";
            connectionState.style.color = "#90EE90";
        } else {
            // Initial state - not connected yet but haven't failed
            workerHealth = "healthy";
            healthIndicator.className = "health-indicator healthy";
            healthStatus.textContent = "Initializing";
            connectionState.textContent = "Initializing...";
            connectionState.style.color = "#87CEEB";
        }
    } else {
        // We've had a connection before, so we can detect failures
        if (!wsConnected && !hasActiveConnections) {
            // Lost connection after having one - this is a failure
            if (workerHealth !== "recovering") {
                workerHealth = "failed";
                healthIndicator.className = "health-indicator failed";
                healthStatus.textContent = "Failed";
                connectionState.textContent = "Disconnected";
                connectionState.style.color = "#FF6B6B";
            }
        } else if (workerHealth === "recovering") {
            healthIndicator.className = "health-indicator recovering";
            healthStatus.textContent = "Recovering";
            connectionState.textContent = "Reconnecting";
            connectionState.style.color = "#FFD93D";
        } else {
            workerHealth = "healthy";
            healthIndicator.className = "health-indicator healthy";
            healthStatus.textContent = "Healthy";
            connectionState.textContent = "Connected";
            connectionState.style.color = "#90EE90";
        }
    }

    if (tasksCompletedEl) tasksCompletedEl.textContent = tasksCompleted;
    if (tasksFailedEl) tasksFailedEl.textContent = tasksFailed;

    // Redraw network to show health status
    if (latestPeers && latestPeers.length > 0) {
        drawNetwork(latestPeers);
    }
}

function simulateWorkerFailure() {
    if (workerHealth === "failed") {
        // Recovery simulation
        workerHealth = "recovering";
        addFaultToleranceEvent("recovery", "Worker recovery initiated...");

        setTimeout(() => {
            workerHealth = "healthy";
            addFaultToleranceEvent("recovery", "Worker recovered successfully!");
            updateHealthStatus();
        }, 2000);
    } else {
        // Failure simulation
        workerHealth = "failed";
        addFaultToleranceEvent("failure", "Worker failure simulated for demonstration");

        // Close all data channels to simulate failure
        Object.keys(dataChannels).forEach(peerId => {
            const channel = dataChannels[peerId];
            if (channel && channel.readyState === "open") {
                channel.close();
            }
        });

        // Close WebSocket connection
        if (ws.readyState === WebSocket.OPEN) {
            ws.close();
        }

        updateHealthStatus();

        // Show reassignment message after a delay
        setTimeout(() => {
            addFaultToleranceEvent("reassignment", "Tasks being reassigned to other workers...");
        }, 1000);
    }
}

// Initialize health status on load
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', updateHealthStatus);
} else {
    updateHealthStatus();
}