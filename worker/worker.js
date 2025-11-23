// Worker Node - receives compute tasks and WASM modules from master via WebRTC
let ws = new WebSocket("ws://localhost:3000");
let myId;
let peerConnections = {}; // { peerId: RTCPeerConnection }
let dataChannels = {}; // { peerId: RTCDataChannel }
let connectedPeers = {}; // { peerId: true } when data channel is open
let computeHistory = []; // Store compute task history
let wasmCache = {}; // { mapFunction: wasmModule }
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

// Chunk reassembly buffer: { chunk_id: { chunks: Map<index, data>, total_chunks, received_chunks } }
let chunkBuffers = {};

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
            pc.setRemoteDescription(
                new RTCSessionDescription(data.answer),
            ).catch(console.error);
        }
    }
    if (data.type === "candidate") {
        const pc = peerConnections[data.from];
        if (pc) {
            pc.addIceCandidate(
                new RTCIceCandidate(data.candidate),
            ).catch(console.error);
        }
    }
    if (data.type === "directTask") {
        // Handle direct task from master via WebSocket
        handleDirectTask(data);
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
        console.log(`🔔 Worker ${myId} received data channel message from ${peerId}`);
        console.log(`   📏 Message length: ${event.data.length} bytes`);
        console.log(`   📝 First 200 chars:`, event.data.substring(0, 200));

        if (typeof event.data === "string") {
            try {
                const msg = JSON.parse(event.data);
                console.log(`   ✅ Parsed JSON successfully`);
                console.log(`   🔑 Message keys:`, Object.keys(msg));

                // Check if this is a chunk message
                if (msg.chunk_id !== undefined && msg.chunk_index !== undefined && msg.total_chunks !== undefined) {
                    console.log(`📦 Received chunk ${msg.chunk_index + 1}/${msg.total_chunks} for ${msg.chunk_id}`);

                    // Initialize buffer for this chunk_id if needed
                    if (!chunkBuffers[msg.chunk_id]) {
                        chunkBuffers[msg.chunk_id] = {
                            chunks: new Map(),
                            total_chunks: msg.total_chunks,
                            received_chunks: 0
                        };
                    }

                    // Store this chunk
                    const buffer = chunkBuffers[msg.chunk_id];
                    buffer.chunks.set(msg.chunk_index, msg.data);
                    buffer.received_chunks++;

                    console.log(`   📊 Buffer status: ${buffer.received_chunks}/${buffer.total_chunks} chunks received`);

                    // Check if all chunks are received
                    if (buffer.received_chunks === buffer.total_chunks) {
                        console.log(`✅ All chunks received for ${msg.chunk_id}, reassembling...`);

                        // Reassemble chunks in order
                        let fullData = '';
                        for (let i = 0; i < buffer.total_chunks; i++) {
                            const chunkData = buffer.chunks.get(i);
                            if (!chunkData) {
                                console.error(`❌ Missing chunk ${i} for ${msg.chunk_id}`);
                                delete chunkBuffers[msg.chunk_id];
                                return;
                            }
                            // Decode base64 chunk
                            const decodedChunk = atob(chunkData);
                            fullData += decodedChunk;
                        }

                        console.log(`   📏 Reassembled message: ${fullData.length} bytes`);

                        // Clean up chunk buffer
                        delete chunkBuffers[msg.chunk_id];

                        // Parse and process the reassembled message
                        try {
                            const reassembledMsg = JSON.parse(fullData);
                            console.log(`📨 Parsed reassembled message`);

                            // Process the reassembled message as normal
                            if (reassembledMsg.type === "computeTask") {
                                console.log(`⚡ Executing computeTask (OLD FORMAT)...`);
                                await handleComputeTask(reassembledMsg, peerId);
                            } else if (reassembledMsg.task_id && reassembledMsg.data_chunk) {
                                console.log(`⚡ Executing Rust WebRTC task (NEW FORMAT)...`);
                                await handleRustWebRTCTask(reassembledMsg, channel);
                            } else if (reassembledMsg.task_id && reassembledMsg.data_chunk_b64) {
                                console.log(`🧪 Executing byte-based Rust WebRTC task (BINARY FORMAT)...`);
                                await handleRustWebRTCTaskBytes(reassembledMsg, channel);
                            } else {
                                console.log(`❓ Unknown reassembled message type:`, reassembledMsg);
                            }
                        } catch (e) {
                            console.error("❌ Error parsing reassembled message:", e);
                        }
                    }

                    return; // Chunk message handled
                }

                // Not a chunk message, handle normally
                console.log(`📨 Parsed message:`, msg);
                console.log(`🔍 Debug - msg.type: "${msg.type}", msg.task_id: "${msg.task_id}", msg.data_chunk exists: ${!!msg.data_chunk}, msg.map_function: "${msg.map_function}"`);

                if (msg.type === "computeTask") {
                    console.log(`⚡ Executing computeTask (OLD FORMAT)...`);
                    await handleComputeTask(msg, peerId);
                } else if (msg.task_id && msg.data_chunk) {
                    console.log(`⚡ Executing Rust WebRTC task (NEW FORMAT)...`);
                    // Handle Rust WebRTC task format
                    await handleRustWebRTCTask(msg, channel);
                } else if (msg.task_id && msg.data_chunk_b64) {
                    console.log(`🧪 Executing byte-based Rust WebRTC task (BINARY FORMAT)...`);
                    await handleRustWebRTCTaskBytes(msg, channel);
                } else {
                    console.log(`❓ Unknown message type:`, msg);
                }
            } catch (e) {
                console.error("❌ Error parsing message:", e);
            }
        } else {
            console.log(`📦 Received non-string data:`, typeof event.data);
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

// Helper: Convert Base64 to ArrayBuffer
function base64ToArrayBuffer(base64) {
    const binary_string = window.atob(base64);
    const len = binary_string.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
        bytes[i] = binary_string.charCodeAt(i);
    }
    return bytes.buffer;
}

// Helper: Convert Uint8Array to Base64 without stack overflow
function uint8ToBase64(bytes) {
    const chunkSize = 0x8000; // 32KB per chunk
    let binary = '';
    for (let i = 0; i < bytes.length; i += chunkSize) {
        const sub = bytes.subarray(i, i + chunkSize);
        binary += String.fromCharCode.apply(null, sub);
    }
    return btoa(binary);
}

// Encode RGBA bytes to PNG (base64) using Canvas/OffscreenCanvas
async function rgbaToPngBase64(rgbaBytes, width, height) {
    try {
        // Validate buffer size
        const expectedSize = width * height * 4;
        if (rgbaBytes.length !== expectedSize) {
            console.warn(`⚠️ Buffer size mismatch: expected ${expectedSize}, got ${rgbaBytes.length}`);
            // Try to adjust if buffer is larger (might have padding)
            if (rgbaBytes.length > expectedSize) {
                rgbaBytes = rgbaBytes.slice(0, expectedSize);
            } else {
                throw new Error(`Buffer too small: ${rgbaBytes.length} < ${expectedSize}`);
            }
        }

        let canvas;
        let ctx;
        if (typeof OffscreenCanvas !== 'undefined') {
            canvas = new OffscreenCanvas(width, height);
            ctx = canvas.getContext('2d');
        } else {
            const c = document.createElement('canvas');
            c.width = width;
            c.height = height;
            canvas = c;
            ctx = canvas.getContext('2d');
        }

        const imageData = new ImageData(width, height);
        imageData.data.set(rgbaBytes);
        ctx.putImageData(imageData, 0, 0);

        const blob = await new Promise((resolve) => {
            if (canvas.convertToBlob) {
                canvas.convertToBlob({ type: 'image/png' }).then(resolve);
            } else {
                canvas.toBlob(resolve, 'image/png');
            }
        });
        const arrayBuffer = await blob.arrayBuffer();
        return uint8ToBase64(new Uint8Array(arrayBuffer));
    } catch (e) {
        console.error('PNG encode failed, returning raw base64 as fallback', e);
        return uint8ToBase64(rgbaBytes);
    }
}

// Helper: Convert Uint8Array to Base64 without stack overflow
function uint8ToBase64(bytes) {
    const chunkSize = 0x8000; // 32KB per chunk
    let binary = '';
    for (let i = 0; i < bytes.length; i += chunkSize) {
        const sub = bytes.subarray(i, i + chunkSize);
        binary += String.fromCharCode.apply(null, sub);
    }
    return btoa(binary);
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

// Handle compute task received from master
async function handleComputeTask(msg, peerId) {
    console.log("Received compute task from", peerId, msg);

    try {
        console.log("🦀 Executing separate WASM computation...");

        // Load separate WASM module and JS glue
        const wasmModule = await loadSeparateWasmModule(new Uint8Array(msg.wasm_module), msg.js_glue);

        let result;

        // NEW FORMAT: Use map_function field directly (preferred)
        if (msg.map_function) {
            console.log(`🔄 Using NEW format with map_function: ${msg.map_function}`);
            const mapFunction = msg.map_function; // "cpu_map", "gpu_map", or "cpu1_map"

            if (mapFunction === "cpu_map") {
                // Execute cpu_map on each element
                result = [];
                for (const num of msg.data_chunk || msg.dataChunk) {
                    const mapped = wasmModule.cpu_map(num);
                    result.push(mapped);
                }
            } else if (mapFunction === "gpu_map") {
                // Execute gpu_map with batch async WebGPU processing
                const gpuResult = await wasmModule.gpu_map(msg.data_chunk || msg.dataChunk);
                // Convert Float32Array to regular Array for JSON serialization
                result = Array.isArray(gpuResult) ? gpuResult : Array.from(gpuResult);
            } else if (mapFunction === "cpu1_map") {
                // Execute cpu1_map on each element (x³)
                result = [];
                for (const num of msg.data_chunk || msg.dataChunk) {
                    const mapped = wasmModule.cpu1_map(num);
                    result.push(mapped);
                }
            } else {
                throw new Error(`Unknown map function: ${mapFunction}`);
            }

            console.log(`✅ NEW format WASM execution completed. Input: [${(msg.data_chunk || msg.dataChunk).join(',')}] -> Output: [${result.join(',')}]`);
            console.log(`   📊 Result type: ${typeof result}, Array: ${Array.isArray(result)}, Length: ${result?.length}`);
        } else {
            // OLD FORMAT: Legacy execution mode logic (fallback)
            console.log(`⚠️ Using OLD format with execution_mode`);
            const executionMode = msg.execution_mode || msg.executionMode || "CPU";
            const baseFunctionName = msg.function_name || msg.functionName || "simple_map_reduce";
            let functionName;

            if (executionMode === "GPU") {
                functionName = `${baseFunctionName}_gpu_wasm`;

                if (!wasmModule[functionName]) {
                    throw new Error(`Function ${functionName} not found in WASM module`);
                }

                // Prepare input for GPU WASM function (expects JSON string)
                const input = {
                    numbers: msg.data_chunk || msg.dataChunk,
                    execution_mode: executionMode
                };
                const inputJson = JSON.stringify(input);

                // Call GPU WASM function (async) and parse result
                const resultJson = await wasmModule[functionName](inputJson);
                const parsedResult = JSON.parse(resultJson);
                result = parsedResult.value;
            } else {
                // CPU execution
                functionName = `${baseFunctionName}_wasm`;

                if (!wasmModule[functionName]) {
                    throw new Error(`Function ${functionName} not found in WASM module`);
                }

                // Prepare input for CPU WASM function (expects JSON string)
                const input = {
                    numbers: msg.data_chunk || msg.dataChunk,
                    execution_mode: executionMode
                };
                const inputJson = JSON.stringify(input);

                // Call CPU WASM function and parse result
                const resultJson = wasmModule[functionName](inputJson);
                const parsedResult = JSON.parse(resultJson);
                result = parsedResult.value;
            }
        }

        console.log("✅ Separate WASM execution completed successfully");

        // Add to compute history
        addToComputeHistory({
            taskId: msg.taskId || msg.task_id,
            dataChunk: msg.dataChunk || msg.data_chunk,
            result: result,
            mode: msg.execution_mode || msg.executionMode || "CPU",
            communication: "Separate WASM"
        });

        // Send result back to master
        const resultMessage = {
            type: "computeResult",
            taskId: msg.taskId,
            result: result,
            workerId: myId
        };

        if (dataChannels[peerId] && dataChannels[peerId].readyState === "open") {
            dataChannels[peerId].send(JSON.stringify(resultMessage));
            console.log("✅ Sent compute result:", resultMessage);
        }

    } catch (error) {
        console.error("❌ Error processing compute task:", error);

        // Send error back to master (no fallback - require WASM)
        const errorMessage = {
            type: "computeResult",
            taskId: msg.taskId,
            result: [],
            workerId: myId,
            error: error.message
        };

        if (dataChannels[peerId] && dataChannels[peerId].readyState === "open") {
            dataChannels[peerId].send(JSON.stringify(errorMessage));
        }
    }
}

// Handle direct task from master via WebSocket (legacy - should use WebRTC)
async function handleDirectTask(msg) {
    console.log("⚠️ Received direct task via WebSocket - this should use WebRTC instead");

    // Send error back indicating WebRTC should be used
    const errorMessage = {
        type: "taskResult",
        to: msg.from,
        taskId: msg.taskId,
        result: [],
        error: "Direct WebSocket tasks not supported - use WebRTC with WASM",
        workerId: myId
    };

    ws.send(JSON.stringify(errorMessage));
}

// Handle Rust WebRTC task format
async function handleRustWebRTCTask(msg, channel) {
    console.log("🦀 Worker received Rust WebRTC task:", msg);

    try {
        // Determine which WASM function to call
        const mapFunction = msg.map_function; // "cpu_map", "gpu_map", or "cpu1_map"
        console.log(`📊 Executing WASM function: ${mapFunction}`);

        // Load or reuse WASM module (optimization: cache WASM to avoid reloading)
        let wasmModule = wasmCache[mapFunction];
        if (!wasmModule && msg.wasm_module && msg.wasm_module.length > 0) {
            console.log("🔧 Loading WASM module and executing map function...");
            // Decode base64 WASM module
            const wasmBytes = base64ToArrayBuffer(msg.wasm_module);
            // Load WASM module with JS glue
            wasmModule = await loadSeparateWasmModule(wasmBytes, msg.js_glue);
            // Cache the loaded module
            wasmCache[mapFunction] = wasmModule;
            console.log("✅ WASM module loaded and cached");
        } else if (wasmModule) {
            console.log("♻️  Reusing cached WASM module");
        } else {
            throw new Error(`No WASM module provided and no cached module for ${mapFunction}`);
        }

        let result;

        if (mapFunction === "cpu_map") {
            // Execute cpu_map on each element
            result = [];
            for (const num of msg.data_chunk) {
                const mapped = wasmModule.cpu_map(num);
                result.push(mapped);
            }
        } else if (mapFunction === "gpu_map") {
            // Execute gpu_map with batch async WebGPU processing
            // Check if WebGPU is available first
            if (typeof navigator !== 'undefined' && navigator.gpu) {
                try {
                    result = await wasmModule.gpu_map(msg.data_chunk);
                } catch (gpuError) {
                    console.error("❌ GPU execution failed, falling back to CPU:", gpuError);
                    console.error("   Error details:", gpuError.message, gpuError.stack);
                    // Fallback to CPU if GPU fails
                    result = [];
                    for (const num of msg.data_chunk) {
                        const mapped = wasmModule.cpu_map(num);
                        result.push(mapped);
                    }
                    console.log("✅ Fallback to CPU completed");
                }
            } else {
                console.warn("⚠️ WebGPU not available, using CPU fallback");
                // WebGPU not available, use CPU
                result = [];
                for (const num of msg.data_chunk) {
                    const mapped = wasmModule.cpu_map(num);
                    result.push(mapped);
                }
                console.log("✅ CPU execution completed (WebGPU unavailable)");
            }
        } else if (mapFunction === "cpu1_map") {
            // Execute cpu1_map on each element (x³)
            result = [];
            for (const num of msg.data_chunk) {
                const mapped = wasmModule.cpu1_map(num);
                result.push(mapped);
            }
        } else {
            throw new Error(`Unknown map function: ${mapFunction}`);
        }

        // Validate result before proceeding
        if (result === undefined || result === null) {
            throw new Error("WASM function returned undefined or null result");
        }

        console.log(`✅ WASM execution completed. Input: [${msg.data_chunk.join(',')}] -> Output: [${result?.join ? result.join(',') : 'invalid'}]`);
        console.log(`   📊 Result type: ${typeof result}, Array: ${Array.isArray(result)}, Length: ${result?.length}`);

        // Convert Float32Array or other typed arrays to regular Array for JSON serialization
        let resultArray;
        if (Array.isArray(result)) {
            resultArray = result;
        } else if (result && typeof result.length === 'number') {
            // Typed array (Float32Array, etc.)
            resultArray = Array.from(result);
        } else {
            throw new Error(`Invalid result type from WASM function: ${typeof result}`);
        }
        console.log(`   🔄 Converted to Array: ${Array.isArray(resultArray)}, Length: ${resultArray.length}`);

        // Add to compute history
        addToComputeHistory({
            taskId: msg.task_id,
            dataChunk: msg.data_chunk,
            result: resultArray,
            mode: mapFunction,
            communication: "WASM " + mapFunction
        });

        // Update fault tolerance stats
        tasksCompleted++;
        updateHealthStatus();

        // Send result back via WebRTC data channel
        const resultMessage = {
            task_id: msg.task_id,
            result: resultArray,
            worker_id: myId
        };

        console.log("📤 Preparing to send result back...");
        console.log("   🔍 Result message:", resultMessage);
        console.log("   🔍 Channel state:", channel.readyState);

        try {
            channel.send(JSON.stringify(resultMessage));
            console.log("✅ Sent WASM result via WebRTC successfully!");
        } catch (e) {
            console.error("❌ Failed to send result:", e);
        }

    } catch (error) {
        console.error("❌ Error processing WASM task:", error);
        console.error("   Error stack:", error.stack);

        // Update fault tolerance stats
        tasksFailed++;
        addFaultToleranceEvent("failure", `Task ${msg.task_id} failed: ${error.message}`);
        updateHealthStatus();

        // Send error back in format that matches WorkerResult enum
        // Must match Floats variant: { task_id, result, worker_id }
        // Send empty result array to indicate failure
        const errorMessage = {
            task_id: msg.task_id,
            result: [], // Empty result indicates failure
            worker_id: myId
        };

        try {
            channel.send(JSON.stringify(errorMessage));
            console.log("✅ Sent error result back to master");
        } catch (sendError) {
            console.error("❌ Failed to send error result:", sendError);
        }
    }
}

// Handle byte-based Rust WebRTC task format (e.g., video frames)
async function handleRustWebRTCTaskBytes(msg, channel) {
    console.log("🦀 Worker received Rust WebRTC BYTE task:", msg);
    console.log("   📏 Meta:", msg.meta);

    try {
        console.log("🔧 Loading WASM module and executing byte map function...");

        // Load or reuse WASM module if needed
        const mapFunction = msg.map_function;
        let wasmModule = wasmCache[mapFunction];
        if (!wasmModule && mapFunction !== 'grayscale_frame_js') {
            const wasmBytes = base64ToArrayBuffer(msg.wasm_module || "");
            if (wasmBytes && wasmBytes.byteLength > 0) {
                wasmModule = await loadSeparateWasmModule(wasmBytes, msg.js_glue || "");
                wasmCache[mapFunction] = wasmModule;
            }
        }

        // Decode data chunk
        const chunkArrayBuffer = base64ToArrayBuffer(msg.data_chunk_b64);
        const chunkBytes = new Uint8Array(chunkArrayBuffer);

        // Determine function
        if (mapFunction !== 'grayscale_frame_js' && (!wasmModule || !wasmModule[mapFunction])) {
            throw new Error(`Unknown byte map function: ${mapFunction}`);
        }

        let resultBytes;
        if (mapFunction === 'grayscale_frame_js') {
            // JS fallback grayscale: in-place over RGBA
            resultBytes = chunkBytes.slice();
            for (let i = 0; i + 3 < resultBytes.length; i += 4) {
                const r = resultBytes[i];
                const g = resultBytes[i + 1];
                const b = resultBytes[i + 2];
                const gray = (0.299 * r + 0.587 * g + 0.114 * b) | 0;
                resultBytes[i] = gray;
                resultBytes[i + 1] = gray;
                resultBytes[i + 2] = gray;
            }
        } else {
            // Try calling with (bytes, meta); extra args are ignored if not needed
            const possible = wasmModule[mapFunction](chunkBytes, msg.meta);
            if (possible instanceof Promise) {
                resultBytes = await possible;
            } else {
                resultBytes = possible;
            }
        }

        // Ensure Uint8Array
        if (!(resultBytes instanceof Uint8Array)) {
            if (ArrayBuffer.isView(resultBytes)) {
                resultBytes = new Uint8Array(resultBytes.buffer);
            } else if (resultBytes instanceof ArrayBuffer) {
                resultBytes = new Uint8Array(resultBytes);
            } else {
                throw new Error("Byte map function did not return a byte buffer");
            }
        }

        // Encode result as PNG base64 to reduce payload size (only for image data)
        // For non-image data (like text), encode directly as base64
        const w = (msg.meta && msg.meta.width) || 0;
        const h = (msg.meta && msg.meta.height) || 0;
        let resultB64;
        if (w > 0 && h > 0) {
            // Image data: encode as PNG
            console.log(`   🖼️  Encoding PNG: ${w}x${h}, bytes: ${resultBytes.length}`);
            resultB64 = await rgbaToPngBase64(resultBytes, w, h);
            console.log(`   📦 PNG base64 length: ${resultB64.length}`);
        } else {
            // Non-image data: encode directly as base64
            console.log(`   📦 Encoding raw bytes as base64: ${resultBytes.length} bytes`);
            resultB64 = uint8ToBase64(resultBytes);
            console.log(`   📦 Base64 length: ${resultB64.length}`);
        }

        // Send result back with meta echoed
        const resultMessage = {
            task_id: msg.task_id,
            result_b64: resultB64,
            worker_id: myId,
            meta: Object.assign({}, msg.meta || null, { format: (w > 0 && h > 0) ? 'png' : 'raw' }),
        };

        // Wait for send queue to drain before sending large result
        const resultJson = JSON.stringify(resultMessage);
        const maxBuffered = 16 * 1024 * 1024; // 16MB
        while (channel.bufferedAmount > maxBuffered) {
            console.log(`   ⏳ Waiting for send queue to drain (${channel.bufferedAmount} bytes buffered)...`);
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        try {
            channel.send(resultJson);
            console.log("✅ Sent BYTE result via WebRTC successfully!");
            addToComputeHistory({
                taskId: msg.task_id,
                dataChunk: [],
                result: [resultBytes.length],
                mode: mapFunction,
                communication: "BYTE WASM",
            });

            // Update fault tolerance stats
            tasksCompleted++;
            updateHealthStatus();
        } catch (e) {
            console.error("❌ Failed to send BYTE result:", e);
        }

    } catch (error) {
        console.error("❌ Error processing BYTE WASM task:", error);
        const errorMessage = {
            task_id: msg.task_id,
            result_b64: "",
            error: error.message,
            worker_id: myId,
            meta: msg.meta || null,
        };
        channel.send(JSON.stringify(errorMessage));
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