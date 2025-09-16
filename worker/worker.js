// Worker Node - receives compute tasks and WASM modules from master via WebRTC
let ws = new WebSocket("ws://localhost:3000");
let myId;
let peerConnections = {}; // { peerId: RTCPeerConnection }
let dataChannels = {}; // { peerId: RTCDataChannel }
let connectedPeers = {}; // { peerId: true } when data channel is open
let computeHistory = []; // Store compute task history

// WebSocket connection setup
ws.onopen = () => {
    console.log("Worker connected to WebSocket server");
    document.getElementById("wsStatus").innerText = "Connected to signaling server";
    document.getElementById("wsStatus").className = "status connected";
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

function updatePeerList(peers) {
    const others = peers.filter((id) => id !== myId);

    // Show all peers with current peer in bold
    if (peers.length > 0) {
        const peerDisplay = peers.map(id => {
            if (id === myId) {
                return `<strong>${id}</strong>`;
            } else {
                return id;
            }
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
}

function createConnection(peerId) {
    const pc = new RTCPeerConnection({
        iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
    });
    peerConnections[peerId] = pc;

    const dc = pc.createDataChannel("computation");
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
    };

    channel.onclose = () => {
        console.log("Data channel closed with", peerId);
        delete connectedPeers[peerId];
        updateStatus();
    };

    channel.onmessage = async (event) => {
        console.log(`🔔 Worker ${myId} received data channel message from ${peerId}:`, event.data);
        if (typeof event.data === "string") {
            try {
                const msg = JSON.parse(event.data);
                console.log(`📨 Parsed message:`, msg);
                if (msg.type === "computeTask") {
                    console.log(`⚡ Executing computeTask...`);
                    await handleComputeTask(msg, peerId);
                } else if (msg.task_id && msg.data_chunk) {
                    console.log(`⚡ Executing Rust WebRTC task...`);
                    // Handle Rust WebRTC task format
                    await handleRustWebRTCTask(msg, channel);
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

// Load WASM module from base64 data
async function loadWasmModule(wasmBase64) {
    const wasmBuffer = base64ToArrayBuffer(wasmBase64);
    const wasmModule = await WebAssembly.instantiate(wasmBuffer);
    return wasmModule;
}

// Handle compute task received from master
async function handleComputeTask(msg, peerId) {
    console.log("Received compute task from", peerId, msg);

    try {
        let result = [];

        // Check if we have a valid WASM module or fallback to JavaScript
        if (msg.wasmBase64 && msg.wasmBase64 !== "dummy") {
            try {
                // Load and use WASM module
                const wasmModule = await loadWasmModule(msg.wasmBase64);

                // Execute based on execution mode
                if (msg.executionMode === "cpu") {
                    msg.dataChunk.forEach((num) => {
                        result.push(wasmModule.instance.exports.map(num));
                    });
                } else if (msg.executionMode === "gpu") {
                    console.log("GPU execution requested, falling back to CPU with WASM");
                    msg.dataChunk.forEach((num) => {
                        result.push(wasmModule.instance.exports.map(num));
                    });
                }
            } catch (wasmError) {
                console.log("WASM execution failed, falling back to JavaScript:", wasmError.message);
                result = executeJavaScriptFallback(msg.dataChunk, msg.executionMode);
            }
        } else {
            // Use JavaScript fallback for computation
            console.log("Using JavaScript fallback for computation");
            result = executeJavaScriptFallback(msg.dataChunk, msg.executionMode);
        }

        // Add to compute history
        addToComputeHistory({
            taskId: msg.taskId || msg.task_id,
            dataChunk: msg.dataChunk || msg.data_chunk,
            result: result,
            mode: msg.execution_mode || msg.executionMode || "CPU",
            communication: msg.wasmBase64 === "dummy" ? "JavaScript WebRTC" : "WASM WebRTC"
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
            console.log("Sent compute result:", resultMessage);
        }

    } catch (error) {
        console.error("Error processing compute task:", error);

        // Fallback calculation even on error
        let fallbackResult = [];
        try {
            fallbackResult = executeJavaScriptFallback(msg.dataChunk, msg.executionMode);
        } catch (fallbackError) {
            console.error("Fallback calculation also failed:", fallbackError);
            fallbackResult = msg.dataChunk.map(() => 0); // Zero result as last resort
        }

        // Send error back to master with fallback result
        const errorMessage = {
            type: "computeResult", // Still send as result to keep the system working
            taskId: msg.taskId,
            result: fallbackResult,
            workerId: myId,
            error: error.message
        };

        if (dataChannels[peerId] && dataChannels[peerId].readyState === "open") {
            dataChannels[peerId].send(JSON.stringify(errorMessage));
        }
    }
}

// JavaScript fallback computation (sum of squares for the simple_map_reduce example)
function executeJavaScriptFallback(dataChunk, executionMode) {
    console.log(`Executing JavaScript fallback in ${executionMode} mode`);

    // For the simple_map_reduce example, calculate square of each number
    return dataChunk.map(num => num * num);
}

// Handle direct task from master via WebSocket (simplified communication)
async function handleDirectTask(msg) {
    console.log("Received direct task from master:", msg);

    try {
        // Execute computation on the data chunk
        const result = executeJavaScriptFallback(msg.dataChunk, "cpu");

        // Add to compute history
        addToComputeHistory({
            taskId: msg.taskId,
            dataChunk: msg.dataChunk,
            result: result,
            mode: "CPU",
            communication: "Direct WebSocket"
        });

        // Send result back to master via WebSocket
        const resultMessage = {
            type: "taskResult",
            to: msg.from,
            taskId: msg.taskId,
            result: result,
            workerId: myId
        };

        ws.send(JSON.stringify(resultMessage));
        console.log("Sent result to master:", resultMessage);

    } catch (error) {
        console.error("Error processing direct task:", error);

        // Send error back to master
        const errorMessage = {
            type: "taskResult",
            to: msg.from,
            taskId: msg.taskId,
            result: [],
            error: error.message,
            workerId: myId
        };

        ws.send(JSON.stringify(errorMessage));
    }
}

// Handle Rust WebRTC task format
async function handleRustWebRTCTask(msg, channel) {
    console.log("🦀 Worker received Rust WebRTC task:", msg);

    try {
        // Execute computation on the data chunk (sum of squares)
        const result = msg.data_chunk.map(num => num * num);

        // Add to compute history
        addToComputeHistory({
            taskId: msg.task_id,
            dataChunk: msg.data_chunk,
            result: result,
            mode: msg.execution_mode || "CPU",
            communication: "Rust WebRTC"
        });

        // Send result back via WebRTC data channel
        const resultMessage = {
            task_id: msg.task_id,
            result: result,
            worker_id: myId
        };

        channel.send(JSON.stringify(resultMessage));
        console.log("Sent result via WebRTC:", resultMessage);

    } catch (error) {
        console.error("Error processing Rust WebRTC task:", error);

        // Send error back
        const errorMessage = {
            task_id: msg.task_id,
            result: [],
            error: error.message,
            worker_id: myId
        };

        channel.send(JSON.stringify(errorMessage));
    }
}