// Worker Node - receives compute tasks and WASM modules from master via WebRTC
let ws = new WebSocket("ws://localhost:3000");
let myId;
let peerConnections = {}; // { peerId: RTCPeerConnection }
let dataChannels = {}; // { peerId: RTCDataChannel }
let connectedPeers = {}; // { peerId: true } when data channel is open
let computeHistory = []; // Store compute task history

// Chunk reassembly buffer: { chunk_id: { chunks: Map<index, data>, total_chunks, received_chunks } }
let chunkBuffers = {};

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

    const dc = pc.createDataChannel("computation", {
        ordered: true,
        maxRetransmits: 3
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
    };

    channel.onclose = () => {
        console.log("Data channel closed with", peerId);
        delete connectedPeers[peerId];
        updateStatus();
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
        console.log("🔧 Loading WASM module and executing map function...");

        // Decode base64 WASM module
        const wasmBytes = base64ToArrayBuffer(msg.wasm_module);

        // Load WASM module with JS glue
        const wasmModule = await loadSeparateWasmModule(wasmBytes, msg.js_glue);

        // Determine which WASM function to call
        const mapFunction = msg.map_function; // "cpu_map", "gpu_map", or "cpu1_map"
        console.log(`📊 Executing WASM function: ${mapFunction}`);

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
            result = await wasmModule.gpu_map(msg.data_chunk);
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

        console.log(`✅ WASM execution completed. Input: [${msg.data_chunk.join(',')}] -> Output: [${result.join(',')}]`);
        console.log(`   📊 Result type: ${typeof result}, Array: ${Array.isArray(result)}, Length: ${result?.length}`);

        // Convert Float32Array or other typed arrays to regular Array for JSON serialization
        const resultArray = Array.isArray(result) ? result : Array.from(result);
        console.log(`   🔄 Converted to Array: ${Array.isArray(resultArray)}`);

        // Add to compute history
        addToComputeHistory({
            taskId: msg.task_id,
            dataChunk: msg.data_chunk,
            result: resultArray,
            mode: mapFunction,
            communication: "WASM " + mapFunction
        });

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