// Worker Node - receives compute tasks and WASM modules from clients via WebRTC
let ws = new WebSocket("ws://localhost:3000");
let myId;
let peerConnections = {}; // { peerId: RTCPeerConnection }
let dataChannels = {}; // { peerId: RTCDataChannel }
let connectedPeers = {}; // { peerId: true } when data channel is open
let computeHistory = []; // Store compute task history
let latestPeers = []; // Cache latest peer list for redraws

// SVG topology elements
const networkSvg = document.getElementById("networkSvg");

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
    latestPeers = peers.slice();

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

    // Workers connect to all other peers (clients and workers)
    others.forEach((peerId) => {
        if (!peerConnections[peerId]) {
            console.log("Worker initiating connection to", peerId);
            createConnection(peerId);
        }
    });

	// Redraw topology
	drawNetwork(latestPeers);
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
        console.log(`🔔 Worker ${myId} received data channel message from ${peerId}:`, event.data);
        if (typeof event.data === "string") {
            try {
                const msg = JSON.parse(event.data);
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

		group.appendChild(circle);
		group.appendChild(label);
		networkSvg.appendChild(group);
	});
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

// Handle compute task received from client
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
            const mapFunction = msg.map_function; // "cpu_map" or "gpu_map"

            if (mapFunction === "cpu_map") {
                // Execute cpu_map on each element
                result = [];
                for (const num of msg.data_chunk || msg.dataChunk) {
                    const mapped = wasmModule.cpu_map(num);
                    result.push(mapped);
                }
            } else if (mapFunction === "gpu_map") {
                // Execute gpu_map on each element (same as cpu_map for consistency)
                result = [];
                for (const num of msg.data_chunk || msg.dataChunk) {
                    const mapped = wasmModule.gpu_map(num);
                    result.push(mapped);
                }
            } else {
                throw new Error(`Unknown map function: ${mapFunction}`);
            }

            console.log(`✅ NEW format WASM execution completed. Input: [${(msg.data_chunk || msg.dataChunk).join(',')}] -> Output: [${result.join(',')}]`);
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

        // Send result back to client
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

        // Send error back to client (no fallback - require WASM)
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

// Handle direct task from client via WebSocket (legacy - should use WebRTC)
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
        const mapFunction = msg.map_function; // "cpu_map" or "gpu_map"
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
            // Execute gpu_map on each element (same as cpu_map for consistency)
            result = [];
            for (const num of msg.data_chunk) {
                const mapped = wasmModule.gpu_map(num);
                result.push(mapped);
            }
        } else {
            throw new Error(`Unknown map function: ${mapFunction}`);
        }

        console.log(`✅ WASM execution completed. Input: [${msg.data_chunk.join(',')}] -> Output: [${result.join(',')}]`);

        // Add to compute history
        addToComputeHistory({
            taskId: msg.task_id,
            dataChunk: msg.data_chunk,
            result: result,
            mode: mapFunction,
            communication: "WASM " + mapFunction
        });

        // Send result back via WebRTC data channel
        const resultMessage = {
            task_id: msg.task_id,
            result: result,
            worker_id: myId
        };

        channel.send(JSON.stringify(resultMessage));
        console.log("✅ Sent WASM result via WebRTC:", resultMessage);

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