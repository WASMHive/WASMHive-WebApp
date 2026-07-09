# 🐝 WASMHive WebApp

The browser side of **WASMHive**: the signaling server that wires the network together, and the worker page that turns any browser tab into a compute node. The Rust master and example workloads live in [WASMHive-Runtime](https://github.com/WASMHive/WASMHive-Runtime).

## What's here

| Path | What it is |
|---|---|
| `server/websocket-server.js` | Signaling server (port 3000): registration, peer lists, WebRTC offer/answer/candidate relay, fair-share allocation broadcast |
| `server/proxy-server.js` | CORS proxy (port 3001) used by the web-crawl workload so worker tabs can fetch cross-origin pages |
| `worker/index.html` + `worker/worker.js` | The worker node page: connects to signaling, opens WebRTC data channels to masters, executes shipped WASM, streams results back. Shows live network topology, task history, health, and fault-tolerance events |

## 🚀 Run it

```bash
cd server
npm install
node websocket-server.js     # signaling on ws://localhost:3000
node proxy-server.js         # optional, needed for the web-crawl example
```

Then open `worker/index.html` in one or more browser tabs. Each tab is one worker node. Start a job from [WASMHive-Runtime](https://github.com/WASMHive/WASMHive-Runtime) and watch tasks land on the dashboard.

## Branches

- `result-chunking`: kept for history. Superseded by the binary framed protocol on main, which chunks transfers in both directions with backpressure (see the [runtime protocol spec](https://github.com/WASMHive/WASMHive-Runtime/blob/main/docs/architecture.md)).

## 📄 License

MIT
