# 🐝 WASMHive WebApp

The browser side of **WASMHive**: the signaling server that wires the network together, and the worker page that turns any browser tab into a compute node. The Rust master and example workloads live in [WASMHive-Runtime](https://github.com/WASMHive/WASMHive-Runtime).

## What's here

| Path | What it is |
|---|---|
| `server/websocket-server.js` | Signaling server (port 3000): registration, peer lists, WebRTC offer/answer/candidate relay, fair-share allocation broadcast |
| `server/proxy-server.js` | CORS proxy (port 3001) used by the web-crawl workload so worker tabs can fetch cross-origin pages: follows redirects, decompresses gzip/brotli, preserves charset headers, rejects bad URLs with a 400 instead of crashing |
| `worker/index.html` + `worker/worker.js` | The worker node page: connects to signaling, opens WebRTC data channels to masters, executes shipped WASM, streams results back. Shows live network topology, task history, health, and fault-tolerance events |

## 🚀 Run it

```bash
cd server
npm install
node websocket-server.js     # signaling on ws://localhost:3000
node proxy-server.js         # optional, needed for the web-crawl example
```

Then open `worker/index.html` in one or more browser tabs. Each tab is one worker node. Start a job from [WASMHive-Runtime](https://github.com/WASMHive/WASMHive-Runtime) and watch tasks land on the dashboard.

### Crawl proxy behavior

The proxy follows up to 5 redirects server-side (the browser can't follow cross-origin `Location` headers itself), transparently decompresses gzip/deflate/brotli bodies, forwards the origin's `Content-Type` charset so workers can decode non-UTF-8 pages, sends a crawler `User-Agent`, and times out stalled origins with a 504 (default 20s; tune with `PROXY_TIMEOUT_MS`, change the port with `PORT`). Malformed and non-http(s) URLs get a 400. Worker tabs on **other machines** can't reach `localhost:3001`; start the crawl with `WASMHIVE_PROXY_URL=http://<master-ip>:3001/proxy` (see the Runtime README) so tasks carry a proxy address those workers can reach.

### Same-machine workers: disable Chrome's mDNS masking

When the worker tabs and the Rust master run on the **same machine**, Chrome and Edge hide each tab's local IP behind an mDNS `.local` address by default. The native master can't resolve those, so tabs register with signaling but no WebRTC data channel ever opens and the master aborts with `no workers connected within 20s`. Launch the browser with that masking disabled:

```bash
# macOS — throwaway profile so it runs alongside your everyday Chrome.
# One worker tab per URL; vary the #fragment to open several.
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --user-data-dir=/tmp/wasmhive-hive \
  --disable-features=WebRtcHideLocalIpsWithMdns \
  "file://$PWD/worker/index.html#1" "file://$PWD/worker/index.html#2"
```

Headless Chrome doesn't apply the masking, so headless workers (as used for the benchmarks) need no flag. None of this is needed when the workers and the master are on **different machines** — real host IPs are exchanged normally.

## Branches

- `result-chunking`: kept for history. Superseded by the binary framed protocol on main, which chunks transfers in both directions with backpressure (see the [runtime protocol spec](https://github.com/WASMHive/WASMHive-Runtime/blob/main/docs/architecture.md)).

## 📄 License

MIT
