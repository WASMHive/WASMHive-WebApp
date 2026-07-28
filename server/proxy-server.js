const http = require("http");
const https = require("https");
const zlib = require("zlib");

const PORT = process.env.PORT || 3001;
const UPSTREAM_TIMEOUT_MS = parseInt(process.env.PROXY_TIMEOUT_MS, 10) || 20000;
const MAX_REDIRECTS = 5;
// Some origins refuse requests with no User-Agent; identify as a crawler.
const USER_AGENT = "Mozilla/5.0 (compatible; WASMHive-crawler/1.0; +https://github.com/WASMHive)";

const CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
};

function fail(res, status, message) {
    // Mid-stream failures can't change the status line; cut the connection so
    // the browser's fetch rejects instead of hanging on a half body.
    if (res.headersSent) {
        res.destroy();
        return;
    }
    res.writeHead(status, { ...CORS_HEADERS, "Content-Type": "text/plain" });
    res.end(message);
}

function parseTarget(rawUrl) {
    const requestUrl = new URL(rawUrl, "http://localhost");
    if (requestUrl.pathname !== "/proxy") return { error: 404 };
    const target = requestUrl.searchParams.get("url");
    if (!target) return { error: 400, message: "Missing url parameter. Use /proxy?url=<target_url>" };
    let parsed;
    try {
        parsed = new URL(target);
    } catch {
        return { error: 400, message: `Invalid URL: ${target}` };
    }
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
        return { error: 400, message: `Unsupported protocol: ${parsed.protocol}` };
    }
    return { url: parsed };
}

// Fetch targetUrl, following redirects, and stream the (decompressed) final
// response to the client. All failure paths answer the client; none throw.
function proxyFetch(targetUrl, redirectsLeft, res) {
    const mod = targetUrl.protocol === "https:" ? https : http;
    const proxyReq = mod.get(
        targetUrl,
        {
            headers: {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Encoding": "gzip, br",
            },
        },
        (proxyRes) => {
            const status = proxyRes.statusCode;

            // Follow redirects proxy-side: the Location header would be useless
            // to the browser client (cross-origin), so chase it here.
            if ([301, 302, 303, 307, 308].includes(status) && proxyRes.headers.location) {
                proxyRes.resume();
                if (redirectsLeft <= 0) {
                    fail(res, 502, `Too many redirects for ${targetUrl.href}`);
                    return;
                }
                let next;
                try {
                    next = new URL(proxyRes.headers.location, targetUrl);
                } catch {
                    fail(res, 502, `Invalid redirect location from ${targetUrl.href}`);
                    return;
                }
                if (next.protocol !== "http:" && next.protocol !== "https:") {
                    fail(res, 502, `Redirect to unsupported protocol: ${next.protocol}`);
                    return;
                }
                console.log(`↪️  ${targetUrl.href} -> ${next.href} (${status})`);
                proxyFetch(next, redirectsLeft - 1, res);
                return;
            }

            console.log(`✅ Response from ${targetUrl.href}: ${status}`);

            // Decompress here so the client always receives plain bytes:
            // Content-Encoding is dropped along with the now-wrong length.
            const encoding = (proxyRes.headers["content-encoding"] || "").toLowerCase();
            let body = proxyRes;
            const decoders = { gzip: zlib.createGunzip, "x-gzip": zlib.createGunzip, br: zlib.createBrotliDecompress, deflate: zlib.createInflate };
            if (decoders[encoding]) {
                const decoder = decoders[encoding]();
                decoder.on("error", (err) => {
                    console.error(`❌ Decompression failed for ${targetUrl.href}:`, err.message);
                    proxyRes.destroy();
                    fail(res, 502, "Decompression failed");
                });
                body = proxyRes.pipe(decoder);
            }

            res.writeHead(status, {
                ...CORS_HEADERS,
                "Content-Type": proxyRes.headers["content-type"] || "text/html",
            });
            body.pipe(res);
            proxyRes.on("error", () => fail(res, 502, "Upstream connection failed"));
        }
    );

    // Idle-socket timeout: also fires on an origin that stalls mid-body, in
    // which case fail() cuts the client connection instead of leaving it hung.
    proxyReq.setTimeout(UPSTREAM_TIMEOUT_MS, () => {
        console.error(`⏱️ Proxy timeout for ${targetUrl.href}`);
        proxyReq.destroy(new Error("upstream timeout"));
        fail(res, 504, "Gateway timeout");
    });

    proxyReq.on("error", (err) => {
        console.error(`❌ Proxy error for ${targetUrl.href}:`, err.message);
        fail(res, 502, `Proxy error: ${err.message}`);
    });

    // Client gave up: stop the upstream transfer too.
    res.on("close", () => proxyReq.destroy());
}

const server = http.createServer((req, res) => {
    try {
        if (req.method === "OPTIONS") {
            res.writeHead(204, CORS_HEADERS);
            res.end();
            return;
        }

        const target = parseTarget(req.url);
        if (target.error === 404) {
            fail(res, 404, "Not found. Use /proxy?url=<target_url>");
            return;
        }
        if (target.error) {
            console.error(`⚠️ Rejected request ${req.url}: ${target.message}`);
            fail(res, target.error, target.message);
            return;
        }

        console.log(`📡 Proxying request to: ${target.url.href}`);
        proxyFetch(target.url, MAX_REDIRECTS, res);
    } catch (err) {
        // A crawl feeds this server arbitrary strings; never let one kill it.
        console.error("❌ Unexpected proxy failure:", err);
        fail(res, 500, "Internal proxy error");
    }
});

server.on("error", (err) => {
    console.error(`❌ Proxy server failed to start on port ${PORT}: ${err.message}`);
    process.exit(1);
});

server.listen(PORT, () => {
    console.log(`🌐 CORS Proxy server listening on port ${PORT}`);
    console.log(`   Use: http://localhost:${PORT}/proxy?url=<target_url>`);
    console.log(`   Example: http://localhost:${PORT}/proxy?url=https://www.google.com`);
});
