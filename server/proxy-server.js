const http = require("http");
const https = require("https");
const url = require("url");

// Create HTTP server for proxy
const server = http.createServer((req, res) => {
    // Enable CORS for all origins
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    // Handle preflight OPTIONS requests
    if (req.method === "OPTIONS") {
        res.writeHead(200);
        res.end();
        return;
    }

    // Simple proxy endpoint: /proxy?url=<target_url>
    if (req.url.startsWith("/proxy?url=")) {
        const targetUrl = decodeURIComponent(req.url.substring(11));
        console.log(`📡 Proxying request to: ${targetUrl}`);

        const parsedUrl = url.parse(targetUrl);
        const protocol = parsedUrl.protocol === "https:" ? https : http;

        let isResponseSent = false;

        const proxyReq = protocol.get(targetUrl, (proxyRes) => {
            if (isResponseSent) return;
            isResponseSent = true;
            
            console.log(`✅ Response from ${targetUrl}: ${proxyRes.statusCode}`);
            res.writeHead(proxyRes.statusCode, {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": proxyRes.headers["content-type"] || "text/html",
            });
            proxyRes.pipe(res);
        });

        proxyReq.on("error", (err) => {
            if (isResponseSent) return;
            isResponseSent = true;
            
            console.error(`❌ Proxy error for ${targetUrl}:`, err.message);
            res.writeHead(500, { "Access-Control-Allow-Origin": "*" });
            res.end(`Proxy error: ${err.message}`);
        });

        proxyReq.setTimeout(30000, () => {
            if (isResponseSent) return;
            isResponseSent = true;
            
            console.error(`⏱️ Proxy timeout for ${targetUrl}`);
            proxyReq.destroy();
            res.writeHead(504, { "Access-Control-Allow-Origin": "*" });
            res.end("Gateway timeout");
        });
    } else {
        res.writeHead(404, { "Access-Control-Allow-Origin": "*" });
        res.end("Not found. Use /proxy?url=<target_url>");
    }
});

const PORT = 3001;
server.listen(PORT, () => {
    console.log(`🌐 CORS Proxy server listening on port ${PORT}`);
    console.log(`   Use: http://localhost:${PORT}/proxy?url=<target_url>`);
    console.log(`   Example: http://localhost:${PORT}/proxy?url=https://www.google.com`);
});

