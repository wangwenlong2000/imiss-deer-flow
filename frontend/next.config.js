/**
 * Run `build` or `dev` with `SKIP_ENV_VALIDATION` to skip env validation. This is especially useful
 * for Docker builds.
 */
import "./src/env.js";

const gatewayPort = process.env.GATEWAY_PORT ?? "8001";
const langgraphPort = process.env.LANGGRAPH_PORT ?? "2024";

const backendBaseURL =
  process.env.DEERFLOW_GATEWAY_URL ??
  process.env.NEXT_PUBLIC_BACKEND_BASE_URL ??
  `http://127.0.0.1:${gatewayPort}`;
const langGraphBaseURL =
  process.env.DEERFLOW_LANGGRAPH_URL ??
  process.env.NEXT_PUBLIC_LANGGRAPH_BASE_URL ??
  `http://127.0.0.1:${langgraphPort}`;

/** @type {import("next").NextConfig} */
const config = {
  devIndicators: false,
  async rewrites() {
    return [
      {
        source: "/api/langgraph/:path*",
        destination: `${langGraphBaseURL}/:path*`,
      },
      {
        source: "/api/models/:path*",
        destination: `${backendBaseURL}/api/models/:path*`,
      },
      {
        source: "/api/mcp/:path*",
        destination: `${backendBaseURL}/api/mcp/:path*`,
      },
      {
        source: "/api/memory/:path*",
        destination: `${backendBaseURL}/api/memory/:path*`,
      },
      {
        source: "/api/skills/:path*",
        destination: `${backendBaseURL}/api/skills/:path*`,
      },
      {
        source: "/api/threads/:path*",
        destination: `${backendBaseURL}/api/threads/:path*`,
      },
      {
        source: "/api/agents/:path*",
        destination: `${backendBaseURL}/api/agents/:path*`,
      },
      {
        source: "/api/channels/:path*",
        destination: `${backendBaseURL}/api/channels/:path*`,
      },
      {
        source: "/api/data-center/:path*",
        destination: `${backendBaseURL}/api/data-center/:path*`,
      },
    ];
  },
};

export default config;
