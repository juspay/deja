import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev mode proxies API calls to the local orchestrator; the production build
// is embedded into the deja-orchestrator binary (rust-embed) and served
// same-origin, so no base-path or CORS configuration is needed.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8070",
      "/runs": "http://127.0.0.1:8070",
      "/recordings": "http://127.0.0.1:8070",
      "/healthz": "http://127.0.0.1:8070",
    },
  },
});
