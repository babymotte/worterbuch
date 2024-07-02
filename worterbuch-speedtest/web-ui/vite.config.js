import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/throughput": "http://localhost:4000",
      "/latency": "http://localhost:4000",
    },
  },
});
