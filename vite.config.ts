import path from "node:path";
import preact from "@preact/preset-vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [preact()],
  build: {
    outDir: path.resolve(__dirname, "static/build"),
    emptyOutDir: true,
    target: "es2020",
    sourcemap: false,
    cssCodeSplit: false,
    rollupOptions: {
      input: path.resolve(__dirname, "frontend/public-app/main.tsx"),
      output: {
        entryFileNames: "public-app.js",
        chunkFileNames: "chunks/[name]-[hash].js",
        assetFileNames: (assetInfo) => {
          if ((assetInfo.name || "").endsWith(".css")) {
            return "public-app.css";
          }
          return "[name][extname]";
        },
        manualChunks: {
          "vendor-preact": ["preact", "preact/jsx-runtime", "@preact/signals"],
        },
      },
    },
  },
});
