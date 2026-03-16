import path from "node:path";
import react from "@vitejs/plugin-react";
import { sentryVitePlugin } from "@sentry/vite-plugin";
import { defineConfig } from "vite";

const sentryAuthToken =
  process.env.SENTRY_AUTH_TOKEN || process.env.SENTRY_ACCESS_TOKEN;

export default defineConfig({
  plugins: [
    react(),
    ...(sentryAuthToken
      ? [
          sentryVitePlugin({
            authToken: sentryAuthToken,
            org: process.env.SENTRY_ORG || process.env.SENTRY_ORG_SLUG || "lucas-54l",
            project: process.env.SENTRY_PROJECT || "endingsignal-web",
            sourcemaps: {
              filesToDeleteAfterUpload: ["./static/build/**/*.map"],
            },
          }),
        ]
      : []),
  ],
  build: {
    sourcemap: sentryAuthToken ? "hidden" : false,
    outDir: path.resolve(__dirname, "static/build"),
    emptyOutDir: true,
    target: "es2020",
    cssCodeSplit: false,
    rollupOptions: {
      input: {
        "public-app": path.resolve(__dirname, "frontend/apps/public/main.tsx"),
        "admin-app": path.resolve(__dirname, "frontend/apps/admin/main.tsx"),
      },
      output: {
        entryFileNames: "[name].js",
        chunkFileNames: "chunks/[name]-[hash].js",
        assetFileNames: (assetInfo) => {
          if ((assetInfo.name || "").endsWith(".css")) {
            return "app-shell.css";
          }
          return "[name][extname]";
        },
        manualChunks: {
          "vendor-react": [
            "react",
            "react-dom",
            "react/jsx-runtime",
            "react-router-dom",
            "@tanstack/react-query",
          ],
        },
      },
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: "./frontend/src/test/setup.ts",
  },
});
