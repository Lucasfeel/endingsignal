import path from "node:path";
import preact from "@preact/preset-vite";
import { sentryVitePlugin } from "@sentry/vite-plugin";
import { defineConfig } from "vite";

const sentryAuthToken =
  process.env.SENTRY_AUTH_TOKEN || process.env.SENTRY_ACCESS_TOKEN;

export default defineConfig({
  plugins: [
    preact(),
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
