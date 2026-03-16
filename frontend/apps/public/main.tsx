import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";

import "../../public-app/styles.css";
import "../../src/styles/base.css";
import { PublicApp } from "../../src/public/app";
import { installPublicTelemetry } from "../../src/public/posthog";

const rootElement = document.getElementById("app-root");

if (rootElement) {
  installPublicTelemetry();
  createRoot(rootElement).render(
    <StrictMode>
      <BrowserRouter>
        <PublicApp />
      </BrowserRouter>
    </StrictMode>,
  );
}
