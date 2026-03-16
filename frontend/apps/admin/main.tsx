import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";

import "../../src/styles/base.css";
import { AdminApp } from "../../src/admin/app";

const rootElement = document.getElementById("app-root");

if (rootElement) {
  createRoot(rootElement).render(
    <StrictMode>
      <BrowserRouter basename="/admin">
        <AdminApp />
      </BrowserRouter>
    </StrictMode>,
  );
}
