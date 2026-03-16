import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";

import { AdminApp } from "./app";

vi.mock("../shared/api", async () => {
  const actual = await vi.importActual<typeof import("../shared/api")>("../shared/api");
  return {
    ...actual,
    apiRequest: vi.fn(async (path: string) => {
      if (String(path).includes("/api/auth/admin/ping")) {
        return { success: true };
      }
      if (String(path).includes("/api/auth/me")) {
        return { success: true, user: { email: "admin@example.com", role: "admin" } };
      }
      return { success: true, items: [] };
    }),
  };
});

describe("AdminApp", () => {
  it("renders admin navigation", async () => {
    window.localStorage.setItem("es_access_token", "token");

    render(
      <MemoryRouter initialEntries={["/manage"]}>
        <AdminApp />
      </MemoryRouter>,
    );

    expect(await screen.findByText("Admin Console")).toBeInTheDocument();
    expect(await screen.findByText("콘텐츠 관리")).toBeInTheDocument();
  });
});
