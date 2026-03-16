import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";

import { PublicApp } from "./app";

vi.mock("../shared/api", async () => {
  const actual = await vi.importActual<typeof import("../shared/api")>("../shared/api");
  return {
    ...actual,
    apiRequest: vi.fn(async (path: string) => {
      const normalizedPath = String(path);

      if (normalizedPath.includes("/api/contents/recommendations_v2")) {
        return {
          contents: [
            {
              content_id: "home-1",
              source: "netflix",
              title: "추천작",
              status: "연재중",
              content_type: "ott",
              display_meta: { authors: ["author"] },
            },
          ],
          returned: 1,
          limit: 12,
        };
      }

      if (normalizedPath.includes("/api/contents/browse_v3")) {
        return {
          contents: [
            {
              content_id: "novel-1",
              source: "ridi",
              title: "꽃",
              status: "연재중",
              content_type: "novel",
              display_meta: { authors: ["Winter flower"] },
            },
          ],
          next_cursor: null,
          returned: 1,
          filters: {},
        };
      }

      if (normalizedPath.includes("/api/auth/me")) {
        return {
          success: true,
          user: {
            id: 1,
            email: "mock-user@endingsignal.local",
            role: "user",
          },
        };
      }

      if (normalizedPath.includes("/api/me/subscriptions")) {
        return {
          items: [],
        };
      }

      if (normalizedPath.includes("/api/contents/search")) {
        return [];
      }

      return [];
    }),
  };
});

describe("PublicApp", () => {
  beforeEach(() => {
    window.localStorage.clear();
    vi.restoreAllMocks();
  });

  it("renders the home shell", async () => {
    window.localStorage.setItem(
      "endingsignal.recently-searched-contents",
      JSON.stringify([
        {
          key: "ridi%3A1",
          openedAt: Date.now(),
          content: {
            content_id: "1",
            source: "ridi",
            title: "최근 본 작품",
            status: "완결",
            content_type: "novel",
            display_meta: { authors: ["writer"] },
          },
        },
      ]),
    );

    render(
      <MemoryRouter initialEntries={["/"]}>
        <PublicApp />
      </MemoryRouter>,
    );

    expect(await screen.findByRole("button", { name: "홈으로 이동" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "검색" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "프로필" })).toBeInTheDocument();
    expect(screen.getByText("추천작")).toBeInTheDocument();
    expect(screen.getByText("검색했던 작품")).toBeInTheDocument();
    expect(screen.getAllByText("최근 본 작품").length).toBeGreaterThan(0);
  });

  it("shows the search empty state", async () => {
    render(
      <MemoryRouter initialEntries={["/search"]}>
        <PublicApp />
      </MemoryRouter>,
    );

    const input = await screen.findByPlaceholderText("작품을 검색해 보세요");
    fireEvent.change(input, { target: { value: "없는작품" } });
    fireEvent.keyDown(input, { key: "Enter", code: "Enter" });

    expect(await screen.findByText("검색 결과가 없어요")).toBeInTheDocument();
  });

  it("uses visible browse cards in the search overlay", async () => {
    render(
      <MemoryRouter initialEntries={["/browse/novel"]}>
        <PublicApp />
      </MemoryRouter>,
    );

    expect((await screen.findAllByRole("button", { name: "꽃 열기" })).length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole("button", { name: "검색" }));

    expect(await screen.findByText("웹소설 페이지에서 살펴본 작품을 보여드려요.")).toBeInTheDocument();
    expect(screen.getAllByText("꽃").length).toBeGreaterThan(0);
  });

  it("restores source chips to neutral when every source is deselected", async () => {
    render(
      <MemoryRouter initialEntries={["/browse/ott"]}>
        <PublicApp />
      </MemoryRouter>,
    );

    const tving = await screen.findByRole("button", { name: "TVING source filter" });
    const netflix = screen.getByRole("button", { name: "Netflix source filter" });
    const laftel = screen.getByRole("button", { name: "Laftel source filter" });

    expect(tving).toHaveClass("is-neutral");
    expect(tving.style.getPropertyValue("--logo-fit")).toBe("contain");
    expect(tving.style.getPropertyValue("--logo-width")).toBe("33px");
    expect(laftel.style.getPropertyValue("--logo-fit")).toBe("contain");
    expect(laftel.style.getPropertyValue("--logo-width")).toBe("33px");

    fireEvent.click(tving);
    expect(tving).toHaveClass("is-bright");
    expect(netflix).toHaveClass("is-dim");

    fireEvent.click(tving);
    expect(tving).toHaveClass("is-neutral");
    expect(netflix).toHaveClass("is-neutral");
  });

  it("does not render the ott sort row", async () => {
    render(
      <MemoryRouter initialEntries={["/browse/ott"]}>
        <PublicApp />
      </MemoryRouter>,
    );

    expect(await screen.findByRole("button", { name: "TVING source filter" })).toBeInTheDocument();
    expect(screen.queryByText("최신순")).not.toBeInTheDocument();
    expect(screen.queryByText("가나다순")).not.toBeInTheDocument();
  });

  it("closes the profile menu when clicking outside", async () => {
    window.localStorage.setItem("es_access_token", "token");

    render(
      <MemoryRouter initialEntries={["/"]}>
        <PublicApp />
      </MemoryRouter>,
    );

    const profileButton = await screen.findByRole("button", { name: /프로필/ });
    fireEvent.click(profileButton);

    const menu = await screen.findByRole("menu", { name: "프로필 메뉴" });
    expect(menu).not.toHaveClass("hidden");

    fireEvent.mouseDown(document.body);
    expect(menu).toHaveClass("hidden");
  });

  it("shows the subscriptions empty state after auth resolves", async () => {
    window.localStorage.setItem("es_access_token", "token");

    render(
      <MemoryRouter initialEntries={["/subscriptions"]}>
        <PublicApp />
      </MemoryRouter>,
    );

    expect(await screen.findByText("완결 알림을 구독한 작품이 없습니다")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "검색하기" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "웹툰 보기" })).toBeInTheDocument();
  });

  it("shows the empty summary copy in my page after auth resolves", async () => {
    window.localStorage.setItem("es_access_token", "token");

    render(
      <MemoryRouter initialEntries={["/mypage"]}>
        <PublicApp />
      </MemoryRouter>,
    );

    expect(await screen.findByText("구독 중인 작품이 아직 없어요")).toBeInTheDocument();
    expect(screen.getByText("내 정보")).toBeInTheDocument();
  });

  it("keeps subscriptions accessible without forcing the auth modal", async () => {
    render(
      <MemoryRouter initialEntries={["/subscriptions"]}>
        <PublicApp />
      </MemoryRouter>,
    );

    expect(await screen.findByText("로그인이 필요해요")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "로그인하기" })).toBeInTheDocument();
    expect(screen.getByRole("dialog", { name: "로그인", hidden: true })).toHaveClass("hidden");
  });
  it("keeps card text constrained for legacy-style ellipsis", async () => {
    render(
      <MemoryRouter initialEntries={["/browse/novel"]}>
        <PublicApp />
      </MemoryRouter>,
    );

    const cardTitle = (await screen.findAllByRole("heading", { level: 3 }))[0];
    const cardMeta = cardTitle.parentElement?.querySelector(".es-card-meta");

    expect(cardTitle).toHaveClass("es-card-title", "min-w-0", "w-full");
    expect(cardMeta).toHaveClass("es-card-meta", "min-w-0", "w-full");
  });

  it("locks the body scroll position when opening a content modal", async () => {
    vi.stubGlobal("scrollTo", vi.fn());
    Object.defineProperty(window, "scrollY", {
      configurable: true,
      value: 480,
    });

    render(
      <MemoryRouter initialEntries={["/browse/novel"]}>
        <PublicApp />
      </MemoryRouter>,
    );

    fireEvent.click((await screen.findAllByRole("button", { name: /열기/ }))[0]);

    expect(document.body.style.position).toBe("fixed");
    expect(document.body.style.top).toBe("-480px");
  });
});
