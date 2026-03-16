import { QueryClientProvider, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Navigate, NavLink, Route, Routes, useParams } from "react-router-dom";

import { apiRequest, ApiError, toQueryString } from "../shared/api";
import { AuthPanel } from "../shared/components/auth-panel";
import { AuthProvider, useAuth } from "../shared/hooks/use-auth";
import { queryClient } from "../shared/query";
import type { AuthUser } from "../shared/types";

const ADMIN_TABS = [
  { id: "manage", label: "콘텐츠 관리" },
  { id: "add", label: "콘텐츠 추가" },
  { id: "deleted", label: "삭제 목록" },
  { id: "publications", label: "공개 변경" },
  { id: "completion-changes", label: "완결 변경" },
  { id: "missing-completion", label: "완결 미설정" },
  { id: "missing-publication", label: "공개 미설정" },
  { id: "audit", label: "감사 로그" },
  { id: "cdc", label: "CDC 이벤트" },
  { id: "reports", label: "작업 리포트" },
  { id: "daily-notification", label: "일일 알림" },
];

function adminNavClassName(isActive: boolean) {
  return isActive ? "active" : "";
}

function useAdminGate() {
  const { user, token, isAuthenticated } = useAuth();
  const gateQuery = useQuery({
    queryKey: ["admin-gate", token],
    enabled: Boolean(token),
    queryFn: () => apiRequest<{ success?: boolean; message?: string }>("/api/auth/admin/ping", { token }),
  });

  return {
    user,
    isAuthenticated,
    isAdmin: user?.role === "admin" && gateQuery.isSuccess,
    isLoading: gateQuery.isLoading,
    error: gateQuery.error,
  };
}

function AdminChrome() {
  const { tab = "manage" } = useParams();
  const gate = useAdminGate();

  if (!gate.isAuthenticated) {
    return (
      <AuthPanel
        description="관리자 콘솔은 기존 auth token으로 보호됩니다. 먼저 로그인한 뒤 admin 권한을 확인합니다."
        title="Admin Console"
      />
    );
  }

  if (gate.isLoading) {
    return (
      <section className="es-panel">
        <p className="es-muted">관리자 권한을 확인하는 중입니다.</p>
      </section>
    );
  }

  if (!gate.isAdmin) {
    return (
      <section className="es-panel es-stack">
        <h2>관리자 권한이 필요합니다.</h2>
        <p className="es-error">
          {gate.error instanceof ApiError
            ? gate.error.message
            : "현재 계정으로는 admin console에 접근할 수 없습니다."}
        </p>
      </section>
    );
  }

  return (
    <div className="es-shell">
      <div className="es-shell-inner es-admin-shell">
        <header className="es-shell-header">
          <div className="es-brand">
            <h1>Admin Console</h1>
            <p>React 첫 슬라이스에서는 탭 구조와 데이터 조회 골격부터 옮깁니다.</p>
          </div>
          <div className="es-stack" style={{ justifyItems: "end" }}>
            <p className="es-muted">{gate.user?.email || "admin"} 계정</p>
            <a className="es-button es-button-secondary" href="/">
              Public으로 이동
            </a>
          </div>
        </header>

        <nav aria-label="Admin tabs" className="es-admin-nav">
          {ADMIN_TABS.map((item) => (
            <NavLink
              className={({ isActive }) => adminNavClassName(isActive)}
              key={item.id}
              to={`/${item.id}`}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>

        <AdminTabPage tab={tab} user={gate.user} />
      </div>
    </div>
  );
}

function ManagePanel() {
  const { token } = useAuth();
  const [contentId, setContentId] = useState("");
  const [source, setSource] = useState("");

  const enabled = Boolean(contentId.trim() && source.trim());
  const lookupQuery = useQuery({
    queryKey: ["admin-lookup", contentId, source],
    enabled,
    queryFn: () =>
      apiRequest(
        `/api/admin/contents/lookup${toQueryString({ content_id: contentId.trim(), source: source.trim() })}`,
        { token },
      ),
  });

  return (
    <section className="es-split">
      <div className="es-panel es-stack">
        <div>
          <h2>콘텐츠 조회</h2>
          <p className="es-muted">
            첫 슬라이스에서는 검색/편집의 전체 동작 대신 lookup과 데이터 구조 확인부터 React로 옮깁니다.
          </p>
        </div>
        <label className="es-field">
          <span>content_id</span>
          <input className="es-input" onChange={(event) => setContentId(event.target.value)} value={contentId} />
        </label>
        <label className="es-field">
          <span>source</span>
          <input className="es-input" onChange={(event) => setSource(event.target.value)} value={source} />
        </label>
      </div>
      <div className="es-panel es-stack">
        <div className="es-section-header">
          <div>
            <h2>조회 결과</h2>
            <p className="es-muted">lookup API 응답을 그대로 보여줍니다.</p>
          </div>
        </div>
        {!enabled ? <p className="es-muted">content_id와 source를 함께 입력하면 바로 조회합니다.</p> : null}
        {lookupQuery.isLoading ? <p className="es-muted">조회 중입니다.</p> : null}
        {lookupQuery.isError ? (
          <p className="es-error">
            {lookupQuery.error instanceof ApiError ? lookupQuery.error.message : "조회에 실패했습니다."}
          </p>
        ) : null}
        {lookupQuery.data ? <pre className="es-json">{JSON.stringify(lookupQuery.data, null, 2)}</pre> : null}
      </div>
    </section>
  );
}

type ContentTypeOption = {
  id: number;
  name: string;
};

type ContentSourceOption = {
  id: number;
  type_id: number;
  name: string;
};

function ContentAddPanel() {
  const { token } = useAuth();
  const queryClient = useQueryClient();
  const [selectedTypeId, setSelectedTypeId] = useState("");
  const [selectedSourceId, setSelectedSourceId] = useState("");
  const [title, setTitle] = useState("");
  const [authorName, setAuthorName] = useState("");
  const [contentUrl, setContentUrl] = useState("");
  const [newTypeName, setNewTypeName] = useState("");
  const [newSourceName, setNewSourceName] = useState("");
  const [message, setMessage] = useState("");

  const typesQuery = useQuery({
    queryKey: ["admin-content-types"],
    queryFn: async () => {
      const payload = await apiRequest<{ success?: boolean; types?: ContentTypeOption[] }>(
        "/api/admin/content-types",
        { token },
      );
      return payload.types || [];
    },
  });

  const sourcesQuery = useQuery({
    queryKey: ["admin-content-sources", selectedTypeId],
    enabled: Boolean(selectedTypeId),
    queryFn: async () => {
      const payload = await apiRequest<{ success?: boolean; sources?: ContentSourceOption[] }>(
        `/api/admin/content-sources${toQueryString({ typeId: selectedTypeId })}`,
        { token },
      );
      return payload.sources || [];
    },
  });

  const createTypeMutation = useMutation({
    mutationFn: async () => {
      await apiRequest("/api/admin/content-types", {
        method: "POST",
        token,
        body: { name: newTypeName },
      });
    },
    onSuccess: async () => {
      setMessage("콘텐츠 타입을 추가했습니다.");
      setNewTypeName("");
      await queryClient.invalidateQueries({ queryKey: ["admin-content-types"] });
    },
  });

  const createSourceMutation = useMutation({
    mutationFn: async () => {
      await apiRequest("/api/admin/content-sources", {
        method: "POST",
        token,
        body: { typeId: Number(selectedTypeId), name: newSourceName },
      });
    },
    onSuccess: async () => {
      setMessage("콘텐츠 소스를 추가했습니다.");
      setNewSourceName("");
      await queryClient.invalidateQueries({ queryKey: ["admin-content-sources", selectedTypeId] });
    },
  });

  const createContentMutation = useMutation({
    mutationFn: async () => {
      await apiRequest("/api/admin/contents", {
        method: "POST",
        token,
        body: {
          title,
          typeId: Number(selectedTypeId),
          sourceId: Number(selectedSourceId),
          authorName: authorName || undefined,
          contentUrl: contentUrl || undefined,
        },
      });
    },
    onSuccess: async () => {
      setMessage("콘텐츠를 추가했습니다.");
      setTitle("");
      setAuthorName("");
      setContentUrl("");
    },
  });

  return (
    <section className="es-split">
      <div className="es-stack">
        <section className="es-panel es-stack">
          <div>
            <h2>콘텐츠 타입 / 소스</h2>
            <p className="es-muted">
              2차 슬라이스에서는 skeleton 대신 실제 content type / source endpoint와 연결된 관리 폼을 붙입니다.
            </p>
          </div>
          <label className="es-field">
            <span>기준 타입</span>
            <select
              className="es-select"
              onChange={(event) => {
                setSelectedTypeId(event.target.value);
                setSelectedSourceId("");
              }}
              value={selectedTypeId}
            >
              <option value="">타입을 선택하세요</option>
              {(typesQuery.data || []).map((item) => (
                <option key={item.id} value={String(item.id)}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <div className="es-row">
            <input
              className="es-input"
              onChange={(event) => setNewTypeName(event.target.value)}
              placeholder="새 콘텐츠 타입 이름"
              value={newTypeName}
            />
            <button
              className="es-button es-button-secondary"
              disabled={!newTypeName.trim() || createTypeMutation.isPending}
              onClick={() => createTypeMutation.mutate()}
              type="button"
            >
              타입 추가
            </button>
          </div>
          <div className="es-row">
            <input
              className="es-input"
              disabled={!selectedTypeId}
              onChange={(event) => setNewSourceName(event.target.value)}
              placeholder="새 소스 이름"
              value={newSourceName}
            />
            <button
              className="es-button es-button-secondary"
              disabled={!selectedTypeId || !newSourceName.trim() || createSourceMutation.isPending}
              onClick={() => createSourceMutation.mutate()}
              type="button"
            >
              소스 추가
            </button>
          </div>
          <div className="es-list">
            {(sourcesQuery.data || []).map((item) => (
              <div className="es-list-item" key={item.id}>
                {item.name}
              </div>
            ))}
          </div>
        </section>
      </div>

      <section className="es-panel es-stack">
        <div>
          <h2>수동 콘텐츠 추가</h2>
          <p className="es-muted">
            `title + typeId + sourceId` 기준으로 실제 `/api/admin/contents` endpoint를 호출합니다.
          </p>
        </div>
        <label className="es-field">
          <span>작품명</span>
          <input className="es-input" onChange={(event) => setTitle(event.target.value)} value={title} />
        </label>
        <label className="es-field">
          <span>타입</span>
          <select
            className="es-select"
            onChange={(event) => {
              setSelectedTypeId(event.target.value);
              setSelectedSourceId("");
            }}
            value={selectedTypeId}
          >
            <option value="">타입을 선택하세요</option>
            {(typesQuery.data || []).map((item) => (
              <option key={item.id} value={String(item.id)}>
                {item.name}
              </option>
            ))}
          </select>
        </label>
        <label className="es-field">
          <span>소스</span>
          <select
            className="es-select"
            disabled={!selectedTypeId}
            onChange={(event) => setSelectedSourceId(event.target.value)}
            value={selectedSourceId}
          >
            <option value="">소스를 선택하세요</option>
            {(sourcesQuery.data || []).map((item) => (
              <option key={item.id} value={String(item.id)}>
                {item.name}
              </option>
            ))}
          </select>
        </label>
        <label className="es-field">
          <span>작가명</span>
          <input className="es-input" onChange={(event) => setAuthorName(event.target.value)} value={authorName} />
        </label>
        <label className="es-field">
          <span>콘텐츠 URL</span>
          <input className="es-input" onChange={(event) => setContentUrl(event.target.value)} value={contentUrl} />
        </label>
        {message ? <p className="es-muted">{message}</p> : null}
        {(typesQuery.isError || sourcesQuery.isError || createContentMutation.isError) ? (
          <p className="es-error">
            {[typesQuery.error, sourcesQuery.error, createContentMutation.error]
              .find(Boolean) instanceof ApiError
              ? ([typesQuery.error, sourcesQuery.error, createContentMutation.error].find(Boolean) as ApiError).message
              : "Admin 요청 중 오류가 발생했습니다."}
          </p>
        ) : null}
        <div className="es-actions">
          <button
            className="es-button es-button-primary"
            disabled={!title.trim() || !selectedTypeId || !selectedSourceId || createContentMutation.isPending}
            onClick={() => createContentMutation.mutate()}
            type="button"
          >
            콘텐츠 추가
          </button>
        </div>
      </section>
    </section>
  );
}

function DataPanel({ path, title, user }: { path: string; title: string; user: AuthUser | null }) {
  const { token } = useAuth();
  const dataQuery = useQuery({
    queryKey: ["admin-panel", path],
    queryFn: () => apiRequest(path, { token }),
  });

  return (
    <section className="es-stack">
      <div className="es-panel es-section-header">
        <div>
          <h2>{title}</h2>
          <p className="es-muted">현재 계정: {user?.email || "-"}</p>
        </div>
        <span className="es-badge">데이터 스켈레톤</span>
      </div>
      <section className="es-panel es-stack">
        <p className="es-muted">
          이 탭은 1차 슬라이스에서 기존 admin endpoint를 React로 다시 연결한 골격입니다. 편집 폼과 bulk action은 다음 슬라이스에서 이관합니다.
        </p>
        {dataQuery.isLoading ? <p className="es-muted">데이터를 불러오는 중입니다.</p> : null}
        {dataQuery.isError ? (
          <p className="es-error">
            {dataQuery.error instanceof ApiError ? dataQuery.error.message : "데이터를 불러오지 못했습니다."}
          </p>
        ) : null}
        {dataQuery.data ? <pre className="es-json">{JSON.stringify(dataQuery.data, null, 2)}</pre> : null}
      </section>
    </section>
  );
}

function AdminTabPage({ tab, user }: { tab: string; user: AuthUser | null }) {
  switch (tab) {
    case "manage":
      return <ManagePanel />;
    case "add":
      return <ContentAddPanel />;
    case "deleted":
      return <DataPanel path="/api/admin/contents/deleted?limit=20&offset=0" title="삭제된 콘텐츠" user={user} />;
    case "publications":
      return <DataPanel path="/api/admin/contents/publications?limit=20&offset=0" title="최근 공개 변경" user={user} />;
    case "completion-changes":
      return <DataPanel path="/api/admin/contents/completion-changes?limit=20&offset=0" title="최근 완결 변경" user={user} />;
    case "missing-completion":
      return (
        <DataPanel
          path="/api/admin/contents/missing-completion?limit=20&offset=0"
          title="완결 미설정 목록"
          user={user}
        />
      );
    case "missing-publication":
      return (
        <DataPanel
          path="/api/admin/contents/missing-publication?limit=20&offset=0"
          title="공개 미설정 목록"
          user={user}
        />
      );
    case "audit":
      return <DataPanel path="/api/admin/audit/logs?limit=20&offset=0" title="감사 로그" user={user} />;
    case "cdc":
      return <DataPanel path="/api/admin/cdc/events?limit=20&offset=0" title="CDC 이벤트" user={user} />;
    case "reports":
      return <DataPanel path="/api/admin/reports/daily-crawler?limit=20&offset=0" title="일일 작업 리포트" user={user} />;
    case "daily-notification":
      return (
        <DataPanel
          path={`/api/admin/reports/daily-notification${toQueryString({ date: new Date().toISOString().slice(0, 10) })}`}
          title="일일 알림 리포트"
          user={user}
        />
      );
    default:
      return <Navigate replace to="/manage" />;
  }
}

export function AdminApp() {
  return (
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <Routes>
          <Route element={<Navigate replace to="/manage" />} index />
          <Route element={<AdminChrome />} path=":tab" />
        </Routes>
      </AuthProvider>
    </QueryClientProvider>
  );
}
