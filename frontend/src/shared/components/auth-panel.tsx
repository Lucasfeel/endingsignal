import { useState, type FormEvent } from "react";

import { ApiError } from "../api";
import { useAuth } from "../hooks/use-auth";

type AuthPanelProps = {
  title: string;
  description: string;
};

export function AuthPanel({ title, description }: AuthPanelProps) {
  const { login, register, isLoading } = useAuth();
  const [mode, setMode] = useState<"login" | "register">("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    try {
      if (mode === "login") {
        await login({ email, password });
      } else {
        await register({ email, password });
      }
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "인증 요청에 실패했습니다.");
    }
  }

  return (
    <section className="es-panel es-auth-panel">
      <div className="es-stack">
        <div>
          <h2>{title}</h2>
          <p className="es-muted">{description}</p>
        </div>
        <form className="es-stack" onSubmit={handleSubmit}>
          <label className="es-field">
            <span>이메일</span>
            <input
              className="es-input"
              autoComplete="email"
              type="email"
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              placeholder="name@example.com"
              required
            />
          </label>
          <label className="es-field">
            <span>비밀번호</span>
            <input
              className="es-input"
              autoComplete={mode === "login" ? "current-password" : "new-password"}
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder="8자 이상"
              required
            />
          </label>
          {error ? <p className="es-error">{error}</p> : null}
          <div className="es-row">
            <button className="es-button es-button-primary" disabled={isLoading} type="submit">
              {mode === "login" ? "로그인" : "회원가입"}
            </button>
            <button
              className="es-button es-button-secondary"
              disabled={isLoading}
              type="button"
              onClick={() => setMode((current) => (current === "login" ? "register" : "login"))}
            >
              {mode === "login" ? "회원가입 전환" : "로그인 전환"}
            </button>
          </div>
        </form>
      </div>
    </section>
  );
}
