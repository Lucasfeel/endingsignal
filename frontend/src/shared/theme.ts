import { useEffect, useState } from "react";

import { getRuntime } from "./runtime";
import { readTheme, writeTheme } from "./storage";

export function resolveInitialTheme() {
  const savedTheme = readTheme();
  if (savedTheme === "light" || savedTheme === "dark") {
    return savedTheme;
  }

  const runtimeTheme = getRuntime().theme?.initialTheme;
  if (runtimeTheme === "light" || runtimeTheme === "dark") {
    return runtimeTheme;
  }

  if (typeof window.matchMedia === "function") {
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }

  return "light";
}

export function applyTheme(theme: "light" | "dark") {
  document.documentElement.dataset.theme = theme;
  writeTheme(theme);
}

export function useTheme() {
  const [theme, setTheme] = useState<"light" | "dark">(resolveInitialTheme);

  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  return {
    theme,
    toggleTheme: () => setTheme((current) => (current === "light" ? "dark" : "light")),
  };
}
