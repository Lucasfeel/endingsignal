const ACCESS_TOKEN_KEY = "es_access_token";
const THEME_KEY = "es_theme";

export function readAccessToken() {
  try {
    return window.localStorage.getItem(ACCESS_TOKEN_KEY);
  } catch (_error) {
    return null;
  }
}

export function writeAccessToken(token: string) {
  try {
    window.localStorage.setItem(ACCESS_TOKEN_KEY, token);
  } catch (_error) {
    // Ignore storage failures and keep the session in memory only.
  }
}

export function clearAccessToken() {
  try {
    window.localStorage.removeItem(ACCESS_TOKEN_KEY);
  } catch (_error) {
    // Ignore storage failures.
  }
}

export function readTheme() {
  try {
    return window.localStorage.getItem(THEME_KEY);
  } catch (_error) {
    return null;
  }
}

export function writeTheme(theme: "light" | "dark") {
  try {
    window.localStorage.setItem(THEME_KEY, theme);
  } catch (_error) {
    // Ignore storage failures.
  }
}
