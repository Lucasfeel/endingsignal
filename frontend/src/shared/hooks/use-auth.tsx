import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type PropsWithChildren,
} from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { apiRequest } from "../api";
import { clearAccessToken, readAccessToken, writeAccessToken } from "../storage";
import type { AuthUser } from "../types";

type LoginPayload = {
  email: string;
  password: string;
};

type RegisterPayload = LoginPayload;

type ChangePasswordPayload = {
  current_password: string;
  new_password: string;
};

export type AuthContextValue = {
  token: string | null;
  user: AuthUser | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  hasToken: boolean;
  login: (payload: LoginPayload) => Promise<void>;
  register: (payload: RegisterPayload) => Promise<void>;
  logout: () => Promise<void>;
  changePassword: (payload: ChangePasswordPayload) => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | null>(null);

function useMeQuery(token: string | null) {
  return useQuery({
    queryKey: ["auth", "me", token],
    enabled: Boolean(token),
    queryFn: async () => {
      const payload = await apiRequest<{ success?: boolean; user?: AuthUser }>("/api/auth/me", {
        token,
      });
      return payload.user || null;
    },
  });
}

export function AuthProvider({ children }: PropsWithChildren) {
  const queryClient = useQueryClient();
  const [token, setToken] = useState<string | null>(() => readAccessToken());
  const meQuery = useMeQuery(token);

  useEffect(() => {
    if (!token) {
      queryClient.removeQueries({ queryKey: ["auth", "me"] });
    }
  }, [queryClient, token]);

  const loginMutation = useMutation({
    mutationFn: async ({ email, password }: LoginPayload) => {
      const payload = await apiRequest<{
        access_token: string;
        user: AuthUser;
      }>("/api/auth/login", {
        method: "POST",
        body: { email, password },
      });
      return payload;
    },
    onSuccess: async (payload) => {
      writeAccessToken(payload.access_token);
      setToken(payload.access_token);
      await queryClient.invalidateQueries({ queryKey: ["auth", "me"] });
    },
  });

  const registerMutation = useMutation({
    mutationFn: async ({ email, password }: RegisterPayload) => {
      await apiRequest("/api/auth/register", {
        method: "POST",
        body: { email, password },
      });
      return loginMutation.mutateAsync({ email, password });
    },
  });

  const logoutMutation = useMutation({
    mutationFn: async () => {
      if (token) {
        await apiRequest("/api/auth/logout", {
          method: "POST",
          token,
        });
      }
    },
    onSettled: async () => {
      clearAccessToken();
      setToken(null);
      await queryClient.invalidateQueries({ queryKey: ["auth", "me"] });
      await queryClient.invalidateQueries({ queryKey: ["subscriptions"] });
    },
  });

  const changePasswordMutation = useMutation({
    mutationFn: async (payload: ChangePasswordPayload) => {
      await apiRequest("/api/auth/change-password", {
        method: "POST",
        token,
        body: payload,
      });
    },
  });

  const value = useMemo<AuthContextValue>(
    () => ({
      token,
      user: meQuery.data || null,
      isAuthenticated: Boolean(token && meQuery.data),
      hasToken: Boolean(token),
      isLoading:
        loginMutation.isPending ||
        registerMutation.isPending ||
        logoutMutation.isPending ||
        changePasswordMutation.isPending ||
        meQuery.isLoading,
      login: async (payload) => {
        await loginMutation.mutateAsync(payload);
      },
      register: async (payload) => {
        await registerMutation.mutateAsync(payload);
      },
      logout: async () => {
        await logoutMutation.mutateAsync();
      },
      changePassword: async (payload) => {
        await changePasswordMutation.mutateAsync(payload);
      },
    }),
    [
      changePasswordMutation,
      loginMutation,
      logoutMutation,
      meQuery.data,
      meQuery.isLoading,
      registerMutation,
      token,
    ],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const value = useContext(AuthContext);
  if (!value) {
    throw new Error("useAuth must be used inside AuthProvider");
  }
  return value;
}
