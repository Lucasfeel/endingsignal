import { useMemo } from "react";
import type React from "react";

import type { AuthContextValue } from "../../shared/hooks/use-auth";
import type { SubscriptionItem } from "../../shared/types";

export function usePublicModalModels({
  auth,
  authDescription,
  authEmail,
  authError,
  authMode,
  authPassword,
  authPasswordConfirm,
  email,
  modalContent,
  modalSubscribed,
  myPageOpen,
  myPageSummaryStatus,
  onAuthClose,
  onAuthSubmit,
  onBack,
  onChangePassword,
  onGoSubscriptions,
  onLogout,
  onRequireAuth,
  onSetAuthEmail,
  onSetAuthMode,
  onSetAuthPassword,
  onSetAuthPasswordConfirm,
  onToggleSubscription,
  showAuthModal,
  subscriptionItems,
}: {
  auth: AuthContextValue;
  authDescription: string;
  authEmail: string;
  authError: string;
  authMode: "login" | "register";
  authPassword: string;
  authPasswordConfirm: string;
  email: string;
  modalContent: any;
  modalSubscribed: boolean;
  myPageOpen: boolean;
  myPageSummaryStatus: string;
  onAuthClose: () => void;
  onAuthSubmit: () => void;
  onBack: () => void;
  onChangePassword: (event: React.FormEvent<HTMLFormElement>) => void;
  onGoSubscriptions: () => void;
  onLogout: () => void;
  onRequireAuth: () => void;
  onSetAuthEmail: (value: string) => void;
  onSetAuthMode: (mode: "login" | "register") => void;
  onSetAuthPassword: (value: string) => void;
  onSetAuthPasswordConfirm: (value: string) => void;
  onToggleSubscription: () => void;
  showAuthModal: boolean;
  subscriptionItems: SubscriptionItem[];
}) {
  return useMemo(
    () => ({
      authModalProps: {
        authDescription,
        authEmail,
        authError,
        authMode,
        authPassword,
        authPasswordConfirm,
        isOpen: showAuthModal,
        onClose: onAuthClose,
        onSetAuthEmail,
        onSetAuthMode,
        onSetAuthPassword,
        onSetAuthPasswordConfirm,
        onSubmit: onAuthSubmit,
      },
      myPageOverlayProps: {
        email,
        isOpen: myPageOpen && auth.isAuthenticated,
        myPageSummaryStatus,
        onBack,
        onChangePassword,
        onGoSubscriptions,
        onLogout,
        subscriptionItems,
      },
      subscribeModalProps: {
        content: modalContent,
        isAuthenticated: auth.isAuthenticated,
        isOpen: Boolean(modalContent),
        isSubscribed: modalSubscribed,
        onClose: onAuthClose,
        onRequireAuth,
        onToggleSubscription,
      },
    }),
    [
      auth.isAuthenticated,
      authDescription,
      authEmail,
      authError,
      authMode,
      authPassword,
      authPasswordConfirm,
      email,
      modalContent,
      modalSubscribed,
      myPageOpen,
      myPageSummaryStatus,
      onAuthClose,
      onAuthSubmit,
      onBack,
      onChangePassword,
      onGoSubscriptions,
      onLogout,
      onRequireAuth,
      onSetAuthEmail,
      onSetAuthMode,
      onSetAuthPassword,
      onSetAuthPasswordConfirm,
      onToggleSubscription,
      showAuthModal,
      subscriptionItems,
    ],
  );
}
