import { useCallback, useEffect, useRef, useState } from "react";

export function useProfileMenuState(onTrack: (name: string, payload?: Record<string, unknown>) => void) {
  const [isProfileMenuOpen, setIsProfileMenuOpen] = useState(false);
  const profileButtonRef = useRef<HTMLButtonElement | null>(null);
  const profileMenuRef = useRef<HTMLDivElement | null>(null);

  const closeProfileMenu = useCallback((reason: string) => {
    setIsProfileMenuOpen((current) => {
      if (!current) return current;
      onTrack("profile_menu_closed", { reason });
      return false;
    });
  }, [onTrack]);

  const toggleProfileMenu = useCallback(() => {
    setIsProfileMenuOpen((current) => {
      const next = !current;
      onTrack(next ? "profile_menu_opened" : "profile_menu_closed", {
        reason: "button",
      });
      return next;
    });
  }, [onTrack]);

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      if (!isProfileMenuOpen) return;
      const target = event.target as Node | null;
      if (!target) return;
      if (profileButtonRef.current?.contains(target) || profileMenuRef.current?.contains(target)) return;
      closeProfileMenu("outside_click");
    }

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape" && isProfileMenuOpen) {
        closeProfileMenu("escape");
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [closeProfileMenu, isProfileMenuOpen]);

  return {
    closeProfileMenu,
    isProfileMenuOpen,
    profileButtonRef,
    profileMenuRef,
    toggleProfileMenu,
  };
}
