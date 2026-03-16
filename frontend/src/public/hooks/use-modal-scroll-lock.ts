import { useEffect, type MutableRefObject } from "react";

const MODAL_SCROLL_STORAGE_KEY = "es.modal-scroll-y";

export function useModalScrollLock({
  routeContent,
  scrollYRef,
}: {
  routeContent: { source: string; contentId: string } | null;
  scrollYRef: MutableRefObject<number | null>;
}) {
  useEffect(() => {
    if (!routeContent) {
      return;
    }

    const persistedScrollY = Number(window.sessionStorage.getItem(MODAL_SCROLL_STORAGE_KEY));
    const restoreY =
      scrollYRef.current ??
      (Number.isFinite(persistedScrollY) && persistedScrollY >= 0 ? persistedScrollY : null);

    if (restoreY == null) {
      return;
    }

    const bodyStyle = document.body.style;
    const previous = {
      left: bodyStyle.left,
      overflowY: bodyStyle.overflowY,
      position: bodyStyle.position,
      right: bodyStyle.right,
      top: bodyStyle.top,
      width: bodyStyle.width,
    };

    bodyStyle.position = "fixed";
    bodyStyle.top = `-${restoreY}px`;
    bodyStyle.left = "0";
    bodyStyle.right = "0";
    bodyStyle.width = "100%";
    bodyStyle.overflowY = "scroll";

    return () => {
      bodyStyle.position = previous.position;
      bodyStyle.top = previous.top;
      bodyStyle.left = previous.left;
      bodyStyle.right = previous.right;
      bodyStyle.width = previous.width;
      bodyStyle.overflowY = previous.overflowY;
      window.scrollTo({ top: restoreY, behavior: "auto" });
      scrollYRef.current = null;
      window.sessionStorage.removeItem(MODAL_SCROLL_STORAGE_KEY);
    };
  }, [routeContent?.contentId, routeContent?.source, scrollYRef]);
}

export function rememberModalScrollPosition(scrollY: number) {
  window.sessionStorage.setItem(MODAL_SCROLL_STORAGE_KEY, String(scrollY));
}
