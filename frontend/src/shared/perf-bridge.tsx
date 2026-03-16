import { useEffect, useState } from "react";

type TransitionState = {
  active: boolean;
  warm: boolean;
  label: string;
};

const INITIAL_STATE: TransitionState = {
  active: false,
  warm: false,
  label: "",
};

function safeLabel(value: unknown) {
  return typeof value === "string" ? value.trim() : "";
}

export function PerfBridge() {
  const [transitionState, setTransitionState] = useState<TransitionState>(INITIAL_STATE);

  useEffect(() => {
    const onTransitionStart = (event: Event) => {
      const detail = (event as CustomEvent<Record<string, unknown>>).detail || {};
      setTransitionState({
        active: true,
        warm: detail.warm === true,
        label: safeLabel(detail.label),
      });
    };

    const onTransitionEnd = () => {
      setTransitionState(INITIAL_STATE);
    };

    window.addEventListener("es:perf:transition-start", onTransitionStart);
    window.addEventListener("es:perf:transition-end", onTransitionEnd);

    return () => {
      window.removeEventListener("es:perf:transition-start", onTransitionStart);
      window.removeEventListener("es:perf:transition-end", onTransitionEnd);
    };
  }, []);

  const className = [
    "es-shell-progress",
    transitionState.active ? "is-visible" : "",
    transitionState.warm ? "is-warm" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div aria-hidden="true" className={className} data-label={transitionState.label || ""}>
      <div className="es-shell-progress-bar" />
    </div>
  );
}
