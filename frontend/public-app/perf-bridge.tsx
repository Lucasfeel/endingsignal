import { render } from "preact";
import { signal } from "@preact/signals";

type TransitionState = {
  active: boolean;
  warm: boolean;
  label: string;
};

const transitionState = signal<TransitionState>({
  active: false,
  warm: false,
  label: "",
});

function PerfProgressBar() {
  const current = transitionState.value;
  const className = [
    "es-shell-progress",
    current.active ? "is-visible" : "",
    current.warm ? "is-warm" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div class={className} aria-hidden="true" data-label={current.label || ""}>
      <div class="es-shell-progress-bar" />
    </div>
  );
}

const safeLabel = (value: unknown) => (typeof value === "string" ? value.trim() : "");

const onTransitionStart = (event: Event) => {
  const detail = (event as CustomEvent<Record<string, unknown>>).detail || {};
  transitionState.value = {
    active: true,
    warm: detail.warm === true,
    label: safeLabel(detail.label),
  };
};

const onTransitionEnd = () => {
  transitionState.value = {
    active: false,
    warm: false,
    label: "",
  };
};

export const mountPerfBridge = () => {
  const mountNode = document.getElementById("esPerfRoot");
  if (!mountNode || mountNode.dataset.mounted === "1") return;

  mountNode.dataset.mounted = "1";
  render(<PerfProgressBar />, mountNode);

  window.addEventListener("es:perf:transition-start", onTransitionStart);
  window.addEventListener("es:perf:transition-end", onTransitionEnd);
};
