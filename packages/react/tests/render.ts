import { act, type ReactElement } from "react";
import { createRoot, type Root } from "react-dom/client";

/** Mounts `element` in a fresh container and returns the root for re-rendering and unmounting. */
export async function render(element: ReactElement): Promise<Root> {
  const root = createRoot(document.createElement("div"));
  await act(async () => root.render(element));
  return root;
}

export async function rerender(root: Root, element: ReactElement): Promise<void> {
  await act(async () => root.render(element));
}

export async function unmount(root: Root): Promise<void> {
  await act(async () => root.unmount());
}

/** Lets real timers and promises run, inside act, until `check` passes or `ms` elapse. */
export async function waitFor(check: () => void, ms = 1000): Promise<void> {
  const deadline = Date.now() + ms;
  for (;;) {
    try {
      check();
      return;
    } catch (err) {
      if (Date.now() > deadline) throw err;
    }
    await act(() => new Promise((r) => setTimeout(r, 5)));
  }
}
