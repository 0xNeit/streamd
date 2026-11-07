import { GlobalRegistrator } from "@happy-dom/global-registrator";

GlobalRegistrator.register();
// Tells React that updates are wrapped in act(), so it flushes effects synchronously.
(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
