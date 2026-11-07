import { GlobalRegistrator } from "@happy-dom/global-registrator";

// Imported by each DOM test as well as preloaded by bunfig.toml, which only applies when bun test
// runs from this package; from the repo root the import is what registers the DOM.
if (!GlobalRegistrator.isRegistered) GlobalRegistrator.register();
