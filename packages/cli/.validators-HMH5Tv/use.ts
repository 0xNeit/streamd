import { isEverything, type Everything } from "./shapes.js";
declare const payload: unknown;
if (isEverything(payload)) { const e: Everything = payload; void e; }