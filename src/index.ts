export { StreamdClient } from "./client.js";
export { FullnodeScanAdapter } from "./adapters/fullnode.js";
export { EventPoller } from "./poller.js";
export { EventQuery } from "./query.js";
export { CursorManager } from "./cursor.js";
export {
  StreamdError,
  StreamdNetworkError,
  StreamdPollingError,
  StreamdTimeoutError,
} from "./errors.js";

export {
  every,
  involvesAddress,
  normalizeAddress,
  ofType,
  some,
  whereField,
} from "./filter.js";

export type { FullnodeScanAdapterOptions } from "./adapters/fullnode.js";
export type {
  FieldPath,
  FieldValue,
  InvolvesAddressOptions,
} from "./filter.js";

export type {
  AccountEventDescriptor,
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventFilter,
  EventPage,
  EventPredicate,
  FetchOptions,
  HandleEventDescriptor,
  ListenerOptions,
  ModuleEventDescriptor,
  MoveEventType,
  OnceOptions,
  StreamdConfig,
  StreamdEvent,
  Subscription,
} from "./types.js";
