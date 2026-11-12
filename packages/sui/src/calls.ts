import { normalizeSuiAddress, toBase64 } from "@mysten/sui/utils";
import { StreamdNetworkError } from "@streamd/core";
import type { EntryFunctionCall, StreamdEvent } from "@streamd/core";

/** The read-mask paths, under a transaction's own path, that `callsOf` reads. */
export const CALL_FIELDS = [
  "kind.programmable_transaction.commands",
  "kind.programmable_transaction.inputs.kind",
  "kind.programmable_transaction.inputs.pure",
  "kind.programmable_transaction.inputs.object_id",
];

interface Argument {
  kind?: number;
  input?: number;
  result?: number;
  subresult?: number;
}
interface MoveCall {
  package?: string;
  module?: string;
  function?: string;
  typeArguments?: string[];
  arguments?: Argument[];
}
interface Input {
  kind?: number;
  pure?: Uint8Array;
  objectId?: string;
}
/** A transaction as the node returns it under the `CALL_FIELDS` mask. */
export interface CallTransaction {
  kind?: {
    data?: {
      oneofKind?: string;
      programmableTransaction?: { inputs?: Input[]; commands?: { command?: { oneofKind?: string; moveCall?: MoveCall } }[] };
    };
  };
}

const GAS = 1;
const INPUT = 2;
const RESULT = 3;
const PURE = 1;
const OBJECT_INPUTS = new Set([2, 3, 4]);
const MOVE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const HEX_ADDRESS = /^0x[0-9a-fA-F]{1,64}$/;
const index = (n: unknown): n is number => Number.isSafeInteger(n) && (n as number) >= 0;

function argument(arg: Argument, inputs: Input[], commands: number): unknown {
  if (arg.kind === GAS) return "GasCoin";
  if (arg.kind === RESULT && index(arg.result) && arg.result < commands) {
    if (arg.subresult === undefined) return { Result: arg.result };
    if (index(arg.subresult)) return { NestedResult: [arg.result, arg.subresult] };
  }
  if (arg.kind === INPUT && index(arg.input) && arg.input < inputs.length) {
    const input = inputs[arg.input]!;
    if (input.kind === PURE && input.pure instanceof Uint8Array) return { Pure: toBase64(input.pure) };
    if (OBJECT_INPUTS.has(input.kind!) && HEX_ADDRESS.test(input.objectId ?? "")) return { Object: normalizeSuiAddress(input.objectId!) };
    return { Input: arg.input };
  }
  return undefined;
}

/** The command as an `EntryFunctionCall`, or `undefined` when any part is malformed. */
function toCall(call: MoveCall, inputs: Input[], commands: number): EntryFunctionCall | undefined {
  const { package: pkg, module, function: fn, typeArguments = [], arguments: args = [] } = call;
  if (!HEX_ADDRESS.test(pkg ?? "") || !MOVE_IDENTIFIER.test(module ?? "") || !MOVE_IDENTIFIER.test(fn ?? "")) return undefined;
  if (!Array.isArray(typeArguments) || !typeArguments.every((t) => typeof t === "string") || !Array.isArray(args)) return undefined;
  const resolved = args.map((a) => argument(a ?? {}, inputs, commands));
  if (resolved.includes(undefined)) return undefined;
  return { function: `${normalizeSuiAddress(pkg!)}::${module}::${fn}`, typeArguments, arguments: resolved };
}

/**
 * The call each event of `transaction` came from: an event names the package
 * and module of the `MoveCall` command whose top-level call emitted it, but
 * not the command, so it takes the one command matching both, and none when
 * no command or several match. Events of one command share one call.
 */
export function callsOf(transaction: CallTransaction | undefined): (packageId: string | undefined, module: string | undefined) => EntryFunctionCall | undefined {
  const data = transaction?.kind?.data;
  if (data?.oneofKind !== "programmableTransaction" || data.programmableTransaction === undefined) return () => undefined;
  const { inputs = [], commands = [] } = data.programmableTransaction;
  const byModule = new Map<string, EntryFunctionCall | undefined>();
  for (const { command } of commands) {
    const call = command?.oneofKind === "moveCall" ? command.moveCall : undefined;
    if (typeof call?.package !== "string" || typeof call.module !== "string") continue;
    const key = `${normalizeSuiAddress(call.package)}::${call.module}`;
    // A second command on the same module makes every event of that module ambiguous.
    byModule.set(key, byModule.has(key) ? undefined : toCall(call, inputs, commands.length));
  }
  return (packageId, module) =>
    typeof packageId !== "string" || typeof module !== "string" ? undefined : byModule.get(`${normalizeSuiAddress(packageId)}::${module}`);
}

/** What a raw event says about the command that emitted it. */
export interface EventOrigin {
  transactionDigest?: string;
  packageId?: string;
  module?: string;
}

/** The `BatchGetTransactions` call of `SuiGrpcClient`'s `ledgerService`. */
export type BatchGetTransactions = (input: { digests: string[]; readMask: { paths: string[] } }) => PromiseLike<{
  response: { transactions?: { result?: { oneofKind?: string; transaction?: { digest?: string; transaction?: CallTransaction } } }[] };
}>;

/** The node refuses a batch of more than 200 digests. */
const BATCH_LIMIT = 200;
const BATCH_FIELDS = ["digest", ...CALL_FIELDS.map((f) => `transaction.${f}`)];

/** `event` with the call `calls` finds for its origin, when there is one. */
export function withCall(event: StreamdEvent, calls: ReturnType<typeof callsOf>, origin: EventOrigin): StreamdEvent {
  const call = calls(origin.packageId, origin.module);
  if (call !== undefined) event.entryFunction = call;
  return event;
}

/**
 * Sets each event's call from its transaction, read in batches. A transaction
 * the node does not return fails the read, so a retry can find it rather than
 * the events silently carrying no call.
 */
export async function attachCalls(batch: BatchGetTransactions, events: [StreamdEvent, EventOrigin][]): Promise<void> {
  const digests = [...new Set(events.flatMap(([, o]) => (o.transactionDigest === undefined ? [] : [o.transactionDigest])))];
  const found = new Map<string, ReturnType<typeof callsOf>>();
  for (let i = 0; i < digests.length; i += BATCH_LIMIT) {
    const { response } = await batch({ digests: digests.slice(i, i + BATCH_LIMIT), readMask: { paths: BATCH_FIELDS } });
    for (const { result } of response.transactions ?? []) {
      const read = result?.oneofKind === "transaction" ? result.transaction : undefined;
      if (typeof read?.digest === "string") found.set(read.digest, callsOf(read.transaction));
    }
  }
  const missing = digests.find((d) => !found.has(d));
  if (missing !== undefined) throw new StreamdNetworkError(`Sui node did not return transaction ${missing}`);
  for (const [event, origin] of events) {
    if (origin.transactionDigest !== undefined) withCall(event, found.get(origin.transactionDigest)!, origin);
  }
}
