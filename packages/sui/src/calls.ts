import { normalizeSuiAddress, toBase64 } from "@mysten/sui/utils";
import type { EntryFunctionCall } from "@streamd/core";

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
