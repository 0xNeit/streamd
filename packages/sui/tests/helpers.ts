import { Transaction } from "@mysten/sui/transactions";
import { toBase58 } from "@mysten/sui/utils";
import type { SuiLedgerClient } from "../src/index.js";
import type { CallTransaction } from "../src/calls.js";

/** A valid Sui digest: base58 of 32 bytes, all `n`. */
export const digest = (n: number) => toBase58(new Uint8Array(32).fill(n));

export const LONG = (n: string) => `0x${n.padStart(64, "0")}`;

export interface FakeEvent {
  cp: bigint;
  ti: bigint;
  ei: number;
  type: string;
  sender?: string;
  amount?: string;
}

/** A transaction `getTransaction` knows; `cp` undefined means executed but not yet checkpointed. */
export interface FakeTransaction {
  digest: string;
  cp?: bigint;
  ti: bigint;
  error?: string;
  events: { type: string; sender?: string; amount?: string }[];
}

export const str = (s: string) => ({ kind: { oneofKind: "stringValue" as const, stringValue: s } });

/**
 * Checkpoint n holds `sizes[n]` transactions; events are scanned in order. The
 * cursor is the next event's position as one byte; each event frame's
 * watermark is the checkpoint before it, and the last frame's the scan's end.
 */
export function fakeLedger(
  sizes: number[],
  events: FakeEvent[],
  fail?: () => unknown,
  prunedBelow = 0n,
  transactions: FakeTransaction[] = [],
): {
  client: SuiLedgerClient;
  calls: { checkpoints: string[]; list: Record<string, unknown>[]; transactions: string[] };
} {
  const totals = sizes.map((_, i) => BigInt(sizes.slice(0, i + 1).reduce((a, b) => a + b, 0)));
  const calls = { checkpoints: [] as string[], list: [] as Record<string, unknown>[], transactions: [] as string[] };
  const literalMatch = (e: FakeEvent, l: { predicate: { oneofKind: string; eventType?: { eventType: string }; sender?: { address: string } } }) =>
    l.predicate.oneofKind === "eventType" ? e.type === l.predicate.eventType!.eventType : e.sender === l.predicate.sender!.address;
  const ledger: SuiLedgerClient["ledgerService"] = {
    listEvents(input) {
      calls.list.push(input as unknown as Record<string, unknown>);
      async function* responses() {
        const err = fail?.();
        if (err !== undefined) throw err;
        if (input.startCheckpoint !== undefined && input.startCheckpoint < prunedBelow) {
          throw Object.assign(new Error("requested data below earliest available"), { code: "OUT_OF_RANGE" });
        }
        const start = input.options.after !== undefined ? input.options.after[0]! : 0;
        // The end bound is exclusive, as the proto documents.
        const last = input.endCheckpoint === undefined ? BigInt(sizes.length - 1) : input.endCheckpoint - 1n;
        let sent = 0;
        let i = start;
        for (; i < events.length; i++) {
          const e = events[i]!;
          if (input.startCheckpoint !== undefined && e.cp < input.startCheckpoint) continue;
          if (e.cp > last) break;
          if (!input.filter!.terms.some((t) => t.literals.every((l) => literalMatch(e, l)))) continue;
          if (sent === input.options.limit) {
            yield { watermark: { cursor: new Uint8Array([i]), checkpoint: e.cp - 1n }, end: { reason: 1 } };
            return;
          }
          sent++;
          yield {
            event: {
              eventType: e.type,
              ...(e.sender !== undefined && { sender: e.sender }),
              json: { kind: { oneofKind: "structValue" as const, structValue: { fields: { amount: str(e.amount ?? "1") } } } },
              checkpoint: e.cp,
              transactionDigest: `D${e.cp}t${e.ti}`,
              transactionIndex: e.ti,
              eventIndex: e.ei,
            },
            watermark: { cursor: new Uint8Array([i + 1]), checkpoint: e.cp - 1n },
          };
        }
        yield { watermark: { cursor: new Uint8Array([i]), checkpoint: last }, end: { reason: 3 } };
      }
      return { responses: responses() };
    },
    async getTransaction({ digest }) {
      calls.transactions.push(digest);
      const err = fail?.();
      if (err !== undefined) throw err;
      const t = transactions.find((x) => x.digest === digest);
      if (t === undefined) throw Object.assign(new Error(`Transaction ${digest} not found`), { code: "NOT_FOUND" });
      const status = t.error === undefined ? { success: true } : { success: false, error: { description: t.error } };
      const json = (amount = "1") => ({ kind: { oneofKind: "structValue" as const, structValue: { fields: { amount: str(amount) } } } });
      return {
        response: {
          transaction: {
            digest,
            ...(t.cp !== undefined && { checkpoint: t.cp }),
            effects: { status },
            events: { events: t.events.map((e) => ({ eventType: e.type, ...(e.sender !== undefined && { sender: e.sender }), json: json(e.amount) })) },
          },
        },
      };
    },
    async getCheckpoint({ checkpointId, readMask }) {
      const seq = checkpointId.oneofKind === "sequenceNumber" ? checkpointId.sequenceNumber : BigInt(sizes.length - 1);
      calls.checkpoints.push(String(checkpointId.oneofKind === undefined ? "latest" : seq));
      if (seq < prunedBelow || seq >= BigInt(sizes.length)) throw Object.assign(new Error(`Checkpoint ${seq} not found`), { code: "NOT_FOUND" });
      return {
        response: {
          checkpoint: {
            sequenceNumber: seq,
            summary: { totalNetworkTransactions: totals[Number(seq)]!, timestamp: { seconds: 1_700_000_000n + seq, nanos: 5_000 } },
            ...(readMask.paths.includes("transactions.digest") && {
              transactions: Array.from({ length: sizes[Number(seq)]! }, (_, i) => ({
                digest: transactions.find((t) => t.cp === seq && t.ti === BigInt(i))?.digest ?? `other${i}`,
              })),
            }),
          },
        },
      };
    },
    async getServiceInfo() {
      return { response: { checkpointHeight: BigInt(sizes.length - 1), lowestAvailableCheckpoint: 0n } };
    },
  };
  return { client: { ledgerService: ledger }, calls };
}

/** Built transaction bytes with every input and the gas resolved, so building needs no node. */
export async function transactionBytes(amount = 1_000_000_000n): Promise<Uint8Array> {
  const tx = new Transaction();
  tx.setSender(LONG("a11ce"));
  tx.setGasPrice(1000n);
  tx.setGasBudget(10_000_000n);
  tx.setGasPayment([{ objectId: LONG("9a5"), version: "1", digest: digest(4) }]);
  const [coin] = tx.splitCoins(tx.gas, [tx.pure.u64(amount)]);
  tx.transferObjects([coin!], tx.pure.address(LONG("b0b")));
  return tx.build();
}

export interface FakeSimulation {
  error?: string;
  events: { type: string; sender?: string; amount?: string }[];
}

/** `client` with a `transactionExecutionService` answering every simulation with `result`, or throwing `fail()`. */
export function withSimulator(client: SuiLedgerClient, result: FakeSimulation, fail?: () => unknown) {
  const requests: unknown[] = [];
  const json = (amount = "1") => ({ kind: { oneofKind: "structValue" as const, structValue: { fields: { amount: str(amount) } } } });
  const simulating: SuiLedgerClient = {
    ...client,
    transactionExecutionService: {
      async simulateTransaction(input) {
        requests.push(input);
        const err = fail?.();
        if (err !== undefined) throw err;
        const status = result.error === undefined ? { success: true } : { success: false, error: { description: result.error } };
        return {
          response: {
            transaction: {
              effects: { status },
              events: { events: result.events.map((e) => ({ eventType: e.type, ...(e.sender !== undefined && { sender: e.sender }), json: json(e.amount) })) },
            },
          },
        };
      },
    },
  };
  return { client: simulating, requests };
}

/** A transaction `listTransactions` serves: its place, signer, gas payer and net gas, and balance changes. */
export interface FakeListed {
  cp: bigint;
  ti: bigint;
  sender?: string;
  gas?: bigint;
  events?: number;
  changes: [address: string, coinType: string, amount: bigint][];
}

/**
 * `client` with a `listTransactions` over `listed`, paged as `listEvents` is
 * in `fakeLedger` (`sizes` must match it), and a `stateService` answering
 * from `balances` (`owner:coinType` keys) and `coins`.
 */
export function withTransfers(
  client: SuiLedgerClient,
  sizes: number[],
  listed: FakeListed[],
  balances = new Map<string, bigint>(),
  coins = new Map<string, { symbol: string; decimals: number }>(),
) {
  const calls = { list: [] as Record<string, unknown>[], balances: [] as string[], coins: [] as string[] };
  const ledgerService: SuiLedgerClient["ledgerService"] = {
    ...client.ledgerService,
    listTransactions(input) {
      calls.list.push(input as unknown as Record<string, unknown>);
      async function* responses() {
        const start = input.options.after !== undefined ? input.options.after[0]! : 0;
        const last = input.endCheckpoint === undefined ? BigInt(sizes.length - 1) : input.endCheckpoint - 1n;
        const address = input.filter?.terms[0]?.literals[0]?.predicate.affectedAddress.address;
        let sent = 0;
        let i = start;
        for (; i < listed.length; i++) {
          const t = listed[i]!;
          if (input.startCheckpoint !== undefined && t.cp < input.startCheckpoint) continue;
          if (t.cp > last) break;
          if (address !== undefined && !t.changes.some(([a]) => a === address) && t.sender !== address) continue;
          if (sent === input.options.limit) {
            yield { watermark: { cursor: new Uint8Array([i]), checkpoint: t.cp - 1n }, end: { reason: 1 } };
            return;
          }
          sent++;
          yield {
            transaction: {
              digest: `D${t.cp}t${t.ti}`,
              checkpoint: t.cp,
              transactionIndex: t.ti,
              timestamp: { seconds: 1_700_000_000n + t.cp, nanos: 0 },
              transaction: { sender: t.sender ?? LONG("a11ce"), gasPayment: { owner: t.sender ?? LONG("a11ce") } },
              effects: { gasUsed: { computationCost: t.gas ?? 0n, storageCost: 0n, storageRebate: 0n } },
              balanceChanges: t.changes.map(([a, coinType, amount]) => ({ address: a, coinType, amount: String(amount) })),
              events: { events: Array.from({ length: t.events ?? 0 }, () => ({})) },
            },
            watermark: { cursor: new Uint8Array([i + 1]), checkpoint: t.cp - 1n },
          };
        }
        yield { watermark: { cursor: new Uint8Array([i]), checkpoint: last }, end: { reason: 3 } };
      }
      return { responses: responses() };
    },
  };
  const stateService: NonNullable<SuiLedgerClient["stateService"]> = {
    async getBalance({ owner, coinType }) {
      calls.balances.push(`${owner}:${coinType}`);
      return { response: { balance: { balance: balances.get(`${owner}:${coinType}`) ?? 0n } } };
    },
    async getCoinInfo({ coinType }) {
      calls.coins.push(coinType);
      const coin = coins.get(coinType);
      if (coin === undefined) throw Object.assign(new Error(`Coin type ${coinType} not found`), { code: "NOT_FOUND" });
      return { response: { metadata: coin } };
    },
  };
  return { client: { ...client, ledgerService, stateService } as SuiLedgerClient, calls };
}

export const grpcError = (code: string, message = code) => Object.assign(new Error(message), { code });

/**
 * A name service answering `lookupName` from `names` (a target address, `null`
 * for a record with none, or an error to throw) and `reverseLookupName` from
 * `reverse`; anything unlisted is `NOT_FOUND`.
 */
export function fakeNames(names: Record<string, string | null | Error>, reverse: Record<string, string | Error> = {}) {
  const calls = { lookups: [] as string[], reverse: [] as string[] };
  const answer = <T>(value: T | Error | undefined, wrap: (v: T) => object) => {
    if (value instanceof Error) throw value;
    if (value === undefined) throw grpcError("NOT_FOUND");
    return { response: { record: wrap(value) } };
  };
  const nameService = {
    lookupName: async ({ name }: { name?: string }) => {
      calls.lookups.push(name!);
      return answer(names[name!], (target: string | null) => ({ name, ...(target !== null && { targetAddress: target }) }));
    },
    reverseLookupName: async ({ address }: { address?: string }) => {
      calls.reverse.push(address!);
      return answer(reverse[address!], (name: string) => ({ name }));
    },
  };
  return { nameService, calls };
}

/** A `MoveCall` command as the node returns it. */
export type Command = { package?: string; module?: string; function?: string; typeArguments?: string[]; arguments?: object[] };

/** A programmable transaction of `commands`, `"split"` standing for a command that is not a call. */
export function ptb(commands: (Command | "split")[], inputs: object[] = []): CallTransaction {
  return {
    kind: {
      data: {
        oneofKind: "programmableTransaction",
        programmableTransaction: {
          inputs,
          commands: commands.map((c) =>
            c === "split" ? { command: { oneofKind: "splitCoins" } } : { command: { oneofKind: "moveCall", moveCall: c } },
          ),
        },
      },
    },
  };
}
