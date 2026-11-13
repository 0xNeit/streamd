import { every, fieldDecoder, fromFunction, fromSender, normalizeAddress, not, some, whereCompare, whereField } from "@streamd/core";
import type { CompareOp, EventPredicate } from "@streamd/core";

export interface WhereClause {
  path: string;
  op: CompareOp;
  value: string;
}

/** The `--sender`, `--function` and `--where` flags, as plain data so options stay comparable and printable. */
export interface CliFilter {
  /** Long form; any one of them signed the transaction. */
  senders: string[];
  /** Any one of them was called. */
  functions: string[];
  /** Every clause holds. */
  where: WhereClause[];
}

export interface FilterFlags {
  sender?: string[];
  function?: string[];
  where?: string[];
  decode?: string[];
}

export const FILTER_OPTIONS = {
  sender: { type: "string", multiple: true },
  function: { type: "string", multiple: true },
  where: { type: "string", multiple: true },
  decode: { type: "string", multiple: true },
} as const;

// Longest operators first, so `<=` is not read as `<` followed by `=5`.
const EXPRESSION = /^([^=!<>]*)(==|!=|<=|>=|=|<|>)(.*)$/;
const INTEGER = /^-?\d+$/;

function clause(raw: string): WhereClause {
  const match = EXPRESSION.exec(raw);
  const [, path = "", op = "", rest = ""] = match ?? [];
  const value = rest.trim();
  if (match === null || /^[=!<>]/.test(value)) {
    throw new Error(`--where must be <field><op><value>, with op one of ==, !=, <, <=, >, >=: ${raw}`);
  }
  const parsed: WhereClause = { path: path.trim(), op: op === "=" ? "==" : (op as CompareOp), value };
  try {
    wherePredicate(parsed);
  } catch (err) {
    throw new Error(`--where ${raw}: ${err instanceof Error ? err.message : String(err)}`);
  }
  return parsed;
}

function wherePredicate({ path, op, value }: WhereClause): EventPredicate {
  if (INTEGER.test(value)) return whereCompare(path, op, value);
  if (op !== "==" && op !== "!=") throw new Error(`${op} needs an integer value`);
  const equal = value === "true" || value === "false" ? some(whereField(path, value === "true"), whereField(path, value)) : whereField(path, value);
  return op === "==" ? equal : not(equal);
}

/** The filter the flags describe, or undefined when none is set; throws a message naming the flag. */
export function parseFilter(flags: FilterFlags): CliFilter | undefined {
  const { sender = [], function: functions = [], where = [] } = flags;
  if (sender.length + functions.length + where.length === 0) return undefined;
  const senders = sender.map((raw) => {
    const address = normalizeAddress(raw);
    if (address === undefined) throw new Error(`--sender must be an address: ${raw}`);
    return address;
  });
  for (const fn of functions) {
    try {
      fromFunction(fn);
    } catch (err) {
      throw new Error(`--function: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  return { senders, functions, where: where.map(clause) };
}

/** One predicate for the whole filter. */
export function filterPredicate(filter: CliFilter): EventPredicate {
  const predicates: EventPredicate[] = filter.where.map(wherePredicate);
  if (filter.functions.length > 0) predicates.unshift(fromFunction(...filter.functions));
  if (filter.senders.length > 0) predicates.unshift(some(...filter.senders.map(fromSender)));
  return every(...predicates);
}

/** The `--decode` paths, comma-separated and repeatable, in order; undefined when none is given. */
export function parseDecode(raw: string[] | undefined): string[] | undefined {
  if (raw === undefined || raw.length === 0) return undefined;
  const paths = raw.flatMap((entry) => entry.split(",").map((path) => path.trim()));
  try {
    fieldDecoder(paths);
  } catch (err) {
    throw new Error(`--decode: ${err instanceof Error ? err.message : String(err)}`);
  }
  return paths;
}
