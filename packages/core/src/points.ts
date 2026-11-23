import { StreamdError } from "./errors.js";
import { MISSING, isThenable, normalizeAddress, parseFieldPath, readField, type FieldPath } from "./filter.js";
import type { Fold } from "./reducers.js";
import type { StreamdEvent } from "./types.js";

/** One way to earn points: every matching event awards its address. */
export interface PointsRule<TEvent = StreamdEvent> {
  /** Unique and non-empty; names the rule in `Score.awards`. */
  id: string;
  /** Synchronous, since a reducer is; a returned thenable throws. */
  where?: (event: TEvent) => boolean;
  /** Who earns: the transaction's `sender` (default) or an address at a path in `event.data`. */
  address?: FieldPath | "sender";
  /** Points per award, `1n` by default; may be negative. */
  points?: bigint | ((event: TEvent) => bigint);
  /** Added once, on the address's first award by this rule. */
  firstTime?: bigint;
  /** Awards per address by this rule, a positive integer. */
  limit?: number;
}

/** A named score to reach, recorded once in `Score.reached`. */
export interface PointsThreshold {
  id: string;
  points: bigint;
}

/** Scores keep `keep` of their points per whole `interval` of chain time (microseconds). */
export interface PointsDecay {
  interval: bigint;
  keep: { numerator: bigint; denominator: bigint };
}

/** Options for `points()`. */
export interface PointsOptions<TEvent = StreamdEvent> {
  rules: readonly PointsRule<TEvent>[];
  thresholds?: readonly PointsThreshold[];
  decay?: PointsDecay;
}

/** One address's standing. */
export interface Score {
  /** Long form. */
  address: string;
  points: bigint;
  /** Rule id to awards made. */
  awards: Record<string, number>;
  /** Threshold id to the version that first reached it; never removed. */
  reached: Record<string, bigint>;
  /** The last event that changed it. */
  version: bigint;
  /** The decay clock, set only when decay is configured. */
  updatedAt?: bigint;
}

/** Scores keyed by long-form address. */
export type PointsState = ReadonlyMap<string, Score>;

/** A `points()` fold. */
export type PointsFold<TEvent = StreamdEvent> = Fold<PointsState, unknown, TEvent>;

interface Rule<TEvent> {
  id: string;
  where?: (event: TEvent) => boolean;
  address: (string | number)[] | "sender";
  points: bigint | ((event: TEvent) => bigint);
  firstTime?: bigint;
  limit?: number;
}

function refuse(message: string): never {
  throw new StreamdError(`points: ${message}`);
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function requireRules<TEvent>(rules: unknown): Rule<TEvent>[] {
  if (!Array.isArray(rules) || rules.length === 0) refuse("rules must be a non-empty array");
  const ids = new Set<string>();
  return rules.map((rule: PointsRule<TEvent>, i) => {
    if (typeof rule !== "object" || rule === null) refuse(`rules[${i}] must be an object`);
    const { id, where, address = "sender", points = 1n, firstTime, limit } = rule;
    if (typeof id !== "string" || id.length === 0) refuse(`rules[${i}].id must be a non-empty string`);
    if (ids.has(id)) refuse(`rule id ${JSON.stringify(id)} is used twice`);
    ids.add(id);
    if (where !== undefined && typeof where !== "function") refuse(`rule ${id}: where must be a function`);
    if (typeof points !== "bigint" && typeof points !== "function") {
      refuse(`rule ${id}: points must be a bigint or a function`);
    }
    if (firstTime !== undefined && typeof firstTime !== "bigint") refuse(`rule ${id}: firstTime must be a bigint`);
    if (limit !== undefined && !isPositiveInteger(limit)) refuse(`rule ${id}: limit must be a positive integer`);
    return {
      id,
      where,
      address: address === "sender" ? "sender" : parseFieldPath(address),
      points,
      firstTime,
      limit,
    };
  });
}

function requireThresholds(thresholds: unknown): PointsThreshold[] {
  if (thresholds === undefined) return [];
  if (!Array.isArray(thresholds)) refuse("thresholds must be an array");
  const ids = new Set<string>();
  return thresholds.map((t: PointsThreshold, i) => {
    if (typeof t !== "object" || t === null) refuse(`thresholds[${i}] must be an object`);
    if (typeof t.id !== "string" || t.id.length === 0) refuse(`thresholds[${i}].id must be a non-empty string`);
    if (ids.has(t.id)) refuse(`threshold id ${JSON.stringify(t.id)} is used twice`);
    ids.add(t.id);
    if (typeof t.points !== "bigint") refuse(`threshold ${t.id}: points must be a bigint`);
    return { id: t.id, points: t.points };
  });
}

function requireDecay(decay: unknown): PointsDecay | undefined {
  if (decay === undefined) return undefined;
  const d = decay as PointsDecay;
  if (typeof d !== "object" || d === null) refuse("decay must be an object");
  if (typeof d.interval !== "bigint" || d.interval <= 0n) refuse("decay.interval must be a positive bigint");
  const { numerator, denominator } = d.keep ?? ({} as PointsDecay["keep"]);
  if (typeof numerator !== "bigint" || typeof denominator !== "bigint" || denominator <= 0n) {
    refuse("decay.keep must be { numerator, denominator } bigints with a positive denominator");
  }
  if (numerator < 0n || numerator > denominator) refuse("decay.keep must be between 0 and 1");
  return { interval: d.interval, keep: { numerator, denominator } };
}

/** `score`'s points and clock after the whole intervals up to `at`. */
function decayTo(score: Score, at: bigint, decay: PointsDecay): { points: bigint; updatedAt: bigint } {
  const since = score.updatedAt ?? at;
  const k = at > since ? (at - since) / decay.interval : 0n;
  if (k === 0n) return { points: score.points, updatedAt: since };
  const { numerator, denominator } = decay.keep;
  // ponytail: exact powers cost grows with k (idle intervals); cap k once floor reaches zero if gaps get huge.
  const points =
    score.points === 0n || numerator === denominator ? score.points
    : numerator === 0n ? 0n
    : (score.points * numerator ** k) / denominator ** k;
  return { points, updatedAt: since + k * decay.interval };
}

function addressOf<TEvent>(rule: Rule<TEvent>, event: StreamdEvent): string | undefined {
  const raw = rule.address === "sender" ? event.sender ?? MISSING : readField(event.data, rule.address);
  if (raw === MISSING || raw == null) return undefined;
  const address = typeof raw === "string" ? normalizeAddress(raw) : undefined;
  if (address === undefined) refuse(`rule ${rule.id}: address ${JSON.stringify(String(raw))} is not an address`);
  return address;
}

/**
 * Per-address scores from `rules`, with first-time bonuses, per-rule limits,
 * thresholds reached and exact decay over chain time. Pass to `reduce()` as
 * `reduce(d, fold, fold.initial)`.
 */
export function points<TEvent extends StreamdEvent = StreamdEvent>(options: PointsOptions<TEvent>): PointsFold<TEvent> {
  if (typeof options !== "object" || options === null) refuse("options must be an object");
  const rules = requireRules<TEvent>(options.rules);
  const thresholds = requireThresholds(options.thresholds);
  const decay = requireDecay(options.decay);

  const award = (current: Score | undefined, address: string, rule: Rule<TEvent>, event: TEvent): Score | undefined => {
    const made = current?.awards[rule.id] ?? 0;
    if (rule.limit !== undefined && made >= rule.limit) return undefined;
    let amount = typeof rule.points === "function" ? rule.points(event) : rule.points;
    if (typeof amount !== "bigint") refuse(`rule ${rule.id}: points returned a non-bigint`);
    if (made === 0 && rule.firstTime !== undefined) amount += rule.firstTime;
    let base = current?.points ?? 0n;
    let updatedAt: bigint | undefined;
    if (decay) {
      if (event.timestamp === undefined) refuse(`rule ${rule.id}: an event without a timestamp cannot decay`);
      ({ points: base, updatedAt } = current ? decayTo(current, event.timestamp, decay) : { points: 0n, updatedAt: event.timestamp });
    }
    const total = base + amount;
    const reached = { ...current?.reached };
    for (const t of thresholds) if (!(t.id in reached) && total >= t.points) reached[t.id] = event.version;
    const score: Score = { address, points: total, awards: { ...current?.awards, [rule.id]: made + 1 }, reached, version: event.version };
    if (updatedAt !== undefined) score.updatedAt = updatedAt;
    return score;
  };

  const reducer = (state: PointsState, event: TEvent): PointsState => {
    let next: Map<string, Score> | undefined;
    for (const rule of rules) {
      if (rule.where) {
        const matched: unknown = rule.where(event);
        if (isThenable(matched)) refuse(`rule ${rule.id}: where must be synchronous`);
        if (!matched) continue;
      }
      const address = addressOf(rule, event);
      if (address === undefined) continue;
      const score = award((next ?? state).get(address), address, rule, event);
      if (score) (next ??= new Map(state)).set(address, score);
    }
    return next ?? state;
  };

  return Object.assign(reducer, { initial: new Map() as PointsState });
}
