import { useEffect, useMemo, useState } from "react";
import type { FormEvent } from "react";
import { checkDescriptor } from "@streamd/aptos";
import type { AptosClient, DescriptorCheck, EventTypeStatus } from "@streamd/aptos";
import { serializeEvent } from "@streamd/core";
import type { ModuleEventDescriptor } from "@streamd/core";
import { useEvents } from "@streamd/react";
import { clientFor, describeError, NETWORKS } from "./client.js";
import type { PlaygroundNetwork } from "./client.js";
import { browserStorage, rememberedKey, storeKey } from "./settings.js";
import { descriptorName, snippet } from "./snippet.js";
import { generateFor, moduleOf } from "./types.js";

const EXAMPLE = "0x1::fungible_asset::Deposit";
const MAX_EVENTS = 25;

const STATUS_TEXT: Partial<Record<EventTypeStatus, string>> = {
  event: "a module event: it can be followed",
  legacy: "a legacy event struct, emitted through an EventHandle: it can be followed",
  "not-event": "a struct, but not one declared #[event]: nothing emits it",
  "missing-module": "no such module is published at this address",
  "missing-struct": "the module has no struct of this name",
};

interface Run {
  eventType: string;
  network: PlaygroundNetwork;
  keyed: boolean;
  client: AptosClient;
  check?: DescriptorCheck;
  checkError?: string;
  source?: string;
  sourceError?: string;
}

const storage = browserStorage();

export function App() {
  const [network, setNetwork] = useState<PlaygroundNetwork>("mainnet");
  const [apiKey, setApiKey] = useState(() => rememberedKey(storage));
  const [remember, setRemember] = useState(() => rememberedKey(storage) !== "");
  const [eventType, setEventType] = useState(EXAMPLE);
  const [run, setRun] = useState<Run>();
  const [following, setFollowing] = useState(false);

  useEffect(() => storeKey(storage, apiKey, remember), [apiKey, remember]);
  useEffect(() => () => run?.client.destroy(), [run?.client]);

  async function submit(e: FormEvent) {
    e.preventDefault();
    const type = eventType.trim();
    const { aptos, client } = clientFor(network, apiKey);
    const keyed = apiKey.trim() !== "";
    const base: Run = { eventType: type, network, keyed, client };
    setFollowing(false);
    setRun(base);
    let check: DescriptorCheck;
    try {
      check = await checkDescriptor(aptos, { kind: "module", eventType: type as ModuleEventDescriptor["eventType"] });
    } catch (err) {
      setRun({ ...base, checkError: describeError(err, keyed) });
      return;
    }
    setRun({ ...base, check });
    if (!check.ok) return;
    setFollowing(true);
    const generated = await generateFor(aptos, type).then(
      (source) => ({ source }),
      (err) => ({ sourceError: describeError(err, keyed) }),
    );
    setRun((current) => (current?.client === client ? { ...current, ...generated } : current));
  }

  const checking = run !== undefined && run.check === undefined && run.checkError === undefined;
  const code =
    run?.check?.ok === true && (run.source !== undefined || run.sourceError !== undefined)
      ? snippet({ network: run.network, eventType: run.eventType, ...withName(run) })
      : undefined;

  return (
    <main>
      <header>
        <h1>streamd playground</h1>
        <p>Check a Move event type, watch it live, and copy the types and code to follow it yourself.</p>
      </header>

      <form onSubmit={submit} aria-labelledby="form-title">
        <h2 id="form-title" className="visually-hidden">Event type</h2>
        <div className="field">
          <label htmlFor="network">Network</label>
          <select id="network" value={network} onChange={(e) => setNetwork(e.target.value as PlaygroundNetwork)}>
            {Object.entries(NETWORKS).map(([value, { label }]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
        </div>
        <div className="field">
          <label htmlFor="api-key">API key (optional)</label>
          <input
            id="api-key"
            type="password"
            autoComplete="off"
            spellCheck={false}
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            aria-describedby="api-key-help"
          />
          <p id="api-key-help" className="help">
            Sent only to the Aptos node. Without one, the public node rate limits this browser.
          </p>
          <label className="check">
            <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} />
            Remember the key on this device
          </label>
        </div>
        <div className="field wide">
          <label htmlFor="event-type">Event type</label>
          <input
            id="event-type"
            required
            spellCheck={false}
            autoCapitalize="off"
            autoCorrect="off"
            placeholder={EXAMPLE}
            value={eventType}
            onChange={(e) => setEventType(e.target.value)}
          />
        </div>
        <button type="submit" disabled={checking}>
          {checking ? "Checking" : "Check"}
        </button>
      </form>

      <section aria-labelledby="check-title" aria-live="polite" aria-busy={checking}>
        <h2 id="check-title">Check</h2>
        {run === undefined && <p className="muted">Enter an event type and press Check.</p>}
        {checking && <p className="muted">Reading the module ABI.</p>}
        {run?.checkError !== undefined && <p className="error">{run.checkError}</p>}
        {run?.check?.types.map((t) => (
          <p key={t.eventType} className={run.check!.ok ? "ok" : "error"}>
            <code>{t.eventType}</code> is {STATUS_TEXT[t.status] ?? t.status}.
          </p>
        ))}
      </section>

      <div className="columns">
        {run?.check?.ok ? (
          <LiveEvents run={run} following={following} onToggle={() => setFollowing((f) => !f)} />
        ) : (
          <section aria-labelledby="live-title">
            <h2 id="live-title">Live events</h2>
            <p className="muted">Followed after a passing check.</p>
          </section>
        )}

        <section aria-labelledby="types-title">
          <div className="section-head">
            <h2 id="types-title">Generated types</h2>
            {run?.source !== undefined && <CopyButton text={run.source} label="types" />}
          </div>
          {run?.sourceError !== undefined && <p className="error">{run.sourceError}</p>}
          {run?.source !== undefined ? (
            <>
              <p className="muted">
                What <code>streamd codegen {moduleName(run.eventType)}</code> writes. Save it as{" "}
                <code>events.ts</code>.
              </p>
              <pre className="code" tabIndex={0} aria-label="Generated TypeScript">
                {run.source}
              </pre>
            </>
          ) : (
            <p className="muted">Generated after a passing check.</p>
          )}
        </section>
      </div>

      <section aria-labelledby="snippet-title">
        <div className="section-head">
          <h2 id="snippet-title">Snippet</h2>
          {code !== undefined && <CopyButton text={code} label="snippet" />}
        </div>
        {code !== undefined ? (
          <pre className="code" tabIndex={0} aria-label="Snippet">
            {code}
          </pre>
        ) : (
          <p className="muted">Written after a passing check.</p>
        )}
      </section>
    </main>
  );
}

function LiveEvents({ run, following, onToggle }: { run: Run; following: boolean; onToggle: () => void }) {
  const descriptor = useMemo<ModuleEventDescriptor | null>(
    () => (following ? { kind: "module", eventType: run.eventType as ModuleEventDescriptor["eventType"] } : null),
    [run.eventType, following],
  );
  const live = useEvents(descriptor, { client: run.client, maxEvents: MAX_EVENTS });
  return (
    <section aria-labelledby="live-title">
      <div className="section-head">
        <h2 id="live-title">Live events</h2>
        <button type="button" className="secondary" onClick={onToggle}>
          {following ? "Stop" : "Start"}
        </button>
      </div>
      <p role="status" className="muted">
        {liveStatus(live.status, live.events.length)}
      </p>
      {live.error !== undefined && (
        <p role="alert" className="error">
          {describeError(live.error, run.keyed)}
        </p>
      )}
      <ol className="events" aria-label="Most recent events, newest first">
        {[...live.events].reverse().map((event) => (
          <li key={`${event.version}:${event.eventIndex}`}>
            <pre>{JSON.stringify(JSON.parse(serializeEvent(event)), null, 2)}</pre>
          </li>
        ))}
      </ol>
    </section>
  );
}

function moduleName(eventType: string): string {
  const { address, module } = moduleOf(eventType);
  return `${address}::${module}`;
}

function withName(run: Run): { name?: string } {
  const name = run.source === undefined ? undefined : descriptorName(run.source, run.eventType);
  return name === undefined ? {} : { name };
}

function liveStatus(status: string, count: number): string {
  if (status === "idle") return "Not following.";
  if (status === "loading") return "Starting at the head of the chain.";
  if (status === "stopped") return "Stopped after repeated errors. Press Stop, then Start, to try again.";
  return count === 0 ? "Following live. Waiting for the next event." : `Following live. ${count} most recent shown.`;
}

function CopyButton({ text, label }: { text: string; label: string }) {
  const [result, setResult] = useState("");
  useEffect(() => setResult(""), [text]);
  const copy = () =>
    Promise.resolve()
      .then(() => navigator.clipboard.writeText(text))
      .catch(() => {
        if (!copyBySelection(text)) throw new Error("not copied");
      })
      .then(
        () => setResult("Copied"),
        () => setResult("Copy failed: select the text and copy it"),
      );
  return (
    <span className="copy">
      <button type="button" className="secondary" onClick={copy}>
        Copy {label}
      </button>
      <span role="status">{result}</span>
    </span>
  );
}

// Where the async clipboard is refused (an embedding frame without the
// permission, an older browser), a selected textarea can still be copied.
function copyBySelection(text: string): boolean {
  const area = document.createElement("textarea");
  area.value = text;
  area.setAttribute("readonly", "");
  area.style.position = "fixed";
  area.style.opacity = "0";
  document.body.append(area);
  area.select();
  try {
    return document.execCommand("copy");
  } catch {
    return false;
  } finally {
    area.remove();
  }
}
