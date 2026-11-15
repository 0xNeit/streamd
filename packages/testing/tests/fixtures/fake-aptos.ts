#!/usr/bin/env bun
// Stands in for the aptos CLI. FAKE_APTOS picks the behaviour: ready (default), exit, never, stubborn.
const args = process.argv.slice(2);
const mode = process.env.FAKE_APTOS ?? "ready";
const flag = (name: string) => args[args.indexOf(name) + 1];

if (args[0] === "node" && args[1] === "run-localnet") {
  console.log(`args ${JSON.stringify(args)}`);
  if (mode === "exit") {
    console.error("error binding to 127.0.0.1:9101: Address already in use");
    process.exit(1);
  }
  const startedAt = Date.now();
  const server = Bun.serve({
    port: Number(flag("--ready-server-listen-port")),
    hostname: "127.0.0.1",
    fetch: () =>
      Response.json({ ready: [], not_ready: mode === "never" || Date.now() - startedAt < 300 ? [{ NodeApi: "starting" }] : [] }),
  });
  process.on("SIGINT", () => {
    if (mode === "stubborn") return;
    console.log("Done, goodbye!");
    server.stop(true);
    process.exit(0);
  });
}
