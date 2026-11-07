import { $ } from "bun";

await $`rm -rf dist`;

const cli = await Bun.build({
  entrypoints: ["src/main.ts"],
  outdir: "dist",
  naming: "cli.js",
  format: "esm",
  target: "node",
  packages: "external",
  banner: "#!/usr/bin/env node",
});
if (!cli.success) throw new AggregateError(cli.logs, "cli build failed");
