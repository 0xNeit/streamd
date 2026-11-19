import { $ } from "bun";

await $`rm -rf dist`;

const result = await Bun.build({
  entrypoints: ["src/index.html"],
  outdir: "dist",
  target: "browser",
  minify: true,
  sourcemap: "linked",
  define: { "process.env.NODE_ENV": '"production"' },
});
if (!result.success) throw new AggregateError(result.logs, "playground build failed");
