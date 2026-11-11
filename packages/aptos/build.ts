import { $, Glob } from "bun";

await $`rm -rf dist`;

for (const [format, name] of [["esm", "[name].js"], ["cjs", "[name].cjs"]] as const) {
  const result = await Bun.build({
    entrypoints: ["src/index.ts", "src/framework.ts"],
    outdir: "dist",
    naming: name,
    format,
    target: "node",
    packages: "external",
    sourcemap: "linked",
  });
  if (!result.success) throw new AggregateError(result.logs, `${format} build failed`);
}

await $`tsc -p tsconfig.build.json`;

// CJS consumers resolve types through .d.cts files, whose relative imports
// must point at other .d.cts files rather than the ESM declarations.
for await (const file of new Glob("**/*.d.ts").scan("dist")) {
  const dts = await Bun.file(`dist/${file}`).text();
  const dcts = dts.replace(/(["'])(\.{1,2}\/[^"']+)\.js\1/g, "$1$2.cjs$1");
  await Bun.write(`dist/${file.replace(/\.d\.ts$/, ".d.cts")}`, dcts);
}
