import { join, resolve, sep } from "node:path";

/** The file under `root` a request path names, or undefined when it would leave `root`. */
export function staticPath(root: string, pathname: string): string | undefined {
  let decoded: string;
  try {
    decoded = decodeURIComponent(pathname);
  } catch {
    return undefined;
  }
  if (decoded.includes("\0")) return undefined;
  const path = resolve(join(root, decoded === "/" ? "index.html" : decoded));
  return path.startsWith(resolve(root) + sep) ? path : undefined;
}

if (import.meta.main) {
  const root = join(import.meta.dir, "dist");
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: Number(process.env.PORT ?? 5207),
    async fetch(request) {
      const path = staticPath(root, new URL(request.url).pathname);
      const file = path === undefined ? undefined : Bun.file(path);
      return file !== undefined && (await file.exists()) ? new Response(file) : new Response("Not found", { status: 404 });
    },
  });
  console.log(`streamd playground on ${server.url}`);
}
