import { afterEach, describe, expect, it } from "bun:test";
import { sendOnce } from "../src/send.js";

const realFetch = globalThis.fetch;
afterEach(() => {
  globalThis.fetch = realFetch;
});

describe("sendOnce", () => {
  it("encodes params, drops unset headers, falls back to text, and abandons a request after the timeout", async () => {
    const requests: Request[] = [];
    globalThis.fetch = (async (input: string | URL, init?: RequestInit) => {
      requests.push(new Request(String(input), init));
      if (init?.signal) await new Promise((_, reject) => init.signal!.addEventListener("abort", () => reject(init.signal!.reason)));
      return new Response("not json", { status: 502, statusText: "Bad Gateway" });
    }) as unknown as typeof fetch;
    const { provider } = sendOnce();
    const response = await provider({ url: "https://a.example/v1/x", method: "POST", params: { start: 5n, skip: undefined }, headers: { a: "1", b: undefined }, body: { k: 1 } });
    expect(response).toMatchObject({ status: 502, statusText: "Bad Gateway", data: "not json" });
    expect(requests[0]!.url).toBe("https://a.example/v1/x?start=5");
    expect(requests[0]!.headers.get("a")).toBe("1");
    expect(requests[0]!.headers.has("b")).toBe(false);
    expect(await requests[0]!.text()).toBe('{"k":1}');
    await expect(sendOnce(20).provider({ url: "https://a.example/v1", method: "GET" })).rejects.toMatchObject({ name: "TimeoutError" });
  });

  it("parses JSON, keeps the method, response headers and a binary body as given", async () => {
    let seen: Request | undefined;
    globalThis.fetch = (async (input: string | URL, init?: RequestInit) => {
      seen = new Request(String(input), init);
      return Response.json({ ok: true }, { headers: { "retry-after": "3" } });
    }) as unknown as typeof fetch;
    const bytes = new Uint8Array([1, 2, 255]);
    const response = await sendOnce().provider({ url: "https://a.example/v1/simulate", method: "POST", body: bytes });
    expect(response).toMatchObject({ status: 200, data: { ok: true } });
    expect((response.headers as Record<string, string>)["retry-after"]).toBe("3");
    expect(seen!.method).toBe("POST");
    expect(new Uint8Array(await seen!.arrayBuffer())).toEqual(bytes);
    await sendOnce().provider({ url: "https://a.example/v1", method: "GET" });
    expect(seen!.method).toBe("GET");
  });
});
