import type { Client, ClientRequest, ClientResponse } from "@aptos-labs/ts-sdk";

/**
 * An SDK client sending each request once, through fetch, abandoned after
 * `timeout` ms. The SDK's own client retries a 429 twice, waiting out its
 * Retry-After, which hides the throttling and spends the rate limit doing so;
 * its browser client parses every body as JSON, so a plain-text 429 loses its
 * status there.
 */
export function sendOnce(timeout?: number): Client {
  return { provider: (request) => send(request, timeout) } as Client;
}

async function send(request: ClientRequest<unknown>, timeout: number | undefined): Promise<ClientResponse<unknown>> {
  const url = new URL(request.url);
  for (const [key, value] of Object.entries(request.params ?? {})) {
    if (value !== undefined) url.searchParams.set(key, String(value));
  }
  const headers = Object.fromEntries(Object.entries(request.headers ?? {}).filter(([, value]) => value !== undefined));
  const body = request.body === undefined ? undefined : request.body instanceof Uint8Array ? request.body : JSON.stringify(request.body);
  const response = await fetch(url, {
    method: request.method,
    headers,
    ...(body !== undefined && { body }),
    ...(timeout !== undefined && { signal: AbortSignal.timeout(timeout) }),
  });
  const text = await response.text();
  let data: unknown = text;
  try {
    data = JSON.parse(text);
  } catch {
    // Not JSON, such as a plain-text 429: the SDK reports the status either way.
  }
  return { status: response.status, statusText: response.statusText, data, headers: Object.fromEntries(response.headers) };
}
