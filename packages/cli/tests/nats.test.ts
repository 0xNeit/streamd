import { afterEach, describe, expect, it } from "bun:test";
import type { Socket } from "bun";
import { connectNats } from "../src/nats.js";

interface Pub {
  subject: string;
  reply: string;
  headers: string;
  body: string;
}

// Enough of a NATS server for the client: INFO on open, PONG for PING, and
// each HPUB parsed by its byte counts and handed to `onPub`, which by default
// answers with a PubAck as a plain MSG.
function fakeServer(
  options: {
    info?: Record<string, unknown>;
    /** Answers CONNECT with this -ERR and closes, as nats-server does on bad credentials. */
    refuse?: string;
    onPub?(socket: Socket<unknown>, pub: Pub, n: number): void;
  } = {},
) {
  const lines: string[] = [];
  const pubs: Pub[] = [];
  let connections = 0;
  const buffers = new Map<Socket<unknown>, Buffer>();
  const onPub =
    options.onPub ??
    ((socket, pub, n) => {
      const ack = `{"stream":"S","seq":${n}}`;
      socket.write(`MSG ${pub.reply} 1 ${ack.length}\r\n${ack}\r\n`);
    });
  const server = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      open(socket) {
        connections++;
        socket.write(`INFO ${JSON.stringify({ server_id: "fake", headers: true, max_payload: 1048576, ...options.info })}\r\n`);
      },
      data(socket, data) {
        let buffer = Buffer.concat([buffers.get(socket) ?? Buffer.alloc(0), Buffer.from(data)]);
        for (;;) {
          const end = buffer.indexOf("\r\n");
          if (end < 0) break;
          const line = buffer.subarray(0, end).toString();
          if (line.startsWith("HPUB ")) {
            const [, subject, reply, headerBytes, total] = line.split(" ");
            if (buffer.length < end + 4 + Number(total)) break;
            const block = buffer.subarray(end + 2, end + 2 + Number(total));
            buffer = buffer.subarray(end + 4 + Number(total));
            lines.push(line);
            const pub = {
              subject: subject!,
              reply: reply!,
              headers: block.subarray(0, Number(headerBytes)).toString(),
              body: block.subarray(Number(headerBytes)).toString(),
            };
            pubs.push(pub);
            onPub(socket, pub, pubs.length);
            continue;
          }
          buffer = buffer.subarray(end + 2);
          lines.push(line);
          if (line.startsWith("CONNECT ") && options.refuse !== undefined) {
            socket.write(`-ERR '${options.refuse}'\r\n`);
            socket.end();
            buffer = Buffer.alloc(0);
            break;
          }
          if (line === "PING") socket.write("PONG\r\n");
        }
        buffers.set(socket, buffer);
      },
    },
  });
  const connect = () =>
    lines.filter((l) => l.startsWith("CONNECT ")).map((l) => JSON.parse(l.slice(8)) as Record<string, unknown>);
  return { server, lines, pubs, connect, connections: () => connections, url: `nats://127.0.0.1:${server.port}` };
}

let server: { stop(closeActiveConnections?: boolean): void } | undefined;
afterEach(() => server?.stop(true));

const never = new AbortController().signal;

describe("connectNats", () => {
  it("connects on the first publish, frames HPUB in UTF-8 bytes, and reads a PubAck", async () => {
    const fake = fakeServer();
    server = fake.server;
    const client = connectNats(fake.url);
    expect(fake.connections()).toBe(0);
    const body = '["dépôt"]';
    const reply = await client.request("aptos.deposits", { "Nats-Msg-Id": "abc" }, body, never);
    expect(reply).toEqual({ data: '{"stream":"S","seq":1}' });
    const block = "NATS/1.0\r\nNats-Msg-Id: abc\r\n\r\n";
    const pub = fake.lines.find((l) => l.startsWith("HPUB "))!;
    expect(pub).toMatch(new RegExp(`^HPUB aptos\\.deposits _INBOX\\.[0-9a-f]{22}\\.1 ${block.length} ${block.length + Buffer.byteLength(body)}$`));
    expect(Buffer.byteLength(body)).toBe(body.length + 2);
    expect(fake.pubs[0]).toMatchObject({ headers: block, body });
    const sub = fake.lines.find((l) => l.startsWith("SUB "))!;
    expect(pub.split(" ")[2]!.startsWith(sub.split(" ")[1]!.slice(0, -1))).toBe(true);
    expect(fake.connect()[0]).toMatchObject({ verbose: false, headers: true, no_responders: true, protocol: 1 });
    expect(fake.connect()[0]).not.toContainAnyKeys(["user", "pass", "auth_token"]);
    client.close();
  });

  it("sends a user and password, or a token, from the URL", async () => {
    const fake = fakeServer();
    server = fake.server;
    const port = fake.server.port;
    const withPassword = connectNats(`nats://str%40md:p%3Ass@127.0.0.1:${port}`);
    await withPassword.request("s", {}, "[]", never);
    const withToken = connectNats(`nats://t0ken@127.0.0.1:${port}`);
    await withToken.request("s", {}, "[]", never);
    expect(fake.connect()[0]).toMatchObject({ user: "str@md", pass: "p:ss" });
    expect(fake.connect()[1]).toMatchObject({ auth_token: "t0ken" });
    expect(fake.connect()[1]).not.toContainAnyKeys(["user", "pass"]);
    withPassword.close();
    withToken.close();
  });

  it("reads a status reply, and a reply split across reads", async () => {
    const fake = fakeServer({
      onPub(socket, pub, n) {
        if (n === 1) {
          socket.write(`HMSG ${pub.reply} 1 28 28\r\nNATS/1.0 503 No Responders\r\n\r\n\r\n`);
          return;
        }
        const ack = '{"stream":"S","seq":2,"duplicate":true}';
        const frame = `MSG ${pub.reply} 1 ${ack.length}\r\n${ack}\r\n`;
        socket.write(frame.slice(0, 20));
        setTimeout(() => socket.write(frame.slice(20, 50)), 10);
        setTimeout(() => socket.write(frame.slice(50)), 20);
      },
    });
    server = fake.server;
    const client = connectNats(fake.url);
    expect(await client.request("s", {}, "[]", never)).toEqual({ status: 503, description: "No Responders", data: "" });
    expect(await client.request("s", {}, "[]", never)).toEqual({ data: '{"stream":"S","seq":2,"duplicate":true}' });
    client.close();
  });

  it("answers the server's PING and ignores a reply for another publish", async () => {
    const fake = fakeServer({
      onPub(socket, pub) {
        const stale = pub.reply.replace(/\.\d+$/, ".99");
        socket.write(`PING\r\nMSG ${stale} 1 2\r\nno\r\n`);
        setTimeout(() => socket.write(`MSG ${pub.reply} 1 2\r\nok\r\n`), 20);
      },
    });
    server = fake.server;
    const client = connectNats(fake.url);
    expect(await client.request("s", {}, "[]", never)).toEqual({ data: "ok" });
    expect(fake.lines.filter((l) => l === "PONG")).toHaveLength(1);
    client.close();
  });

  it("rejects with the server's -ERR, and connects again on the next publish", async () => {
    const fake = fakeServer({
      onPub(socket, pub, n) {
        if (n === 1) socket.write("-ERR 'Permissions Violation for Publish to \"s\"'\r\n");
        else socket.write(`MSG ${pub.reply} 1 2\r\nok\r\n`);
      },
    });
    server = fake.server;
    const client = connectNats(fake.url);
    await expect(client.request("s", {}, "[]", never)).rejects.toThrow("NATS -ERR 'Permissions Violation for Publish to \"s\"'");
    expect(await client.request("s", {}, "[]", never)).toEqual({ data: "ok" });
    expect(fake.connections()).toBe(2);
    client.close();
  });

  it("rejects a refused handshake with the server's -ERR", async () => {
    const fake = fakeServer({ info: { auth_required: true }, refuse: "Authorization Violation" });
    server = fake.server;
    const client = connectNats(`nats://u:wrong@127.0.0.1:${fake.server.port}`);
    await expect(client.request("s", {}, "[]", never)).rejects.toThrow("NATS -ERR 'Authorization Violation'");
    expect(fake.pubs).toHaveLength(0);
    client.close();
  });

  it("connects again after close", async () => {
    const fake = fakeServer();
    server = fake.server;
    const client = connectNats(fake.url);
    await client.request("s", {}, "[]", never);
    client.close();
    await client.request("s", {}, "[]", never);
    expect(fake.connections()).toBe(2);
    client.close();
  });

  it("rejects a publish when the server closes the connection", async () => {
    const fake = fakeServer({ onPub: (socket) => socket.terminate() });
    server = fake.server;
    const client = connectNats(fake.url);
    await expect(client.request("s", {}, "[]", never)).rejects.toThrow(/NATS connection closed|ECONNRESET/);
    client.close();
  });

  it.each([
    [{ tls_required: true }, "the NATS server requires TLS, which this sink does not speak"],
    [{ headers: false }, "the NATS server does not support headers (NATS 2.2 or later)"],
  ])("refuses a server whose INFO is %j", async (info, text) => {
    const fake = fakeServer({ info });
    server = fake.server;
    const client = connectNats(fake.url);
    await expect(client.request("s", {}, "[]", never)).rejects.toThrow(text);
    expect(fake.lines.some((l) => l.startsWith("CONNECT "))).toBe(false);
    client.close();
  });

  it("fails a publish when nothing listens, rather than queueing it", async () => {
    const fake = fakeServer();
    const { port } = fake.server;
    fake.server.stop(true);
    const client = connectNats(`nats://127.0.0.1:${port}`);
    await expect(client.request("s", {}, "[]", never)).rejects.toThrow(/ECONNREFUSED/);
    client.close();
  });

  it("rejects an in-flight publish on abort", async () => {
    const fake = fakeServer({ onPub: () => {} });
    server = fake.server;
    const client = connectNats(fake.url);
    const abort = new AbortController();
    const pending = client.request("s", {}, "[]", abort.signal);
    setTimeout(() => abort.abort(new Error("stopped")), 20);
    await expect(pending).rejects.toThrow("stopped");
    client.close();
  });
});
