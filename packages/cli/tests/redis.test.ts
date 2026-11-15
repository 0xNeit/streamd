import { afterEach, describe, expect, it } from "bun:test";
import { connectRedis } from "../src/redis.js";

// Enough RESP3 for Bun's client: HELLO answered with a map, every other command
// with the next reply in `replies`, and a count of connections accepted.
function fakeServer(replies: string[]) {
  const commands: string[] = [];
  let connections = 0;
  const server = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      open: () => void connections++,
      data(socket, data) {
        for (const command of data.toString().split(/\*\d+\r\n/).filter(Boolean)) {
          const name = command.split("\r\n")[1]!;
          commands.push(name);
          socket.write(name === "HELLO" ? "%1\r\n+server\r\n+redis\r\n" : replies.shift() ?? "-ERR no reply\r\n");
        }
      },
    },
  });
  return { server, commands, connections: () => connections };
}

let server: { stop(closeActiveConnections?: boolean): void } | undefined;
afterEach(() => server?.stop(true));

describe("connectRedis under Bun", () => {
  it("connects on the first command, and again after close", async () => {
    const fake = fakeServer(["$3\r\n1-0\r\n", "$3\r\n2-0\r\n"]);
    server = fake.server;
    const client = await connectRedis(`redis://127.0.0.1:${fake.server.port}`, 1000);
    expect(fake.connections()).toBe(0);
    expect(await client.send("XADD", ["s", "*", "body", "[]"])).toBe("1-0");
    client.close();
    expect(await client.send("XADD", ["s", "*", "body", "[]"])).toBe("2-0");
    expect(fake.connections()).toBe(2);
    expect(fake.commands).toEqual(["HELLO", "XADD", "HELLO", "XADD"]);
    client.close();
  });

  it("rejects with the server's error reply as the message", async () => {
    const fake = fakeServer(["-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"]);
    server = fake.server;
    const client = await connectRedis(`redis://127.0.0.1:${fake.server.port}`, 1000);
    await expect(client.send("XADD", ["s", "*", "body", "[]"])).rejects.toThrow(/^WRONGTYPE /);
    client.close();
  });

  it("fails a command when nothing listens, rather than queueing it", async () => {
    const fake = fakeServer([]);
    const { port } = fake.server;
    fake.server.stop(true);
    const client = await connectRedis(`redis://127.0.0.1:${port}`, 1000);
    await expect(client.send("XADD", ["s", "*", "body", "[]"])).rejects.toThrow();
    client.close();
  });
});
