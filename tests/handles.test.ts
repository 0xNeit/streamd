import { describe, it, expect } from "bun:test";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { listEventHandles } from "../src/handles.js";
import { FullnodeScanAdapter } from "../src/adapters/fullnode.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";

const ADDR = "0x" + "a".repeat(64);
const FORUM = "0x1::voting::VotingForum<0x1::governance_proposal::GovernanceProposal>";

interface Reply {
  status?: number;
  data: unknown;
}

function fakeNode(route: (path: string) => Reply) {
  const paths: string[] = [];
  const client = {
    async provider(req: { url: string }) {
      const path = req.url.replace(/^http:\/\/fake\/v1\/?/, "");
      paths.push(path);
      const reply = route(path);
      const status = reply.status ?? 200;
      return { status, statusText: status === 200 ? "OK" : "Error", data: reply.data, headers: {} };
    },
  };
  const aptos = new Aptos(
    new AptosConfig({
      network: Network.LOCAL,
      fullnode: "http://fake/v1",
      client: client as never,
      clientConfig: { http2: false },
    }),
  );
  return { aptos, paths };
}

function handle(counter: string, creationNum: string) {
  return { counter, guid: { id: { addr: ADDR, creation_num: creationNum } } };
}

const resources = [
  {
    type: "0x1::account::Account",
    data: {
      authentication_key: ADDR,
      coin_register_events: handle("15", "0"),
      key_rotation_events: handle("0", "1"),
      sequence_number: "3",
    },
  },
  {
    type: FORUM,
    data: {
      events: { vote_events: handle("9", "4") },
      next_proposal_id: "2",
    },
  },
  {
    type: "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>",
    data: {
      coin: { value: "100" },
      deposit_events: handle("7", "2"),
      frozen: false,
      withdraw_events: { counter: "x", guid: { id: { addr: ADDR, creation_num: "3" } } },
    },
  },
];

function serve(data: unknown) {
  return fakeNode((path) => {
    if (path === `accounts/${ADDR}/resources`) return { data };
    throw new Error(`unexpected path ${path}`);
  });
}

describe("listEventHandles", () => {
  it("returns a handle descriptor, count and guid for each top-level EventHandle field", async () => {
    const { aptos } = serve(resources);
    expect(await listEventHandles(aptos, ADDR)).toEqual([
      {
        descriptor: {
          kind: "handle",
          accountAddress: ADDR,
          eventHandleStruct: "0x1::account::Account",
          fieldName: "coin_register_events",
        },
        count: 15n,
        guid: { creationNumber: 0n, accountAddress: ADDR },
      },
      {
        descriptor: {
          kind: "handle",
          accountAddress: ADDR,
          eventHandleStruct: "0x1::account::Account",
          fieldName: "key_rotation_events",
        },
        count: 0n,
        guid: { creationNumber: 1n, accountAddress: ADDR },
      },
      {
        descriptor: {
          kind: "handle",
          accountAddress: ADDR,
          eventHandleStruct: "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>",
          fieldName: "deposit_events",
        },
        count: 7n,
        guid: { creationNumber: 2n, accountAddress: ADDR },
      },
    ]);
  });

  it("skips handles nested inside a struct field and malformed counters", async () => {
    const { aptos } = serve(resources);
    const fields = (await listEventHandles(aptos, ADDR)).map((h) => h.descriptor.fieldName);
    expect(fields).not.toContain("events");
    expect(fields).not.toContain("vote_events");
    expect(fields).not.toContain("withdraw_events");
  });

  it("returns an empty list for an account with no resources", async () => {
    const { aptos } = serve([]);
    expect(await listEventHandles(aptos, ADDR)).toEqual([]);
  });

  it("normalizes a short address into the descriptor", async () => {
    const { aptos, paths } = fakeNode(() => ({
      data: [{ type: "0x1::block::BlockResource", data: { new_block_events: handle("5", "3") } }],
    }));
    const [found] = await listEventHandles(aptos, "0x1");
    expect(found!.descriptor.accountAddress).toBe("0x" + "0".repeat(63) + "1");
    expect(paths).toEqual(["accounts/0x1/resources"]);
  });

  it("rejects an invalid address before any request", async () => {
    const { aptos, paths } = serve(resources);
    const err = await listEventHandles(aptos, "not-an-address").catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect(paths).toEqual([]);
  });

  it("rejects a failed request as StreamdNetworkError with its status", async () => {
    const { aptos } = fakeNode(() => ({ status: 500, data: { message: "boom" } }));
    const err = await listEventHandles(aptos, ADDR).catch((e) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(500);
    expect((err as StreamdNetworkError).cause).toBeDefined();
  });

  it("yields descriptors FullnodeScanAdapter reads from the handle endpoint", async () => {
    const { aptos, paths } = fakeNode((path) => {
      if (path.endsWith("/resources")) return { data: resources };
      return { data: [] };
    });
    const [first] = await listEventHandles(aptos, ADDR);
    await new FullnodeScanAdapter(aptos).fetchEvents(first!.descriptor, {});
    expect(paths[1]).toBe(`accounts/${ADDR}/events/0x1%3A%3Aaccount%3A%3AAccount/coin_register_events`);
  });
});
