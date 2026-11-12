import { StreamdError, StreamdNetworkError, UNSIGNED_INTEGER } from "@streamd/core";
import { TRANSFER_DESCRIPTOR, assetKey, requireAsset } from "./transfers.js";
import type { BalanceReader, EventAdapter, ViewRequest } from "@streamd/core";

const APT = `0x${"a".padStart(64, "0")}`;
const APT_COIN = "0x1::aptos_coin::AptosCoin";
const APT_COIN_KEY = assetKey(APT_COIN);
const LEGACY_TOKEN = /^0x[0-9a-fA-F]+::.+::.+::\d+$/s;

/** The asset's map key: long-form addresses, with APT one entry however it was named. */
function canonical(asset: string): string {
  const key = assetKey(asset);
  return key === APT_COIN_KEY ? APT : key;
}

function balanceRequest(owner: string, asset: string): ViewRequest {
  if (asset === APT || asset.includes("::")) {
    return {
      function: "0x1::coin::balance",
      typeArguments: [asset === APT ? APT_COIN : asset],
      functionArguments: [owner],
    };
  }
  return {
    function: "0x1::primary_fungible_store::balance",
    typeArguments: ["0x1::fungible_asset::Metadata"],
    functionArguments: [owner, asset],
  };
}

/** Rejects with a non-network error, which stops a poller, when the view aborts, as for a token object. */
async function requireBalance(
  adapter: EventAdapter,
  owner: string,
  asset: string,
  version: bigint,
): Promise<bigint> {
  const request = balanceRequest(owner, asset);
  let result: unknown[];
  try {
    result = await adapter.view!(request, version, TRANSFER_DESCRIPTOR);
  } catch (err) {
    if (err instanceof StreamdError && !(err instanceof StreamdNetworkError)) {
      throw new StreamdError(`Not a fungible asset or coin type: ${asset}`, err);
    }
    throw err;
  }
  const amount = Array.isArray(result) ? result[0] : undefined;
  if (typeof amount !== "string" || !UNSIGNED_INTEGER.test(amount)) {
    throw new StreamdError(`${request.function} of ${owner} at version ${version} returned no amount`);
  }
  return BigInt(amount);
}

/**
 * Aptos balances by view, which keeps them exact: gas moves APT with no
 * withdraw, a coin can move as its paired fungible asset, and a scan of the
 * whole chain cannot keep up with mainnet.
 */
export const APTOS_BALANCES: BalanceReader = {
  check(adapter) {
    if (typeof adapter.view !== "function" || typeof adapter.getHeadVersion !== "function") {
      throw new StreamdError("balances needs an adapter that implements view and getHeadVersion");
    }
  },
  assets(named) {
    const names: readonly string[] = named === undefined ? [APT] : typeof named === "string" ? [named] : named;
    const assets = [...new Set(names.map((name) => canonical(requireAsset(name))))];
    const token = assets.find((asset) => LEGACY_TOKEN.test(asset));
    if (token !== undefined) throw new StreamdError(`A legacy token has no balance to follow: ${token}`);
    return assets;
  },
  canonical: (asset) => canonical(requireAsset(asset)),
  type: (asset) => balanceRequest("0x0", asset).function,
  read: requireBalance,
};
