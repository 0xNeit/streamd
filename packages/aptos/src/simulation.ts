import { StreamdError, isObject } from "@streamd/core";
import type { AptosTransactionSimulation } from "./types.js";

/** A simulation request whose keys fit its transaction, refused before anything is sent. */
export function requireSimulation(value: unknown): AptosTransactionSimulation {
  const simulation = value as Partial<Record<keyof AptosTransactionSimulation, unknown>> | null;
  const transaction = simulation?.transaction;
  if (!isObject(transaction) || !isObject(transaction.rawTransaction)) {
    throw new StreamdError("Not a built transaction: pass the result of aptos.transaction.build.simple");
  }
  const { signerPublicKey, secondarySignersPublicKeys: secondaryKeys, feePayerPublicKey } = simulation!;
  for (const [name, key] of [["signerPublicKey", signerPublicKey], ["feePayerPublicKey", feePayerPublicKey]] as const) {
    if (key !== undefined && !isObject(key)) throw new StreamdError(`${name} must be a PublicKey`);
  }
  const secondary = transaction.secondarySignerAddresses;
  if (secondary !== undefined && !Array.isArray(secondary)) {
    throw new StreamdError("Not a built transaction: secondarySignerAddresses must be a list");
  }
  if (secondaryKeys !== undefined) {
    if (secondary === undefined) {
      throw new StreamdError("secondarySignersPublicKeys needs a multi-agent transaction (aptos.transaction.build.multiAgent)");
    }
    if (!Array.isArray(secondaryKeys) || secondaryKeys.some((k) => k !== undefined && !isObject(k))) {
      throw new StreamdError("secondarySignersPublicKeys must be a list of PublicKey or undefined");
    }
    if (secondaryKeys.length !== secondary.length) {
      throw new StreamdError(
        `secondarySignersPublicKeys has ${secondaryKeys.length} key(s) for ${secondary.length} secondary signer(s)`,
      );
    }
  }
  if (feePayerPublicKey !== undefined && transaction.feePayerAddress === undefined) {
    throw new StreamdError("feePayerPublicKey needs a sponsored transaction (built withFeePayer)");
  }
  return value as AptosTransactionSimulation;
}
