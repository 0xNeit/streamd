/** A `google.protobuf.Value` as the SDK's gRPC client decodes it. */
export interface ProtoValue {
  kind:
    | { oneofKind: "nullValue"; nullValue: unknown }
    | { oneofKind: "numberValue"; numberValue: number }
    | { oneofKind: "stringValue"; stringValue: string }
    | { oneofKind: "boolValue"; boolValue: boolean }
    | { oneofKind: "structValue"; structValue: { fields: Record<string, ProtoValue> } }
    | { oneofKind: "listValue"; listValue: { values: ProtoValue[] } }
    | { oneofKind: undefined };
}

/** Plain JSON from a protobuf `Value`; Sui writes u64 and wider as strings, as Aptos does. */
export function toJson(value: ProtoValue | undefined): unknown {
  const kind = value?.kind;
  switch (kind?.oneofKind) {
    case "numberValue":
      return kind.numberValue;
    case "stringValue":
      return kind.stringValue;
    case "boolValue":
      return kind.boolValue;
    case "structValue":
      return Object.fromEntries(Object.entries(kind.structValue.fields).map(([k, v]) => [k, toJson(v)]));
    case "listValue":
      return kind.listValue.values.map(toJson);
    default:
      return null;
  }
}
