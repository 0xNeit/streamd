# @streamd/cli

The `streamd` command-line executable for tailing, querying, forwarding, storing, inspecting, and generating types from on-chain events.

## Commands

- `tail`, `query`, `forward`, `alert`, `store` (events, or with `--records transfers|swaps|sales` the chain client's records; `store stats`, `store prune` and `store cursors` maintain what it wrote), `watch`, `tx`, `explain`, `inspect`, `transfers`, `swaps`, `sales`, `activity`, `upgrades` (exiting 1 on a removed or changed signature), `trace`, `holders`, `positions`, `balances`, `handles`, `name`, `check`, `compare`, `monitor`, `record`, `coverage`, and `codegen` (with `--watch` to rewrite its `--out` file as packages are published).
- `--chain sui` loads `@streamd/sui` and `@mysten/sui` only when invoked (both are optional peers).

For full usage, flags, and configuration details, see the [streamd documentation](https://github.com/0xNeit/streamd#readme).
