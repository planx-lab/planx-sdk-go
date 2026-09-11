# Planx Go SDK

## Collaboration and references

Use [workspace guidance](../AGENTS.md) for task completion, authorization and
proportionate checks. Read relevant clauses of the [canonical contract](../planx-spec/AI_CONTRACT.md),
[architecture](../planx-spec/planx-architecture.md) and accepted
[ADR-017](../planx-spec/adr/017-builtin-typed-data-integration.md) when their
subject changes. Use [repo.lock](repo.lock) for source ownership. Do not reread
the entire specification for an unrelated edit.

## Ownership

- Own the external plugin runtime: gRPC servers, sessions, flow control,
  batching, panic recovery and observability hooks. These internals stay in
  `internal/`; plugins do not manage sessions, concurrency or backpressure.
- `sdk/` and `record/` are public contracts under ADR-017. The public typed-data
  layer owns schema/value validation, cloning and boundary encoding/decoding.
  The old opaque-only restriction does not prohibit this approved layer.
- Engine may consume the public typed-data/SPI contract, never SDK internals.
  SDK does not import Engine or contain its database drivers, CEL or Lookup logic.
- Keep SPI minimal, batch-oriented and synchronous. Do not expose gRPC/session/
  window types or add a single-record execution API. Plugins remain user code.
- Verify public changes in affected SDK, Engine and external plugin consumers;
  preserve external-process, precision and transport regression tests.
