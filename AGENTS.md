# fractic-aws-dynamo

Typed DynamoDB access for the Fractic backends and CLIs: the `dynamo_object!`
schema macro (`PkSk` ids, nesting and id logic, materialized attributes,
`MaybeCommitted*` scaffolding), `DynamoUtil` (get / query / batch / update
plans / consistent reads / sort maintenance), `DynamoCtxView`, and the `ext`
layer (`crud` parent/child graph operations, `ingress` ordered / unordered
creates, `bundling` portable export / import, `query_generic!`). Public,
provided as-is (`fractic-io/rust-aws-dynamo`, identity `fractic`).

## Read first

- `~/.agents/AGENTS.md` (workspace orientation; loaded automatically by Claude
  Code, read it if you are Codex or Grok) and `~/.agents/guides/rust.md`
  (§4 shows how consumers define entities and Dynamo operations with this
  crate).
- `PLANS.md`: known correctness and optimisation work still to do.

## Layout

`src/lib.rs` → `context.rs`, `errors.rs`, `schema.rs` + `schema/`
(`identifiers/` generation / placement / relations, `types/` `PkSk`,
`Coordinate`, `ForeignRef`, `Timestamp`, item (de)serialization,
materialization, `add_ons.rs`), `util.rs` + `util/` (one file per concern:
`query`, `query_execution`, `update_plan`, `calculate_sort`,
`raw_batch_helpers`, `consistency_overlay`, `backend`, `test`), `ext/`
(`crud.rs`, `ingress.rs`, `macros.rs`, `bundling/` with its own `tests.rs`).

## Working here

- No flake here: `nix develop /efs/fractic/storyengine/util --command cargo
  test` (the crate has unit tests, including `schema/identifiers/tests.rs`
  and `ext/bundling/tests.rs`; add to them).
- Every stored object in storyengine and app-backend goes through this crate:
  changes to id generation, serialization or query semantics are breaking and
  can corrupt data; title them `Breaking: …`, document the migration, and
  verify against sandbox data before merging.
- New generic operations go to `util/<concern>.rs` as methods on
  `DynamoUtil`; object-graph behaviour goes to `ext/`; schema-level features
  to `schema/`. Keep `backend.rs` the only place that calls the AWS SDK.
- Commit / PR titles: `[util] …`, `[ext] …`, `[schema] …`, imperative,
  trailing period.
