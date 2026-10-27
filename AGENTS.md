# fractic-aws-dynamo

Typed DynamoDB access: the `dynamo_object!` schema macro (`PkSk` ids,
nesting and id logic, materialized attributes, `MaybeCommitted*`
scaffolding), `DynamoUtil` (get / query / batch / update plans / consistent
reads / sort maintenance), `DynamoCtxView`, and the `ext` layer (`crud`
parent / child graph operations, `ingress` ordered / unordered creates,
`bundling` portable export / import, `query_generic!`). Consumer side:
`/efs/fractic/_/rust.md` §4.

`src/lib.rs` → `context.rs`, `errors.rs`, `schema.rs` + `schema/`
(`identifiers/` generation / placement / relations, `types/` `PkSk`,
`Coordinate`, `ForeignRef`, `Timestamp`, item (de)serialization,
materialization, `add_ons.rs`), `util.rs` + `util/` (one file per concern:
`query`, `query_execution`, `update_plan`, `calculate_sort`,
`raw_batch_helpers`, `consistency_overlay`, `backend`, `test`), `ext/`
(`crud.rs`, `ingress.rs`, `macros.rs`, `bundling/` with its own `tests.rs`).

- Every stored object in storyengine and app-backend goes through this
  crate: a change to id generation, serialization or query semantics can
  corrupt data. Title it `Breaking: …`, document the migration, verify
  against sandbox data before merging.
- New generic operations are methods on `DynamoUtil` in `util/<concern>.rs`;
  object-graph behaviour goes to `ext/`, schema-level features to
  `schema/`. `backend.rs` is the only place that calls the AWS SDK.
- Unit tests live in `schema/identifiers/tests.rs` and
  `ext/bundling/tests.rs`; add to them. `PLANS.md` lists known correctness
  and optimisation work.
- Commit / PR titles: `[util] …`, `[ext] …`, `[schema] …`.
