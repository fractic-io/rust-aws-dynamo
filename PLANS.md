## Correctness

- Atomic writes (maybe init & close capturing, where all in-between actions are 'committed' in a single operation up to 100 items?).
- Consistent reads accross API network (lambda invocation) boundaries.

## Optimization

- `raw_batch_delete_partition`: Alternate page fetch & batch deletes, to avoid allocating entire Vec of IDs in memory, and skip ID deduplication.
