## Correctness

- Transaction support.
- Consistent reads.

## Optimization

- `raw_batch_delete_partition`: Alternate page fetch & batch deletes, to avoid allocating entire Vec of IDs in memory, and skip ID deduplication.
