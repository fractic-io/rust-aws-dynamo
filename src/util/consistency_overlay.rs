use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use fractic_context::register_ctx_singleton;

use crate::{schema::PkSk, DynamoCtxView};

use super::DynamoMap;

// Definitions.
// ----------------------------------------------------------------------------

/// How long successful local writes participate in reconciled GSI queries.
pub const DEFAULT_CONSISTENCY_OVERLAY_RETENTION: Duration = Duration::from_secs(5);

/// A recent successful mutation used to reconcile eventually consistent GSI
/// results.
#[derive(Clone, Debug)]
pub enum OverlayMutation {
    Put { id: PkSk, item: Arc<DynamoMap> },
    Delete(PkSk),
}

// Public interface.
// ----------------------------------------------------------------------------

/// Shared recent-write state used to reconcile GSI queries within one context.
pub trait DynamoConsistencyOverlay: Send + Sync {
    fn record_put(&self, table: &str, item: DynamoMap);
    fn record_delete(&self, table: &str, id: PkSk);
    fn snapshot(&self, table: &str) -> Vec<OverlayMutation>;
}

impl OverlayMutation {
    pub(crate) fn id(&self) -> PkSk {
        match self {
            Self::Put { id, .. } => id.clone(),
            Self::Delete(id) => id.clone(),
        }
    }
}

// Internal: Overlay state.
// ----------------------------------------------------------------------------

#[derive(Default)]
struct OverlayState {
    next_generation: u64,
    tables: HashMap<String, TableOverlay>,
}

#[derive(Default)]
struct TableOverlay {
    entries: HashMap<PkSk, Entry>,
    expirations: BinaryHeap<Reverse<Expiration>>,
}

#[derive(Debug)]
struct Entry {
    generation: u64,
    mutation: OverlayMutation,
}

#[derive(Debug)]
struct Expiration {
    at: Instant,
    generation: u64,
    id: PkSk,
}

impl PartialEq for Expiration {
    fn eq(&self, other: &Self) -> bool {
        self.generation == other.generation
    }
}

impl Eq for Expiration {}

impl PartialOrd for Expiration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Expiration {
    fn cmp(&self, other: &Self) -> Ordering {
        self.at
            .cmp(&other.at)
            .then_with(|| self.generation.cmp(&other.generation))
    }
}

impl TableOverlay {
    fn remove_expired(&mut self, now: Instant) {
        while self
            .expirations
            .peek()
            .is_some_and(|expiration| expiration.0.at <= now)
        {
            let Reverse(expiration) = self.expirations.pop().unwrap();
            if self
                .entries
                .get(&expiration.id)
                .is_some_and(|entry| entry.generation == expiration.generation)
            {
                self.entries.remove(&expiration.id);
            }
        }
    }
}

// Internal: In-memory implementation.
// ----------------------------------------------------------------------------

/// Default in-memory consistency overlay.
pub struct InMemoryDynamoConsistencyOverlay {
    retention: Duration,
    state: Mutex<OverlayState>,
}

impl InMemoryDynamoConsistencyOverlay {
    pub fn new(retention: Duration) -> Self {
        Self {
            retention,
            state: Mutex::new(OverlayState::default()),
        }
    }

    fn record(&self, table: &str, id: PkSk, mutation: OverlayMutation) {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        state.next_generation = state.next_generation.wrapping_add(1);
        let generation = state.next_generation;
        let table = state.tables.entry(table.to_owned()).or_default();
        table.remove_expired(now);
        table.entries.insert(
            id.clone(),
            Entry {
                generation,
                mutation,
            },
        );
        table.expirations.push(Reverse(Expiration {
            at: now + self.retention,
            generation,
            id,
        }));
    }
}

impl Default for InMemoryDynamoConsistencyOverlay {
    fn default() -> Self {
        Self::new(DEFAULT_CONSISTENCY_OVERLAY_RETENTION)
    }
}

impl DynamoConsistencyOverlay for InMemoryDynamoConsistencyOverlay {
    fn record_put(&self, table: &str, item: DynamoMap) {
        let id = PkSk::from_map(&item)
            .expect("consistency overlay put did not contain valid pk/sk fields");
        self.record(
            table,
            id.clone(),
            OverlayMutation::Put {
                id,
                item: Arc::new(item),
            },
        );
    }

    fn record_delete(&self, table: &str, id: PkSk) {
        self.record(table, id.clone(), OverlayMutation::Delete(id));
    }

    fn snapshot(&self, table: &str) -> Vec<OverlayMutation> {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        let Some(table) = state.tables.get_mut(table) else {
            return Vec::new();
        };
        table.remove_expired(now);
        table
            .entries
            .values()
            .map(|entry| entry.mutation.clone())
            .collect()
    }
}

// Dependency registration.
// ----------------------------------------------------------------------------

register_ctx_singleton!(
    dyn DynamoCtxView,
    dyn DynamoConsistencyOverlay,
    |_ctx: Arc<dyn DynamoCtxView>| async move { Ok(InMemoryDynamoConsistencyOverlay::default()) }
);

// Tests.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::AttributeValue;
    use fractic_core::collection;

    use super::*;

    fn item(pk: &str, sk: &str, value: &str) -> DynamoMap {
        collection! {
            "pk".to_string() => AttributeValue::S(pk.to_string()),
            "sk".to_string() => AttributeValue::S(sk.to_string()),
            "value".to_string() => AttributeValue::S(value.to_string()),
        }
    }

    #[test]
    fn retains_only_the_latest_mutation_for_each_item() {
        let overlay = InMemoryDynamoConsistencyOverlay::new(Duration::from_secs(60));
        overlay.record_put("table", item("P", "A", "old"));
        overlay.record_put("table", item("P", "A", "new"));
        overlay.record_delete(
            "table",
            PkSk {
                pk: "P".into(),
                sk: "B".into(),
            },
        );

        let snapshot = overlay.snapshot("table");
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.iter().any(|mutation| matches!(
            mutation,
            OverlayMutation::Put { item, .. }
                if item.get("value") == Some(&AttributeValue::S("new".into()))
        )));
    }

    #[test]
    fn expires_mutations() {
        let overlay = InMemoryDynamoConsistencyOverlay::new(Duration::ZERO);
        overlay.record_put("table", item("P", "A", "value"));
        assert!(overlay.snapshot("table").is_empty());
    }
}
