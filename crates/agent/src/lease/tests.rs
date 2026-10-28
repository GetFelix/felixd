use std::sync::Mutex;

use anyhow::bail;

use super::*;

const TTL: Duration = Duration::from_secs(9);

fn timing() -> LeaseTiming {
    LeaseTiming { ttl: TTL }
}

/// One cache key with a TTL, on tokio's clock so tests can move time.
#[derive(Default)]
struct FakeStore {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    entry: Option<(Bytes, u64, Instant)>,
    /// Like Felix, the first write to a new cache is version 0.
    next_version: u64,
    /// Every call fails before doing anything.
    down: bool,
    /// The next put is applied but its answer is lost.
    lose_next_answer: bool,
    conditions: Vec<CacheCondition>,
}

impl State {
    fn live(&self) -> Option<&(Bytes, u64, Instant)> {
        self.entry
            .as_ref()
            .filter(|(_, _, at)| Instant::now() < *at)
    }
}

impl FakeStore {
    fn set(&self, f: impl FnOnce(&mut State)) {
        f(&mut self.state.lock().unwrap());
    }

    fn version(&self) -> Option<u64> {
        self.state.lock().unwrap().live().map(|e| e.1)
    }

    /// Another writer replaces the entry.
    fn overwrite(&self) {
        let mut s = self.state.lock().unwrap();
        let v = s.next_version;
        s.next_version += 1;
        s.entry = Some((Bytes::from_static(b"{}"), v, Instant::now() + TTL));
    }
}

impl LeaseStore for FakeStore {
    async fn put_if(
        &self,
        _key: &str,
        value: Bytes,
        ttl_ms: u64,
        condition: CacheCondition,
    ) -> Result<CacheConditionResult> {
        let mut s = self.state.lock().unwrap();
        assert!(s.conditions.len() < 1000, "the holder keeps writing");
        if s.down {
            bail!("unreachable");
        }
        s.conditions.push(condition);
        let current = s.live().map(|e| e.1);
        let ok = match condition {
            CacheCondition::Absent => current.is_none(),
            CacheCondition::Version(v) => current == Some(v),
        };
        if !ok {
            return Ok(CacheConditionResult {
                applied: false,
                version: current,
            });
        }
        let v = s.next_version;
        s.next_version += 1;
        s.entry = Some((value, v, Instant::now() + Duration::from_millis(ttl_ms)));
        if std::mem::take(&mut s.lose_next_answer) {
            bail!("answer lost");
        }
        Ok(CacheConditionResult {
            applied: true,
            version: Some(v),
        })
    }

    async fn get_versioned(&self, _key: &str) -> Result<Option<VersionedValue>> {
        let s = self.state.lock().unwrap();
        if s.down {
            bail!("unreachable");
        }
        Ok(s.live().map(|(value, version, _)| VersionedValue {
            value: value.clone(),
            version: *version,
        }))
    }

    async fn delete_if(&self, _key: &str, version: u64) -> Result<CacheConditionResult> {
        let mut s = self.state.lock().unwrap();
        let ok = s.live().map(|e| e.1) == Some(version);
        if ok {
            s.entry = None;
        }
        Ok(CacheConditionResult {
            applied: ok,
            version: None,
        })
    }
}

#[tokio::test(start_paused = true)]
async fn a_claim_takes_an_absent_lease_and_its_version_gives_the_epoch() {
    let store = FakeStore::default();
    let held = claim(&store, "a", "w", timing()).await.unwrap().unwrap();
    assert_eq!(held.epoch, held.version + 1);
    assert_eq!(store.version(), Some(held.version));
    assert_eq!(
        store.state.lock().unwrap().conditions,
        [CacheCondition::Absent]
    );
}

#[tokio::test(start_paused = true)]
async fn a_held_lease_is_not_claimed_until_it_expires() {
    let store = FakeStore::default();
    let first = claim(&store, "a", "w", timing()).await.unwrap().unwrap();
    assert!(claim(&store, "b", "w", timing()).await.unwrap().is_none());
    tokio::time::advance(TTL).await;
    let second = claim(&store, "b", "w", timing()).await.unwrap().unwrap();
    assert!(second.epoch > first.epoch);
}

#[tokio::test(start_paused = true)]
async fn the_holder_renews_every_third_of_the_ttl_at_the_version_it_wrote() {
    let store = FakeStore::default();
    let mut held = claim(&store, "a", "w", timing()).await.unwrap().unwrap();
    let claimed = held.version;
    let (tx, fence) = held.fence();
    let start = Instant::now();
    let holding = tokio::time::timeout(
        TTL * 3 + Duration::from_secs(1),
        hold(&store, &mut held, &tx),
    )
    .await;
    assert!(holding.is_err(), "the lease was lost: {holding:?}");
    assert!(fence.holds());
    let conditions = store.state.lock().unwrap().conditions.clone();
    // A claim, then one renewal every 3 s for 27 s, each at the last version.
    assert_eq!(conditions.len(), 1 + 9);
    let expected: Vec<_> = std::iter::once(CacheCondition::Absent)
        .chain((0..9).map(|i| CacheCondition::Version(claimed + i)))
        .collect();
    assert_eq!(conditions, expected);
    assert_eq!(held.sent_at, start + TTL * 3);
    assert_eq!(held.epoch, claimed + 1);
}

#[tokio::test(start_paused = true)]
async fn a_refused_renewal_loses_the_lease_at_once() {
    let store = FakeStore::default();
    let mut held = claim(&store, "a", "w", timing()).await.unwrap().unwrap();
    let (tx, _fence) = held.fence();
    store.overwrite();
    let start = Instant::now();
    let lost = tokio::time::timeout(TTL, hold(&store, &mut held, &tx)).await;
    assert_eq!(lost.expect("still holding"), Lost::Refused);
    assert_eq!(Instant::now() - start, timing().renew_after());
}

#[tokio::test(start_paused = true)]
async fn an_expired_lease_is_not_renewed() {
    let store = FakeStore::default();
    let mut held = claim(&store, "a", "w", timing()).await.unwrap().unwrap();
    let (tx, _fence) = held.fence();
    // The holder was frozen past the TTL, so the broker expired the entry
    // before the renewal that was due at 3 s.
    tokio::time::advance(TTL).await;
    let lost = tokio::time::timeout(TTL, hold(&store, &mut held, &tx)).await;
    assert_eq!(lost.expect("still holding"), Lost::Refused);
}

#[tokio::test(start_paused = true)]
async fn without_felix_the_holder_stops_by_its_own_clock_at_two_thirds() {
    let store = FakeStore::default();
    let mut held = claim(&store, "a", "w", timing()).await.unwrap().unwrap();
    let (tx, fence) = held.fence();
    let start = Instant::now();
    store.set(|s| s.down = true);
    let holder = async {
        let lost = tokio::time::timeout(TTL * 2, hold(&store, &mut held, &tx)).await;
        (lost.expect("still holding"), Instant::now() - start)
    };
    let watcher = async {
        tokio::time::sleep(timing().stop_after() - Duration::from_millis(1)).await;
        assert!(fence.holds(), "stopped early");
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(!fence.holds(), "still holds at 2/3 of the TTL");
    };
    let ((lost, after), ()) = tokio::join!(holder, watcher);
    assert_eq!(lost, Lost::Unreachable);
    assert_eq!(after, timing().stop_after());
    // Nobody else could have claimed it yet.
    assert!(after < TTL);
}

#[tokio::test(start_paused = true)]
async fn a_renewal_whose_answer_was_lost_is_found_by_reading_it_back() {
    let store = FakeStore::default();
    let mut held = claim(&store, "a", "w", timing()).await.unwrap().unwrap();
    let claimed = held.version;
    let (tx, fence) = held.fence();
    store.set(|s| s.lose_next_answer = true);
    let holding = tokio::time::timeout(TTL * 2, hold(&store, &mut held, &tx)).await;
    assert!(holding.is_err(), "the lease was lost: {holding:?}");
    assert!(fence.holds());
    assert!(held.version > claimed + 1);
    assert_eq!(store.version(), Some(held.version));
}

#[tokio::test(start_paused = true)]
async fn release_deletes_only_this_holders_lease() {
    let store = FakeStore::default();
    let held = claim(&store, "a", "w", timing()).await.unwrap().unwrap();
    store.overwrite();
    release(&store, &held).await;
    assert!(store.version().is_some());
    let held = Held {
        version: store.version().unwrap(),
        ..held
    };
    release(&store, &held).await;
    assert_eq!(store.version(), None);
}
