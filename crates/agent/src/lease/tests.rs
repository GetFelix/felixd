use std::sync::Mutex;

use anyhow::bail;

use super::*;

const TTL: Duration = Duration::from_secs(9);

fn timing() -> LeaseTiming {
    LeaseTiming { ttl: TTL }
}

/// One lease key with a TTL, on tokio's clock so tests can move time, and its
/// epoch counter.
#[derive(Default)]
struct FakeStore {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    entry: Option<(Bytes, u64, Instant)>,
    epoch: Option<(Bytes, u64)>,
    next_epoch_version: u64,
    /// Lease puts that fail before doing anything.
    fail_puts: u32,
    /// Lease reads that fail.
    fail_reads: u32,
    /// Another claimer takes an epoch just before the next epoch write.
    epoch_race: bool,
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
        if s.fail_puts > 0 {
            s.fail_puts -= 1;
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
        let mut s = self.state.lock().unwrap();
        if s.down {
            bail!("unreachable");
        }
        if s.fail_reads > 0 {
            s.fail_reads -= 1;
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

    async fn get_epoch(&self, _key: &str) -> Result<Option<VersionedValue>> {
        let s = self.state.lock().unwrap();
        Ok(s.epoch.as_ref().map(|(value, version)| VersionedValue {
            value: value.clone(),
            version: *version,
        }))
    }

    async fn put_epoch_if(
        &self,
        _key: &str,
        value: Bytes,
        condition: CacheCondition,
    ) -> Result<CacheConditionResult> {
        let mut s = self.state.lock().unwrap();
        if std::mem::take(&mut s.epoch_race) {
            let v = s.next_epoch_version;
            s.next_epoch_version += 1;
            s.epoch = Some((Bytes::from_static(b"7"), v));
        }
        let current = s.epoch.as_ref().map(|e| e.1);
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
        let v = s.next_epoch_version;
        s.next_epoch_version += 1;
        s.epoch = Some((value, v));
        Ok(CacheConditionResult {
            applied: true,
            version: Some(v),
        })
    }
}

async fn take(store: &FakeStore, node: &str) -> Option<Held> {
    claim(store, node, "w", timing(), async { Ok(0) })
        .await
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn a_claim_takes_an_absent_lease_then_writes_its_epoch_into_it() {
    let store = FakeStore::default();
    let held = take(&store, "a").await.unwrap();
    assert_eq!(held.epoch, 1);
    assert_eq!(store.version(), Some(held.version));
    assert_eq!(
        store.state.lock().unwrap().conditions,
        [CacheCondition::Absent, CacheCondition::Version(0)]
    );
    let found = store.get_versioned("w").await.unwrap().unwrap();
    assert_eq!(Lease::decode(&found.value), Ok(Lease::held("a", 1)));
}

#[tokio::test(start_paused = true)]
async fn a_claim_takes_an_epoch_above_the_output_streams_even_with_a_new_counter() {
    // The leases and epochs caches were recreated, so their versions and the
    // counter start again, but the output stream still holds epoch 500.
    let store = FakeStore::default();
    let held = claim(&store, "a", "w", timing(), async { Ok(500) })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.epoch, 501);
    release(&store, &held).await;
    // The counter remembers it even if the output stream does not move.
    assert_eq!(take(&store, "b").await.unwrap().epoch, 502);
}

#[tokio::test(start_paused = true)]
async fn a_claimer_that_loses_the_epoch_race_gives_the_lease_back() {
    let store = FakeStore::default();
    // Someone took an epoch between this claimer's read and its write.
    store.set(|s| s.epoch_race = true);
    assert!(take(&store, "a").await.is_none());
    assert_eq!(store.version(), None, "the claim was not released");
}

#[tokio::test(start_paused = true)]
async fn a_held_lease_is_not_claimed_until_it_expires() {
    let store = FakeStore::default();
    let first = take(&store, "a").await.unwrap();
    assert!(take(&store, "b").await.is_none());
    tokio::time::advance(TTL).await;
    let second = take(&store, "b").await.unwrap();
    assert!(second.epoch > first.epoch);
}

#[tokio::test(start_paused = true)]
async fn the_holder_renews_every_third_of_the_ttl_at_the_version_it_wrote() {
    let store = FakeStore::default();
    let mut held = take(&store, "a").await.unwrap();
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
    // A claim and its epoch, then one renewal every 3 s for 27 s, each at the
    // last version.
    assert_eq!(conditions.len(), 2 + 9);
    let expected: Vec<_> = [CacheCondition::Absent, CacheCondition::Version(0)]
        .into_iter()
        .chain((0..9).map(|i| CacheCondition::Version(claimed + i)))
        .collect();
    assert_eq!(conditions, expected);
    assert_eq!(held.sent_at, start + TTL * 3);
    assert_eq!(held.epoch, 1);
}

#[tokio::test(start_paused = true)]
async fn a_refused_renewal_loses_the_lease_at_once() {
    let store = FakeStore::default();
    let mut held = take(&store, "a").await.unwrap();
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
    let mut held = take(&store, "a").await.unwrap();
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
    let mut held = take(&store, "a").await.unwrap();
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
    let mut held = take(&store, "a").await.unwrap();
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
async fn a_read_back_renewal_is_credited_from_the_earliest_attempt_it_may_be() {
    let store = FakeStore::default();
    let mut held = take(&store, "a").await.unwrap();
    let (tx, fence) = held.fence();
    let first_sent = Instant::now() + timing().renew_after();
    // The first renewal lands but its answer and the read-back are lost. The
    // retry fails before reaching the broker, and its read-back finds the
    // first one, whose TTL counts from when it was sent.
    store.set(|s| {
        s.lose_next_answer = true;
        s.fail_reads = 1;
    });
    let retry = async {
        tokio::time::sleep_until(first_sent + Duration::from_millis(1)).await;
        store.set(|s| s.fail_puts = 1);
    };
    let holding = async {
        tokio::time::timeout(
            timing().renew_after() * 2 - Duration::from_millis(1),
            hold(&store, &mut held, &tx),
        )
        .await
    };
    let (holding, ()) = tokio::join!(holding, retry);
    assert!(holding.is_err(), "the lease was lost: {holding:?}");
    assert_eq!(*fence.deadline.borrow(), first_sent + timing().stop_after());
    assert_eq!(held.sent_at, first_sent);
}

#[tokio::test(start_paused = true)]
async fn release_deletes_only_this_holders_lease() {
    let store = FakeStore::default();
    let held = take(&store, "a").await.unwrap();
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
