## Design

### Watermark logic
Watermarks are used to handle out-of-order events per partition.

- Definition: watermark = `max(observed event_time) − allowedLateness` per partition.
- An event is late if its event_time < watermark and gets dropped.
- The watermark advances monotonically on every event admitted.
- Lateness subtraction is folded into the stored watermark itself to simplify the code and eliminate the need to subtract it every time when checking isTooLate.

### Write semantics: emit-and-update

I have chosen emit-and-update rather than emit-once as the write semantics. The README permits both, so either solution is acceptable for this assignment.

Trade-off:
- emit-and-update emits the attributed-page-view as soon as the page-view arrives, regardless of its attribution state. Consumers that can act on this data benefit from it. Consumers that need finalized attribution can see unfinalized data until either the relevant click arrives or the waiting time window is over. For such consumers, we can implement another field called `finalized` in `AttributedPageView` and set it to true once either of the above-mentioned conditions is met. This is an improvement I consider out of scope for this assignment.
- The advantage is code simplicity — emit-once would otherwise need to handle watermark-gated buffered drains plus idle-partition handling.

### Delivery guarantees

**Guarantee:** at-least-once.
**Why it holds:** Records are persisted in OutputSink before acknowledging. Sink writes are idempotent: replaying the same record produces the same row.

**Failure modes:**
- Crash between `write` and `flush`: row is not persisted, offset not committed, replay re-writes the same row, no data loss.
- Crash between `flush` and acknowledgment: row is persisted, offset not committed, replay re-writes the same row, no data loss.
- Crash during `flush`: transaction is atomic, i.e., all or nothing, no data loss.
- **Known crash-recovery gap**
  - Crash after a clean PV (no attributable click) emit+ack where the PV is in the sink. The in-memory PV buffer is lost on crash. A late click arriving post-crash cannot backpatch the pre-crash PV.
  - Possible fix: more on it in the "Possible Improvements" section below.

### Concurrency model

- This implementation is thread-safe and works under concurrent load. Tests are implemented in `ConcurrencyTest` to ensure this requirement.
- Spring Kafka is used with 3 consumer threads (`ConcurrentKafkaListenerContainerFactory`) per topic.
- Shared state: `ClickStateStore`, `PageViewBufferStore`, `WatermarkTracker`, and `OutputSink` are all independently thread-safe.
- Backpatches (i.e., attributing a click to a buffered PV) converge under concurrency. Every recomputation re-reads `findAttributableClick`, which always returns the latest click. Out-of-order writes to the sink converge to the correct final attribution.

### Capacity planning and scaling


**State size:**
- ClickStateStore: Order of (active_users × avg_clicks_per_user_per_window). With 30-min window and event_time-based bounds.
- PageViewBufferStore: Order of (active_users × avg_pvs_per_user_per_lateness_window). With 4-min default lateness, much smaller than click store.
- WatermarkTracker: O(num_partitions). Negligible.

**Per-instance throughput limits:**
- Bounded by `OutputSink` synchronous flush rate (single SQLite connection, `synchronized` write/flush).
- For higher throughput, replace SQLite with a horizontally-scalable sink (Kafka topic, Cassandra, S3 batched writes).

**Scaling out:**
We can scale out our solution by
- adding Kafka partitions
- adding more processor instances

State is distributed cleanly:
- Watermarks are already per partition.
- ClickStore and PageViewBuffer are per user, and since user_id is used as the Kafka key, all events for the same user land on the same partition (and therefore the same instance).

SQLite as sink with ~200 records per second is the current bottleneck, not the engine. To improve, we should first replace it with something more scalable, e.g., a batched async writer or a Kafka producer.

---

## Possible Improvements

These are out of scope for this submission but documented as next steps:

1. **Reload PV Buffer state on crash-recovery:** on startup, re-read recent rows (`event_time >= now - allowedLateness`) into the PV buffer so post-crash late clicks can still backpatch.

2. **Add a `finalized` boolean flag for stable downstream reads:** intended for consumers who want to see attributed-page-views only once their values are finalized. The flag would initially be false when the attributed click is null and the page view is committed. Out of scope for now, since the output schema in the given README doesn't include it, and we want to keep the contract the same.

3. **Track the `attributedClickTime` per buffered PV:** skip the re-emit if the new click's `event_time` is older or equal. Cuts redundant DB writes for hot users.

4. **Same-instant collision in state stores:** both `ClickStateStore` and `PageViewBufferStore` are keyed by `Instant` per user. Two events at the same millisecond for the same user can collide. Probability is low at millisecond precision. Potential fix: use a `List<Event>` as the inner value type instead of a single event.

5. **Idle-partition handling:** when a partition's stream is silent, its watermark stops advancing and state can't be evicted. Potential fix is to add a heartbeat at the source side, or to detect wall-clock idleness. Not relevant in production where partitions stay active.

6. **Hot-user state amplification:** a user generating thousands of events keeps a large in-memory buffer. Potential solution is to cap per-user state with an LRU.

7. **Higher-throughput sink:** replace SQLite with a connection pool, batched async writer, or external sink (Kafka topic of attributed_page_views, Cassandra, Iceberg).

8. **`userLocks` map grows unbounded:** `JoinEngine` keeps a per-user object in a `ConcurrentHashMap` so that same-user click + PV processing serializes (preventing a backpatch race where the click thread's `findAttributablePageViews` could otherwise observe an empty buffer between the PV thread's `flush` and `addPageView`). Each entry is ~64 bytes, so 1M users ≈ 64 MB. Potential fix is to delete entries inside `evictOldState` once a user's `ClickStateStore` and `PageViewBufferStore` are both empty. Safe pattern: only delete while holding the lock yourself, otherwise another thread can race against you and end up holding a different lock object for the same user.

9. **Backpressure:** the processing chain is fully synchronous — the consumer thread blocks inside `processPageView` until `outputSink.write` + `outputSink.flush` + `ack` complete, and only then polls Kafka for the next record. As a result, the consumer cannot OOM under slow-sink conditions with the current implementation.

10. **Skew handling:**
    - Cross-user parallelism is in place: the per-user lock doesn't block across users.
    - Per-partition watermarks aggregate across all users on that partition. Chatty users advancing the watermark effectively shrink the lateness budget for any quieter user on the same partition. A delayed event from a quieter user can be dropped as too-late because the chatty user's recent events pushed the watermark past it.
    - Hot-partition skew is mitigated by adding Kafka partitions; no further fix beyond that.
