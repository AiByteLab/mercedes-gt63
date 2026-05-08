Here’s a solid **L6 data engineering** code challenge that directly tests **windowed joins, real-time data, offset management, and concurrency**—with room to see system design instincts without turning it into a pure architecture interview.

## Challenge: Real-time Session Attribution with Windowed Stream Joins

### What they build

Implement a small **stream processor** that consumes two event streams and produces an output stream:

* **Stream A: `page_view` events**

  * `user_id`, `event_time` (event-time), `url`, `event_id`
* **Stream B: `ad_click` events**

  * `user_id`, `event_time`, `campaign_id`, `click_id`

**Goal:** For each `page_view`, attach the **most recent** `ad_click` for the same `user_id` that happened within the last **30 minutes** in **event time**, and emit:

* `page_view_id`, `user_id`, `event_time`, `url`, `attributed_campaign_id` (nullable), `attributed_click_id` (nullable)

This is a **windowed join** with “latest-match” semantics.

---

## Requirements (these drive the evaluation)

### 1) Windowed join + out-of-order handling

* Events arrive **out of order** (by event_time).
* You must support **watermarks**:

  * configurable allowed lateness (e.g., 2 minutes).
* A `page_view` can be emitted when you are confident no earlier `ad_click` will arrive that should supersede attribution (based on watermark), **or** you can emit updates (but then specify your update strategy clearly).

**Edge cases to include:**

* click arrives after its page_view (but still within lateness)
* multiple clicks in the 30-min window → pick the latest click
* late events beyond allowed lateness → drop or dead-letter (must be explicit)

### 2) Offset management & delivery guarantees

* Input streams provide offsets (like Kafka):

  * `partition`, `offset`, `payload`
* Candidate must implement a **consumer loop** that:

  * tracks offsets **per partition**
  * **commits offsets** only after output is durably written (or staged)
* Must be safe under crash/restart:

  * no lost outputs
  * minimize duplicates (at-least-once is acceptable if dedup is implemented)

**Ask for:**

* at-least-once + idempotent output OR exactly-once style via transactional write simulation
* output sink can be a local append-only log file or sqlite table with unique constraints

### 3) Stateful processing (join state + eviction)

They need to manage state:

* store recent `ad_click`s keyed by `user_id`
* optionally buffer `page_view`s waiting for watermark
* implement **state eviction**:

  * evict click history older than 30 minutes + lateness
  * bounded memory

### 4) Concurrency

The processor should:

* run with **N worker threads/tasks**
* preserve **per-partition ordering** while still parallelizing
* avoid race conditions on shared state
* demonstrate correct locking/partitioned state strategy

A good approach: “one worker per partition” OR “partitioned state shards” with consistent hashing.

### 5) Determinism + testability

Provide a deterministic test harness:

* feed a fixed set of events with known ordering and offsets
* assert output records exactly (including attribution correctness)
* include restart test: process half, crash, restart, ensure correctness

---

## Input/Output Contract (simple, interview-friendly)

### Input

They get two iterators (or generators) that yield records:

```text
(topic, partition, offset, payload_json)
```

### Output

A sink interface:

```text
write(record)  # must be durable for commit
```

---

## Deliverables you ask them for

1. Working processor implementation (language of choice)
2. Explanation of:

   * watermark logic
   * join semantics (emit-once vs update)
   * offset commit strategy
   * concurrency model
   * state sizing & eviction
3. Tests for:

   * out-of-order
   * late data
   * restart with committed offsets
   * concurrent partitions

---

## Scoring rubric (what “L6” looks like)

### Strong L6 signals

* Clean separation of concerns: ingestion, state, watermark, output, offset mgmt
* Clear correctness argument (when/why they emit)
* Thoughtful state eviction & memory bounds
* Uses idempotency keys / dedup strategy (event_id/click_id)
* Concurrency without global locks (partition/shard approach)
* Restart safety demonstrated in tests

### Red flags

* commits offsets before output durability
* no eviction (unbounded memory)
* incorrect attribution when events arrive out of order
* concurrency that breaks partition ordering
* “watermark” hand-waved (no concrete rule)

---

## Add-on extension (if they finish early)

Pick one:

* **Re-attribution updates:** emit corrections when late clicks arrive (produce “upsert” output)
* **Backpressure:** simulate slow sink and ensure consumers don’t OOM
* **Skew handling:** hot users / hot partitions—how they mitigate contention

---

## Quick “interview packet” you can hand them

* Problem statement (above)
* Sample event schema + a small dataset
* Provided stub interfaces for `Source`, `Sink`, `OffsetStore`
* 90–120 minutes coding + 15 minutes discussion

---

If you tell me **preferred language** (Java/Scala/Python/Go) and whether you want **single-machine (threads)** vs **async IO**, I can also provide:

* a ready-to-use starter skeleton,
* a curated dataset that hits all the edge cases,
* and an answer key for expected outputs.

