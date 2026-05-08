# Real-time Session Attribution with Windowed Stream Joins

## Welcome!

You have received your first challenge to become a part of an awesome team!

**A note about AI tools:** Using AI is not forbidden, but keep in mind this challenge is designed to understand your capabilities. The more you rely on AI, the less we'll understand your real code skills, problem-solving approach, and architectural thinking. We encourage you to use AI as a productivity tool, but make sure the core design decisions and implementation logic reflect your own understanding.

---

## Challenge Overview

Implement a **stream processor** that consumes two event streams and produces an output stream with joined data. This challenge will demonstrate your ability to handle **stream joins, real-time data, delivery guarantees, and concurrency** in a production-like streaming system.

### What You'll Build

A stream processing system that joins two event streams:

**Stream A: `page_view` events**
- `user_id`, `event_time`, `url`, `event_id`

**Stream B: `ad_click` events**
- `user_id`, `event_time`, `campaign_id`, `click_id`

**Goal:** For each `page_view`, attach the **most recent** `ad_click` for the same `user_id` that happened within **30 minutes** prior to `page_view` event in **event time**, and emit new `attributed_page_view` event with fields:

- `page_view_id`, `page_id`, `user_id`, `event_time`, `url`, `attributed_campaign_id` (nullable), `attributed_click_id` (nullable)

### Example Flow

```
User clicks ad for campaign X (12:00) → User views page (12:01) → Output: Page view attributed to ad campaign X
User views page (12:05) → No prior click for that user on that page → Output: Page view with nulls in `attributed_campaign_id` and `attributed_click_id`
```

---

## Important: Repository Naming

**To avoid this challenge being tracked or indexed online, you must name your repository as a car brand and model.**

Examples: `tesla-model-s`, `honda-civic`, `ford-mustang`, `toyota-camry`

**Do not** use terms like "challenge", "interview", "stream-processor", etc. in your repository name.

---

## Requirements

### 1) Stream Join + Out-of-Order Handling

* Events arrive **out of order** (with respect to `event_time`)
* You must support **watermarks**:
  * Configurable allowed lateness (max 15 minutes)
* A `page_view` can be emitted when you are confident no other `ad_click` will arrive that should supersede attribution (based on watermark), **or** you can emit updates to already produced `attributed_page_view` (but then specify your update strategy clearly).

**Edge cases to handle:**

* Click arrives after its page_view (but still within lateness)
* Multiple clicks in the 30-min window → pick the latest click
* Duplicate clicks in the 30-min window → emit only one attributed page view
* Late events beyond allowed lateness → drop or dead-letter (must be explicit)

### 2) Offset Management & Delivery Guarantees

* Input streams provide offsets (like Kafka):
  * `partition`, `offset`, `payload`
* You must implement a **consumer loop** that:
  * Consumes events from both streams
  * Implements join logic according to above spec
  * Writes output to a sink (local file or local DB) and commits consumed offsets to the stream
* Your implementation must be resilient to crashes and restarts. Document your delivery guarantees and what happens under different failure scenarios.

### 3) Concurrency

To achieve high throughput, your implementation should use multiple workers/threads to process partitions and/or batches concurrently. Make sure your implementation is thread-safe and works correctly under concurrent load.

### 5) Determinism + Testability

Provide a deterministic test harness:

* Feed a fixed set of events with known ordering and offsets
* Assert output records exactly (including attribution correctness)
* Include restart test: process half, crash, restart, ensure correctness

---

## Input/Output Contract

### Input

Two streams that yield records:

```
(topic, partition, offset, payload_json)
```

### Output

A sink interface:

```
write(record)
```

Offset commit interface:

```
commit(topic, partition, offset)
```
---

## Deliverables

1. **Working processor implementation** (language of choice: Java or Scala)
2. **Documentation explaining:**
   * Watermark logic
   * Write semantics (emit-once vs update)
   * Delivery guarantees (at-least-once, exactly-once, idempotence, etc.) and failure modes
   * Concurrency model
   * Capacity planning and scaling (state size, number of workers/instances)
3. **Tests for:**
   * Out-of-order events
   * Late data
   * Restart with committed offsets
   * Concurrent partitions
4. **README with:**
   * Setup instructions
   * How to run the processor
   * How to verify results

---

## Bonus Challenge

**Frontend Dashboard (optional, can be AI-generated)**

Create a simple web interface that shows the data flow in real-time:

* Visual representation of incoming events (clicks and page views)
* Current watermark position
* Attribution matches being made
* State size / memory usage
* Processing lag by partition

This can be built with any framework (React, Vue, simple HTML/JS) and can leverage AI code generation tools. The focus is on **visualizing the streaming concepts**, not frontend engineering excellence.

Example features:
* Live event stream visualization
* Attribution timeline view
* Metrics dashboard (throughput, latency, state size)
* Event inspector (click any event to see its journey)

---

## Event Schemas

### Input: Page View Event
```json
{
  "user_id": "user_1",
  "event_time": "2024-01-01T12:10:00Z",
  "url": "https://example.com/product",
  "event_id": "pv_1"
}
```

### Input: Ad Click Event
```json
{
  "user_id": "user_1",
  "event_time": "2024-01-01T12:00:00Z",
  "campaign_id": "campaign_A",
  "click_id": "click_1"
}
```

### Output: Attributed Page View
```json
{
  "page_view_id": "pv_1",
  "user_id": "user_1",
  "event_time": "2024-01-01T12:10:00Z",
  "url": "https://example.com/product",
  "attributed_campaign_id": "campaign_A",
  "attributed_click_id": "click_1"
}
```

## Test Scenarios

The data generator creates events covering these edge cases:

| Scenario | Description | Expected Behavior |
|----------|-------------|-------------------|
| **Normal** | Click at 12:00, page view at 12:10 | Attributes to campaign |
| **Out-of-order** | Page view arrives before its click | Handles via watermarks |
| **Multiple clicks** | 2+ clicks in 30-min window | Attributes to most recent |
| **Old click** | Click >30 minutes before page view | No attribution |
| **Late event** | Event beyond allowed lateness (2 min) | Dropped |
| **No click** | Page view with no prior click | Null attribution |

---

## Extension Ideas (Optional)

If you finish the core requirements and want to go further, consider these extensions:

* **Backpressure:** Simulate slow sink and ensure consumers don't OOM
* **Skew handling:** Hot users / hot partitions—how to mitigate contention
* **Metrics & Monitoring:** Add instrumentation to track processing latency, throughput, and state size

---

## Learning Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring Kafka Reference](https://spring.io/projects/spring-kafka)
- [Streaming Systems Book](http://streamingsystems.net/)
- [The Dataflow Model Paper](https://research.google/pubs/pub43864/)

---

## Questions?

If you have questions about the requirements, edge cases, or technical setup, please reach out to your contact person.

---

**Good luck, and we're excited to see your solution! 🚀**
