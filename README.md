# Real-time Session Attribution — Stream Processor

A Spring Boot stream processor that joins page_views and ad_clicks Kafka streams, emitting an attributed_page_view for each page view with the most recent in-window click from the same user. Built around event-time watermarks, at-least-once delivery with idempotent writes, per-user concurrency, and bounded state via watermark-driven eviction.

## Quick Start

### Prerequisites
- Java 21+
- Maven 3.9+
- Docker (for Kafka)

### Setup

```bash
# Verify environment
./test-setup.sh

# Build
mvn package -DskipTests
```

### Run the processor (live)

```bash
# 1. Start Kafka infrastructure
docker compose up -d zookeeper kafka kafka-ui

# 2. Start the processor
mvn spring-boot:run

# 3. Produce test events (in a separate terminal)
python data_generator.py
```

Kafka UI: http://localhost:8080
SQLite output: `./output/attributed_page_views.db`

### Verify results

```bash
sqlite3 ./output/attributed_page_views.db "SELECT * FROM attributed_page_views;"
```

Expected output (rows from `data_generator.py`):

```
pv_1|user_1|2024-01-01T12:10:00Z|https://example.com/product1|campaign_A|click_1
pv_2|user_2|2024-01-01T12:15:00Z|https://example.com/product2|campaign_B|click_2
pv_3|user_3|2024-01-01T12:30:00Z|https://example.com/product3|campaign_D|click_3b
pv_4|user_4|2024-01-01T13:10:00Z|https://example.com/product4||
pv_5|user_5|2024-01-01T12:45:00Z|https://example.com/product5||
pv_6|user_6|2024-01-01T13:20:00Z|https://example.com/product6||
```

Scenario coverage:
- `pv_1` — normal in-order attribution.
- `pv_2` — out-of-order click within lateness; backpatched after the click arrives.
- `pv_3` — multiple clicks in window; attributed to the latest.
- `pv_4` — click was 35 minutes earlier (outside 30-min attribution window); NULL attribution.
- `pv_5` — click was too late (beyond watermark); dropped → NULL attribution.
- `pv_6` — no click for this user; NULL attribution.

### Run tests

```bash
# All tests
mvn test

# A specific test
mvn test -Dtest='JoinEngineTest#outOfOrderClickArrival'
```

## Configuration

All tunables live in `src/main/resources/application.yml`:

| Property | Default | Purpose |
|---|---|---|
| `watermark.allowed-lateness-minutes` | 4 | How late an event can arrive (max 15 per spec). |
| `attribution.window-minutes` | 30 | Look-back window for click → page-view attribution. |
| `kafka.consumer.concurrency` | 3 | Number of consumer threads per topic. |
| `kafka.consumer.group-id` | `stream-processor-group` | Kafka consumer group. |
| `output.database.path` | `./output/attributed_page_views.db` | SQLite sink path. |

## Repository Naming

This repository is named `mercedes-gt63` per the assignment's requirement to use a car brand and model.

## Deliverables

1. **Working processor implementation** in Java (Spring Boot).
2. **Documentation** — see [DESIGN.md](DESIGN.md) for:
   * Watermark logic
   * Write semantics (emit-once vs update)
   * Delivery guarantees (at-least-once, idempotence) and failure modes
   * Concurrency model
   * Capacity planning and scaling
3. **Tests** (23 total, all passing):
   * Out-of-order events
     * `JoinEngineTest::outOfOrderClickArrival`
   * Late data / window boundaries / no-click
     * `JoinEngineTest::multipleClicksPicksLatest`
     * `JoinEngineTest::clickOutside30MinWindowNotAttributed`
     * `JoinEngineTest::veryLateEventDropped`
     * `JoinEngineTest::pageViewWithNoClick`
   * Restart with committed offsets
     * `RestartTest::restartFromCommittedOffsets`
     * `RestartTest::noDataLossOnCrashBeforeAck`
     * `RestartTest::ackOnlyAfterSinkFlush`
   * Concurrent partitions
     * `ConcurrencyTest::parallelPartitionProcessing`
     * `ConcurrencyTest::partitionOrderingPreserved`
     * `ConcurrencyTest::noDeadlocks`
     * `ConcurrencyTest::backpatchSucceedsUnderConcurrentSameUserTraffic`
4. **README** — this file (setup, run, verify).

## A note on AI usage

I used Claude as a pair-programming assistant for Java syntax, refactoring, and reasoning through trade-offs. Design decisions documented in `DESIGN.md` are mine — made through iterative discussion in which I rejected several of the AI's proposals (per-(topic, partition) watermark redesign, wall-clock idle-flush, engine-wide `synchronized`).
