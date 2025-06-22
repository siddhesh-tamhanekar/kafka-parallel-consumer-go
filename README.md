
# Parallel Kafka Consumer
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)

A custom Kafka consumer with built-in concurrency support, implemented in Go. This project is not a replacement for existing Kafka client libraries — it uses [franz-go](https://github.com/twmb/franz-go) under the hood and focuses purely on robust and production-safe consumer behavior.


> **Inspired by:** [`confluentinc/parallel-consumer`](https://github.com/confluentinc/parallel-consumer)

---

## 🎥 Demo Videos

### 1. Graceful Shutdown and Offset Commit Handling

[![Graceful Shutdown Demo](https://img.youtube.com/vi/wX18o9ZZlWU/hqdefault.jpg)](https://youtu.be/wX18o9ZZlWU)

* Demonstrates how the consumer listens to termination signals and completes all in-flight message processing before committing offsets and shutting down.
* 📺 [Watch Video](https://youtu.be/wX18o9ZZlWU)

### 2. Kafka Consumer Group Rebalancing in Action

[![Rebalance Demo](https://img.youtube.com/vi/qAc_PclKNv8/hqdefault.jpg)](https://youtu.be/qAc_PclKNv8)

* Shows how partitions are reassigned when another consumer joins or leaves the group, including loss and reassignment logs.
* During rebalancing:

  * The consumer stops taking new messages.
  * Waits for all in-progress work to finish.
  * Commits processed offsets before giving up partition ownership.
  * After rebalance completes, newly assigned partitions resume processing.

⚠️ **Caution:** Since the consumer waits for in-progress work to finish before completing the rebalance protocol, the `partition.rebalance.timeout.ms` (or `session.timeout.ms` depending on client) in Kafka should be configured accordingly to avoid unnecessary consumer eviction. In the future, this behavior may become configurable — allowing users to choose whether to wait for in-progress messages or only commit completed ones.

* 📺 [Watch Video](https://youtu.be/qAc_PclKNv8)

### 3. Crash Scenerio and Message Reprocessing

[![Crash Scenerio Demo](https://img.youtube.com/vi/5TIeONRPiB4/hqdefault.jpg)](https://youtu.be/5TIeONRPiB4)

* Simulates consumer crash and demonstrates Kafka's at-least-once guarantee by reprocessing uncommitted messages.
* 📺 [Watch Video](https://youtu.be/5TIeONRPiB4)


---

## 🛠 Features

* ⚙️ **Controlled Concurrency:** A global semaphore ensures a cap on total in-flight message processing, enabling efficient load distribution and flow control.
* 🚦 **Graceful Shutdown:** Listens to OS signals, cancels context, and waits for all workers to finish before exit. Offset commits happen automatically after successful processing.
* 💥 **Crash Handling:** Demonstrates recovery of uncommitted messages by relying on Kafka's at-least-once delivery semantics.
* 🔄 **Rebalancing Visibility:** Logs partition assignment, revocation, and reassignment during consumer group rebalances.
* 🧪 **Demo Utilities:** Includes demo producers, viewers, and a self-contained test setup under the `demo/` directory.

---

## 📁 Usage Reference

To understand how to run and test the consumer, refer to the `demo/` folder and the [DEMO.md](./demo/DEMO.md) guide, which describes the test setup in detail.

---

## 🚧 Future Enhancements

* Add retry and dead-letter queue (DLQ) handling logic
* Cleanup excessive or debug logs for better clarity
* Support idempotency using distributed cache to avoid message reprocessing on node crash
* Ensure correct order of execution for messages with the same key
* Expose Prometheus metrics for observability
* Make offset commit strategy configurable
* Decouple from franz-go: Refactor the consumer logic to be library-agnostic, enabling support for alternative Kafka clients via clean interfaces

---

## 🤝 Contributing

Contributions are welcome! If you find a bug, have a suggestion, or want to extend functionality:

1. Open an issue describing the problem or enhancement.
2. Fork the repo and create a new branch.
3. Submit a pull request with clear description and context.

For major changes, please open a discussion first.

Thanks for helping improve this project!

