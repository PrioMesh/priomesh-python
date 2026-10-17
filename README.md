# PrioMesh

PrioMesh is an in-development Python project for configurable priority and fair-queue scheduling.

The goal is to let applications choose scheduling policies and storage backends without changing the queue API. The first implementation is for Python; a compatible Go implementation is planned for a later phase.

> **Status:** Early development. The features below describe project goals and are not yet available unless implemented in the codebase.

## Goals

- Schedule work across configurable priority levels.
- Support named sub-queues with selectable dequeue policies, including FIFO and fairness-oriented policies.
- Reduce starvation through policies such as aging or weighted scheduling.
- Keep queue behavior independent from the storage backend.
- Start with in-memory and Redis backend options.
- Provide a consistent model and scheduling behavior for future Python and Go implementations.

## Planned architecture

```text
Producer
   │ enqueue
   ▼
Priority levels
   │ select eligible sub-queue
   ▼
Sub-queue policy (FIFO / fair scheduling)
   │ claim next job
   ▼
Storage backend (memory / Redis)
   │
   ▼
Worker
```

The exact policy names, delivery guarantees, and backend capabilities will be documented as they are implemented.

## Development

This repository is being initialized. Setup instructions and examples will be added once the first usable implementation is available.

## License

PrioMesh is licensed under the MIT License. See [LICENSE](LICENSE).
