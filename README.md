# PrioMesh

PrioMesh is an early-stage Python library for priority-based task scheduling
across named sub-queues. It provides pluggable priority and sub-queue policies.

The current implementation is an in-memory, single-process prototype. Durable
storage, workers, and distributed execution are not implemented yet.

## Requirements

- Python 3.10 or newer

## Install

For development from a local checkout:

```bash
git clone https://github.com/PrioMesh/priomesh-python.git
cd priomesh-python
python -m pip install -e .
```

Once the project is published to PyPI, end users will be able to install it
with:

```bash
python -m pip install priomesh-python
```

Until then, users can install directly from the GitHub repository:

```bash
python -m pip install "git+https://github.com/PrioMesh/priomesh-python.git"
```

## Quick start

```python
import asyncio

from priomesh import Job, Priority, PriorityFairQueue, QueueShutDown


async def main() -> None:
    queue = PriorityFairQueue()

    async def consume() -> None:
        try:
            while True:
                job = await queue.get()
                print(job)
        except QueueShutDown:
            pass

    consumer = asyncio.create_task(consume())
    await queue.put(Job("job-1", Priority.HIGH, "tenant-a"))
    await queue.put(Job("job-2", Priority.HIGH, "tenant-b"))
    await queue.shutdown()
    await consumer


asyncio.run(main())
```

`get()` first selects an active priority level, then selects an active
sub-queue within that level. Jobs within each priority/sub-queue pair are
returned in FIFO order. Calling `get()` on an empty queue waits until a job is
put; `empty()` checks whether the queue currently has any jobs.

Call `shutdown()` to stop accepting jobs. It performs a graceful shutdown:
`put()` raises `QueueShutDown`, while `get()` continues returning jobs that
were already queued. Once the queue is drained, waiting and future `get()`
calls raise `QueueShutDown`, allowing consumers to exit. The example above
shows a consumer draining the queue and stopping cleanly.

## Scheduling policies

Priority and sub-queue policies are independent and can be combined:

| Policy | Behavior | Use when |
| --- | --- | --- |
| `RandomPriorityPolicy` (default) | Randomly selects an active priority, weighting higher priorities more heavily. With all four levels active, the weights are 4:3:2:1 from `CRITICAL` to `LOW`. | Higher priorities should usually run sooner, while lower priorities should still be eligible. |
| `AbsolutePriorityPolicy` | Always selects the highest-priority active level. | Priority order must be strict. A continuously active higher priority can indefinitely delay lower priorities. |
| `AgingWeightedRandomSubQueuePolicy` (default) | Randomly selects a sub-queue with weight `sqrt(pending_jobs) * (waiting_rounds + 1)`. | Larger backlogs should receive more opportunities, while sub-queues skipped in prior selections gain weight. |
| `FifoSubQueuePolicy` | Selects the sub-queue at the front of the rotation; a sub-queue with remaining work goes to the back after serving one job. | Predictable round-robin service among active sub-queues at the selected priority is preferred. |
| `RandomSubQueuePolicy` | Selects uniformly at random among active sub-queues. | Sub-queues should have equal selection probability, regardless of backlog. |

The aging policy is probabilistic: its weights affect relative selection
chances, but it does not guarantee a fixed service ratio or a maximum wait.
`waiting_rounds` counts selections of other sub-queues at the same priority,
not elapsed time. When a sub-queue is selected, its waiting count resets.

Policies can be replaced when constructing the queue:

```python
from priomesh import (
    AbsolutePriorityPolicy,
    AgingWeightedRandomSubQueuePolicy,
    FifoSubQueuePolicy,
    PriorityFairQueue,
    RandomPriorityPolicy,
    RandomSubQueuePolicy,
)

# Strict priority order, with deterministic round-robin among sub-queues.
queue = PriorityFairQueue(
    priority_policy=AbsolutePriorityPolicy(),
    subqueue_policy=FifoSubQueuePolicy(),
)

# Probabilistic priority and uniform random sub-queue selection.
queue = PriorityFairQueue(
    priority_policy=RandomPriorityPolicy(),
    subqueue_policy=RandomSubQueuePolicy(),
)

# These are also the defaults; this is equivalent to PriorityFairQueue().
queue = PriorityFairQueue(
    priority_policy=RandomPriorityPolicy(),
    subqueue_policy=AgingWeightedRandomSubQueuePolicy(),
)
```

## Run the example from a checkout

```bash
python -m pip install -e .
python examples/basic.py
```

## Project status

PrioMesh is under active early development. The current queue is in-memory and
does not provide persistence, worker execution, retries, or cross-process
scheduling. A Go implementation is planned after the scheduling semantics
stabilize.

## License

PrioMesh is licensed under the MIT License. See [LICENSE](LICENSE).
