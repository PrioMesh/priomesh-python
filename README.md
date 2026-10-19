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

from priomesh import Job, Priority, PriorityFairQueue


async def main() -> None:
    queue = PriorityFairQueue()
    await queue.put(Job("job-1", Priority.HIGH, "tenant-a"))
    await queue.put(Job("job-2", Priority.HIGH, "tenant-b"))

    job = await queue.get()
    print(job)


asyncio.run(main())
```

The default scheduler randomly chooses among active priority levels, favoring
higher priorities, then uses aging-weighted random selection among sub-queues
at the selected level. A sub-queue's selection weight is
`sqrt(pending_jobs) * (waiting_rounds + 1)`: deeper queues receive more service
opportunities, while a queue that is repeatedly skipped gains weight. This is
probabilistic scheduling, not a strict service ratio or a maximum-wait
guarantee. Use `FifoSubQueuePolicy` instead when deterministic round-robin
service among active sub-queues is preferred.

Policies can be replaced when constructing the queue:

```python
from priomesh import (
    AbsolutePriorityPolicy,
    FifoSubQueuePolicy,
    PriorityFairQueue,
)

queue = PriorityFairQueue(
    priority_policy=AbsolutePriorityPolicy(),
    subqueue_policy=FifoSubQueuePolicy(),
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
